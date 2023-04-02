import json
import logging
from pathlib import Path
from typing import Dict, Iterable

import plyvel
from plyvel import DB

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.__common.override_helper import OverrideHelper
from fvttpacker.__constants import world_db_names, UTF_8
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.override_confirmer import OverrideConfirmer, AllYesOverrideConfirmer


class Unpacker:

    def __init__(self,
                 override_confirmer: OverrideConfirmer = AllYesOverrideConfirmer()):

        self.override_confirmer = override_confirmer

    def unpack_world_into_dirs_under(self,
                                     path_to_parent_input_dir: Path,
                                     path_to_parent_target_dir: Path) -> None:
        """
        Similar to `unpack_dbs_into_dirs_under`, but only unpacks the dbs under the given directory
        (`path_to_parent_input_dir`) that belong to a world and ignores the rest.
        :param path_to_parent_input_dir: "./foundrydata/Data/worlds/test/data"
        :param path_to_parent_target_dir: "./unpack_result"
        """

        AssertHelper.assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)

        input_db_paths_to_target_dir_paths: Dict[Path, Path] = dict()

        for db_name in world_db_names:
            path_to_input_db = path_to_parent_input_dir.joinpath(db_name)
            path_to_target_dir = path_to_parent_target_dir.joinpath(db_name)

            if not path_to_target_dir.is_dir():
                raise FvttPackerException(f"Missing world LevelDB {db_name} under {path_to_parent_input_dir}.")

            input_db_paths_to_target_dir_paths[path_to_input_db] = path_to_target_dir

        self.unpack_dbs_into_dirs(input_db_paths_to_target_dir_paths)

    def unpack_dbs_into_dirs_under(self,
                                   path_to_parent_input_dir: Path,
                                   path_to_parent_target_dir: Path) -> None:
        """
        Unpacks all LevelDBs in the given directory (`path_to_input_dbs_parent_dir`) into sub-folders of the given target
        directory (`path_to_parent_target_dir').
        :param path_to_parent_input_dir: e.g. "./foundrydata/Data/modules/shared-module/packs"
        :param path_to_parent_target_dir: e.g. "unpack_result"
        """

        AssertHelper.assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)

        input_db_paths_to_target_dir_paths: Dict[Path, Path] = dict()

        for db_name in path_to_parent_input_dir.glob("*/"):
            db_name = db_name[:-1]

            path_to_input_db = path_to_parent_input_dir.joinpath(db_name)

            path_to_target_dir = path_to_parent_target_dir.joinpath(db_name)

            input_db_paths_to_target_dir_paths[path_to_input_db] = path_to_target_dir

        self.unpack_dbs_into_dirs(input_db_paths_to_target_dir_paths)

    def unpack_dbs_into_dirs(self,
                             input_db_paths_to_target_dir_paths: Dict[Path, Path],
                             skip_input_checks=False,
                             skip_target_checks=False):
        """
        Unpacks all the given LevelDBs (keys). Each into its respective directory at the given path (values).

        :param input_db_paths_to_target_dir_paths: Contains the paths to the input LevelDBs as keys
        and the paths to the target directories as values
        :param skip_input_checks:
        :param skip_target_checks:
        """

        path_to_input_db: Path
        path_to_target_dir: Path

        # check input db paths
        if not skip_input_checks:
            AssertHelper.assert_paths_to_input_dbs_are_ok(input_db_paths_to_target_dir_paths.keys())

        # check target dir paths
        if not skip_target_checks:
            AssertHelper.assert_paths_to_target_dirs_are_ok(input_db_paths_to_target_dir_paths.values())

        # ask which existing dirs should be overriden and filter out the dirs that should not be overriden
        input_db_paths_to_target_dir_paths = OverrideHelper.ask_and_filter_out_non_override(
            input_db_paths_to_target_dir_paths,
            self.override_confirmer.confirm_batch_override_dirs)

        # read all input dbs -> fail fast
        input_db_paths_to_dicts: Dict[Path, Dict[str, str]] = self.__read_dbs_as_dicts(
            input_db_paths_to_target_dir_paths.keys())

        # coming this far means:
        # - all input dbs were successfully read into dicts

        for (path_to_input_db, path_to_target_dir) in input_db_paths_to_target_dir_paths.items():
            self.unpack_dict_into_dir(input_db_paths_to_dicts[path_to_input_db],
                                      path_to_target_dir)

    def __read_dbs_as_dicts(self,
                            paths_to_input_dbs: Iterable[Path]) -> Dict[Path, Dict[str, str]]:

        result: Dict[Path, Dict[str, str]] = dict()

        for path_to_input_db in paths_to_input_dbs:
            result[path_to_input_db] = self.read_db_as_dict(path_to_input_db,
                                                            skip_checks=True)

        return result

    def read_db_as_dict(self,
                        path_to_input_db: Path,
                        skip_checks: bool) -> Dict[str, str]:
        """
        Reads the given LevelDB (`path_to_db`) into memory.

        :param path_to_input_db: e.g. "./foundrydata/Data/worlds/test/actors"
        :param skip_checks:

        :return: dict with filenames as keys and file contents as values
        """

        logging.info("Reading LevelDB '%s' into a dict", path_to_input_db)

        if not skip_checks:
            AssertHelper.assert_path_to_input_db_is_ok(path_to_input_db)

        with LevelDBHelper.try_open_db(path_to_input_db,
                                       skip_checks=True,
                                       must_exist=True) as db:
            return self.__read_db_as_dict(db)

    def __read_db_as_dict(self,
                          db: DB) -> Dict[str, str]:

        result: Dict[str, str] = dict()

        for entry in db.iterator():
            key: bytes = entry[0]
            value: bytes = entry[1]

            key_str = key.decode(UTF_8)
            value_str = value.decode(UTF_8)

            result[key_str] = value_str

        return result

    def unpack_db(self,
                  path_to_db: Path,
                  target_dir: Path) -> None:
        """
        Unpacks the leveldb at the given `path_to_db` into a new folder under the given `output_dir`
        :param path_to_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        :param target_dir: e.g. "./unpack_result/actors"
        """

        # TODO update

        if not target_dir.exists():
            target_dir.mkdir()

        db = plyvel.DB(str(path_to_db))

        self.unpack_db_into_folder(db,
                                   target_dir)

        db.close()

    def unpack_db_into_folder(self,
                              db: DB,
                              target_dir: Path) -> None:
        """
        Unpacks the given `db` into the given `target_dir`
        :param db: leveldb to unpack
        :param target_dir: e.g. "./unpack_result/actors"
        """

        # TODO update

        for entry in db.iterator():
            key: bytes = entry[0]
            value: bytes = entry[1]

            json_dict = json.loads(value.decode(UTF_8))

            target_file = target_dir.joinpath(key.decode(UTF_8) + ".json")

            with open(target_file, "wt+", encoding=UTF_8) as file:
                json.dump(json_dict,
                          file,
                          indent="  ")

    def unpack_dict_into_dir(self,
                             input_dict: Dict[str, str],
                             path_to_target_dir: Path,
                             skip_checks: bool):
        """
        Packs the given dictionary (`input_dict`) into the given directory (`path_to_target_dict`).
        - Removes all entries from `to_db` that are not in `from_dict`
        - Adds missing entries to `to_db`
        - Updates entries in `to_db` if necessary

        :param input_dict: The dict to unpack into the directory
        :param path_to_target_dir: The path to the directory to unpack the dict into
        """

        if not skip_checks:
            AssertHelper.assert_path_to_target_dir_is_ok(path_to_target_dir)

        if not path_to_target_dir.exists():
            path_to_target_dir.mkdir()

        logging.info("Unpacking dict '%s' into directory '%s'",
                     hex(id(input_dict)),
                     path_to_target_dir)

        files_in_target_dir = path_to_target_dir.glob("*")

        # Remove entries
        for file_in_target_dir in files_in_target_dir:
            if file_in_target_dir.name not in input_dict.keys():
                file_in_target_dir.unlink()
                logging.info("Deleted file '%s'", file_in_target_dir)

        filename: str
        file_content: str

        for (filename, file_content) in input_dict.items():

            path_to_file = path_to_target_dir.joinpath(filename)

            if not path_to_file.exists():
                path_to_file.touch()

            current_file_content: str

            with open(path_to_file, "rt", encoding=UTF_8) as file:
                current_file_content = file.read()

            if file_content == current_file_content:
                continue

            with open(path_to_file, "wt", encoding=UTF_8) as file:
                file.write(file_content)
                logging.info("Updated file '%s'", filename)
