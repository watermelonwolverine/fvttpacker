import json
from pathlib import Path
from typing import Dict

import plyvel
from plyvel import DB

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__common.override_helper import OverrideHelper
from fvttpacker.__constants import world_db_names, UTF_8
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.override_confirmer import OverrideConfirmer, AllYesOverrideConfirmer
from fvttpacker.unpacker.dict_to_dir_writer import DictToDirWriter
from fvttpacker.unpacker.leveldb_to_dict_reader import LevelDBToDictReader


class Unpacker:

    def __init__(self,
                 override_confirmer: OverrideConfirmer = AllYesOverrideConfirmer()):

        self.override_confirmer = override_confirmer

    def unpack_world_dbs_under_x_into_dirs_under_y(self,
                                                   x_path_to_parent_input_dir: Path,
                                                   y_path_to_parent_target_dir: Path) -> None:
        """
        Similar to `unpack_dbs_under_x_into_dirs_under_y`, but only unpacks the LevelDBs under the given directory
        (`x_path_to_parent_input_dir`) that belong to a world and ignores the rest.

        :param x_path_to_parent_input_dir: e.g. "./foundrydata/Data/worlds/test/data"
        :param y_path_to_parent_target_dir: e.g. "./unpack_result"
        """

        AssertHelper.assert_path_to_parent_input_dir_is_ok(x_path_to_parent_input_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(y_path_to_parent_target_dir)

        input_db_paths_to_target_dir_paths: Dict[Path, Path] = dict()

        for db_name in world_db_names:
            path_to_input_db = x_path_to_parent_input_dir.joinpath(db_name)
            path_to_target_dir = y_path_to_parent_target_dir.joinpath(db_name)

            if not path_to_target_dir.is_dir():
                raise FvttPackerException(f"Missing LevelDB {db_name} under {x_path_to_parent_input_dir}.")

            input_db_paths_to_target_dir_paths[path_to_input_db] = path_to_target_dir

        self.unpack_dbs_into_dirs(input_db_paths_to_target_dir_paths)

    def unpack_dbs_under_x_into_dirs_under_y(self,
                                             x_path_to_parent_input_dir: Path,
                                             y_path_to_parent_target_dir: Path) -> None:
        """
        Unpacks all LevelDBs in the given directory (`x_path_to_parent_input_dir`) into sub-folders of the given target
        directory (`y_path_to_parent_target_dir').
        :param x_path_to_parent_input_dir: e.g. "./foundrydata/Data/modules/shared-module/packs"
        :param y_path_to_parent_target_dir: e.g. "unpack_result"
        """

        AssertHelper.assert_path_to_parent_input_dir_is_ok(x_path_to_parent_input_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(y_path_to_parent_target_dir)

        input_db_paths_to_target_dir_paths: Dict[Path, Path] = dict()

        for db_name in x_path_to_parent_input_dir.glob("*/"):
            db_name = db_name[:-1]

            path_to_input_db = x_path_to_parent_input_dir.joinpath(db_name)

            path_to_target_dir = y_path_to_parent_target_dir.joinpath(db_name)

            input_db_paths_to_target_dir_paths[path_to_input_db] = path_to_target_dir

        self.unpack_dbs_into_dirs(input_db_paths_to_target_dir_paths)

    def unpack_dbs_into_dirs(self,
                             input_db_paths_to_target_dir_paths: Dict[Path, Path],
                             skip_input_checks=False,
                             skip_target_checks=False):
        """
        Unpacks all the given LevelDB at the given Paths (keys).
        Each into its respective directory at the given Path (values).

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
        input_db_paths_to_dicts: Dict[Path, Dict[str, str]] = LevelDBToDictReader.read_dbs_as_dicts(
            input_db_paths_to_target_dir_paths.keys())

        # coming this far means:
        # - all input dbs were successfully read into dicts

        for (path_to_input_db, path_to_target_dir) in input_db_paths_to_target_dir_paths.items():
            DictToDirWriter.write_dict_into_dir(input_db_paths_to_dicts[path_to_input_db],
                                                path_to_target_dir,
                                                skip_checks=True)

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

    @staticmethod
    def unpack_db_into_folder(db: DB,
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
