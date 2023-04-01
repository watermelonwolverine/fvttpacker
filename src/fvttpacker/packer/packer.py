import json
import logging
from pathlib import Path
from typing import Tuple, Dict, List

import plyvel
from plyvel import DB

from fvttpacker.__constants import world_db_names, UTF_8
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.leveldb_tools import LevelDBTools
from fvttpacker.override_confirmer import OverrideConfirmer, AllYesOverrideConfirmer
from fvttpacker.packer.packer_assert_helper import PackerAssertHelper


class Packer:
    assert_helper = PackerAssertHelper()

    def __init__(self,
                 override_confirmer: OverrideConfirmer = AllYesOverrideConfirmer()):

        self.override_confirmer = override_confirmer
        self.leveldb_tools = LevelDBTools()

    def pack_world_into_dbs_at(self,
                               path_to_parent_input_dir: Path,
                               path_to_parent_target_dir: Path) -> None:
        """
        Similar to `pack_subdirs_into_dbs_at`, but only packs the sub-folders under the given directory (`parent_input_dir`)
        that belong to a world.
        E.g. "actors", "cards", etc. will be packed, "monster-compendia" will not be packed.
        """

        self.assert_helper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)
        self.assert_helper.assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir)

        dirs_to_dbs: Dict[Path, Path] = dict()

        for db_name in world_db_names:
            input_dir = path_to_parent_input_dir.joinpath(db_name)

            if not input_dir.is_dir():
                raise FvttPackerException(f"Missing world data directory {db_name}.")

            target_db = path_to_parent_target_dir.joinpath(db_name)

            dirs_to_dbs[input_dir] = target_db

        self.pack_dirs_into_dbs(dirs_to_dbs,
                                skip_input_checks=True)

    def pack_subdirs_into_dbs_under(self,
                                    path_to_parent_input_dir: Path,
                                    path_to_parent_target_dir: Path) -> None:
        """
        Packs all sub-folders located under the given directory (`parent_input_dir`) into LevelDBs located under the
        given target directory (`parent_target_dir`).

        :param path_to_parent_input_dir: e.g. ".unpacked_dbs"
        :param path_to_parent_target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        """

        self.assert_helper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)
        self.assert_helper.assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir)

        dirs_to_dbs: Dict[Path, Path] = dict()

        for db_name in path_to_parent_input_dir.glob("*/"):
            # Remove trailing /
            db_name = db_name[:-1]

            input_dir = path_to_parent_input_dir.joinpath(db_name)

            target_db = path_to_parent_target_dir.joinpath(db_name)

            dirs_to_dbs[input_dir] = target_db

        self.pack_dirs_into_dbs(dirs_to_dbs,
                                skip_input_checks=True)

    def pack_dirs_into_dbs(self,
                           input_dir_paths_to_target_db_paths: Dict[Path, Path],
                           skip_input_checks=False,
                           skip_target_checks=False) -> None:
        """
        Packs all the given directories (keys). Each into its respective LevelDB at the given path (values).
        :param input_dir_paths_to_target_db_paths:
        """

        path_to_input_dir: Path

        # check input dir paths
        if not skip_input_checks:

            for path_to_input_dir in input_dir_paths_to_target_db_paths.keys():
                self.assert_helper.assert_path_to_input_dir_is_ok(path_to_input_dir)

        # check target db paths
        if not skip_target_checks:
            for path_to_target_db in input_dir_paths_to_target_db_paths.values():
                self.assert_helper.assert_path_to_target_db_is_ok(path_to_target_db,
                                                                  must_exist=False)

        # look for existing target dbs
        existing_target_dbs: List[Path] = list()
        for (path_to_input_dir, path_to_target_db) in input_dir_paths_to_target_db_paths.items():
            if path_to_target_db.exists():
                existing_target_dbs.append(path_to_target_db)

        # ask which existing dbs should be overriden
        override_answer: Dict[Path, bool] = dict()
        if len(existing_target_dbs) > 0:
            override_answer = self.override_confirmer.confirm_batch_override_leveldb(existing_target_dbs)

        for (path_to_input_dir, path_to_target_db) in input_dir_paths_to_target_db_paths.items():

            # Don't override the excluded ones
            if path_to_target_db in existing_target_dbs \
                    and override_answer[path_to_target_db] == False:
                continue

            self.pack_dir_into_db_at(path_to_input_dir,
                                     path_to_target_db,
                                     skip_checks=True)

    def pack_dir_into_db_at(self,
                            path_to_input_dir: Path,
                            path_to_target_db: Path,
                            skip_checks=False) -> None:
        """
        Packs the given directory (`input_dir`) into the leveldb at the given location (`path_to_target_db`).
        If the `path_to_target_db` does not point to an existing LevelDB a new one will be created.
        Each json file will be converted into a db entry.
        The filename will be the key encoded as UTF-8.
        The file content will be the json string without indentations also encoded as UTF-8.

        :param path_to_input_dir: e.g. "./unpacked_dbs/actors"
        :param path_to_target_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        """
        if not skip_checks:
            self.assert_helper.assert_path_to_input_dir_is_ok(path_to_input_dir)
            self.assert_helper.assert_path_to_target_db_is_ok(path_to_target_db)

        db = self.leveldb_tools.try_open_db(path_to_target_db)

        self.pack_dir_into_db(path_to_input_dir,
                              db)

        db.close()

    def pack_dir_into_db(self,
                         input_dir: Path,
                         target_db: DB) -> None:
        """
        Packs the given directory (`input_dir`) into the given LevelDB `target_db`
        :param input_dir: The directory to pack
        :param target_db: The LevelDB to pack the `input_dir` into
        :return:
        """

        # read folder as a whole.
        folder_dict = self.read_dir_as_dict(input_dir)

        # avoid rewriting whole database everytime
        # instead only re-write updated entries
        self.pack_dict_into_db(folder_dict,
                               target_db)

    def read_dir_as_dict(self,
                         input_dir: Path) -> Dict[str, str]:
        # May not be the best use of memory, but it's nice to have everything in a dict
        """
        Reads the given directory (`input_dir`) into memory.
        With filenames as keys and file contents as values.

        :param input_dir: e.g. "./unpacked_data/actors"
        :return: dict with filenames as keys and file contents as values
        """
        result = dict()

        for path_to_file in input_dir.glob("*.json"):
            with open(path_to_file, "rt", encoding=UTF_8) as file:
                json_dict = json.load(file)

            # remove .json at the end
            key: str = path_to_file.name[0:-5]

            result[key] = json.dumps(json_dict)

        return result

    def pack_dict_into_db(self,
                          from_dict: Dict[str, str],
                          target_db: DB) -> None:

        """
        Packs the given dictionary (`from_dict`) into the given LevelDB (`target_db`).
        - Removes all entries from `to_db` that are not in `from_dict`
        - Adds missing entries to `to_db`
        - Updates entries in `to_db` if necessary
        :param from_dict: The dict to pack into the LevelDB
        :param target_db: The handle of the LevelDB to pack the dict into
        """

        wb: plyvel._plyvel.WriteBatch = target_db.write_batch()

        entry: Tuple[bytes]

        # Remove entries
        for entry in target_db.iterator():
            key_bytes: bytes = entry[0]
            key_str: str = key_bytes.decode(UTF_8)

            if key_str not in from_dict.keys():
                wb.delete(key_bytes)
                logging.debug("Deleted key %s from db", key_str)

        key_str: str
        value_str: str
        for (key_str, value_str) in from_dict.items():

            key_bytes: bytes = key_str.encode(UTF_8)
            target_value_bytes: bytes = value_str.encode(UTF_8)

            current_value_bytes: bytes = target_db.get(key_bytes)

            should_put = current_value_bytes is None \
                         or current_value_bytes != target_value_bytes

            if should_put:
                wb.put(key_bytes, from_dict[key_str].encode(UTF_8))
                logging.debug("Updated key %s", key_str)

        wb.write()
