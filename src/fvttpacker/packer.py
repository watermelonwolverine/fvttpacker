import json
import logging
from typing import Union, Tuple, Dict

import plyvel
from plyvel import DB
from pathlib import Path

from fvttpacker.__constants import world_db_names, UTF_8
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.override_confirmer import OverrideConfirmer, AllYesOverrideConfirmer


class Packer:

    def __init__(self,
                 override_confirmer: OverrideConfirmer = AllYesOverrideConfirmer):

        self.override_confirmer = override_confirmer

    def pack_world(self,
                   input_dir: Path,
                   target_dir: Path) -> None:
        """
        Similar to pack_folder_as_dbs, but only packs the sub-folders under input_dir that belong to a world.
        E.g. "actors", "cards", etc. will be packed, "monster-compendia" will not be packed.
        """

        self.__assert_target_dir_is_ok(target_dir)

        for db_name in world_db_names:
            input_subdir = input_dir.joinpath(db_name)

            target_db = target_dir.joinpath(db_name)

            self.pack_folder_as_db(input_subdir,
                                   target_db)

    def __assert_target_dir_is_ok(self,
                                  target_dir: Path):

        if not target_dir.exists():
            raise FvttPackerException(f"Directory '{target_dir}' does not exist")

        if target_dir.exists() \
                and target_dir.is_file():
            raise FvttPackerException(f"Path '{target_dir}' already exists as a file.")

    def pack_folders_as_dbs(self,
                            input_dir: Path,
                            target_dir: Path) -> None:
        """
        Packs all sub-folders located in input_dir into new leveldbs located under target_dir.
        Will delete any existing folders that would collide with the newly created leveldb folders.

        :param input_dir: e.g. ".unpacked_dbs"
        :param target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        """

        self.__assert_target_dir_is_ok(target_dir)

        for db_name in input_dir.glob("*/"):
            # Remove trailing /
            db_name = db_name[:-1]

            input_subdir = input_dir.joinpath(db_name)

            target_db = target_dir.joinpath(db_name)

            self.pack_folder_as_db(input_subdir,
                                   target_db)

    def pack_folder_as_db(self,
                          input_dir: Path,
                          target_db: Path) -> None:
        """
        Packs the given input directory (input_dir) into a new leveldb at the given location (target_db).
        Each json file will be converted into a db entry.
        The filename will be the key encoded as UTF-8.
        The file content will be the json string without indentations also encoded as UTF-8.

        :param input_dir: e.g. "./unpacked_dbs/actors"
        :param target_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        """

        db = self.__try_open_db(target_db)

        # db is None -> User chose not to override db
        if db is None:
            return

        self.__pack_folder_into_db(input_dir,
                                   db)

        db.close()

    def __try_open_db(self,
                      target_db: Path) -> Union[DB, None]:
        """
        Tries to open the `target_db`
        :param target_db: Path to the db to open
        :return: None if the db exists, and it was chosen not to override the db or the handle to the db if the db did
        not exist or the user chose to override it.
        """
        create_if_missing = False

        if target_db.exists():

            # Can't handle files
            if not target_db.is_dir():
                raise FvttPackerException(f"Path '{target_db}' already exists as a file.")

            # Path exists as folder and can be opened as leveldb
            if self.__test_open_as_leveldb(target_db):

                # confirm override
                should_override = self.override_confirmer.confirm_override_leveldb(target_db)

                if not should_override:
                    # no to override -> nothing to do
                    return None
                else:
                    create_if_missing = False

            # Can't handle folders that are not levelDBs
            else:
                raise FvttPackerException(f"{target_db} already exists, but cannot be opened as LevelDB.")
        else:
            create_if_missing = True

        try:
            # bool_create_if_missing is not working even though it is suggested
            # use create_if_missing instead
            return plyvel.DB(str(target_db),
                             create_if_missing=create_if_missing)
        except plyvel.Error as err:
            raise FvttPackerException(f"Unable to open {target_db} as leveldb.", err)

    def __test_open_as_leveldb(self,
                               target_db: Path):

        try:
            db = plyvel.DB(str(target_db))
            db.close()
            return True
        except plyvel.Error as err:
            return False

    def __pack_folder_into_db(self,
                              input_dir: Path,
                              db: DB) -> None:

        # read folder as a whole.
        folder_dict = self.__read_folder_as_dict(input_dir)

        # avoid rewriting whole database everytime
        # instead only re-write updated entries
        self.__sync_dict_to_db(folder_dict,
                               db)

    def __read_folder_as_dict(self,
                              input_dir: Path) -> Dict[str, str]:

        """
        Reads the `input_dir` into memory as a dict with filenames as keys and file contents as values.
        May not be the best use of memory, but it's nice to have everything in a dict.

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

    def __sync_dict_to_db(self,
                          from_dict: Dict[str, str],
                          to_db: DB):

        """
        Synchronizes the given dictionary `from_dict` to the given `to_db`.
        - Removes all entries from `to_db` that are not in `from_dict`
        - Adds missing entries to `to_db`
        - Updates entries in `to_db` if necessary
        :param from_dict:
        :param to_db:
        :return:
        """

        wb: plyvel._plyvel.WriteBatch = to_db.write_batch()

        entry: Tuple[bytes]

        # Remove entries
        for entry in to_db.iterator():
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

            current_value_bytes: bytes = to_db.get(key_bytes)

            should_put = current_value_bytes is None \
                         or current_value_bytes != target_value_bytes

            if should_put:
                wb.put(key_bytes, from_dict[key_str].encode(UTF_8))
                logging.debug("Updated key %s", key_str)

        wb.write()
