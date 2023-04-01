from pathlib import Path
from typing import Union

import plyvel

from fvttpacker.fvttpacker_exception import FvttPackerException


class LevelDBHelper:

    @staticmethod
    def try_open_db(path_to_target_db: Path) -> Union[plyvel.DB, None]:
        """
        Tries to open the `target_db`
        If it exists it confirms overriding via the `override_confirmer` that was given to the constructor.
        :param path_to_target_db: Path to the db to open.
        :return: The handle to the db.
        :raise: FvttPackerException if the path can not be opened as LevelDB.
        """
        create_if_missing: bool

        if path_to_target_db.exists():

            # Can't handle files
            if not path_to_target_db.is_dir():
                raise FvttPackerException(f"Path '{path_to_target_db}' already but not as a directory.")

            # Path exists as directory and can be opened as leveldb
            if LevelDBHelper.test_open_as_leveldb(path_to_target_db):
                create_if_missing = False
            # Can't handle folders that are not levelDBs
            else:
                raise FvttPackerException(f"{path_to_target_db} already exists, but cannot be opened as LevelDB.")
        else:
            create_if_missing = True

        try:
            # bool_create_if_missing is not working even though it is suggested
            # use create_if_missing instead
            return plyvel.DB(str(path_to_target_db),
                             create_if_missing=create_if_missing)
        except plyvel.Error as err:
            raise FvttPackerException(f"Unable to open {path_to_target_db} as leveldb.", err)

    @staticmethod
    def test_open_as_leveldb(path_to_db: Path):

        if not LevelDBHelper.__check_for_necessary_files(path_to_db):
            return False

        try:
            db = plyvel.DB(str(path_to_db))
            db.close()
            return True
        except plyvel.Error:
            return False

    @staticmethod
    def __check_for_necessary_files(path_to_db: Path) -> bool:
        """
        Checks if the given directory (`path_to_db`) is empty or if it contains  LOCK, LOG and CURRENT files.
        Reason: We don't want to open any arbitrary folder as LevelDB.
        1. It would put a LOCK and a LOG file everywhere
        2. Probably shouldn't write LevelDBs into arbitrary folders?.
        :return: True if the folder contains LOCK, LOG and CURRENT files or if it is empty
        """

        # empty directories are ok
        children = list(path_to_db.glob("*"))

        if len(children) == 0:
            return True

        LOCK_found = False
        LOG_found = False
        CURRENT_found = False

        for child in children:
            if child.name == "LOCK":
                LOCK_found = True
            if child.name == "LOG":
                LOG_found = True
            if child.name == "CURRENT":
                CURRENT_found = True

        return LOCK_found and LOG_found and CURRENT_found
