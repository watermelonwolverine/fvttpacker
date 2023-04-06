import logging
from pathlib import Path
from typing import Union

import plyvel

from fvttpacker.fvttpacker_exception import FvttPackerException


class LevelDBHelper:

    @staticmethod
    def try_open_db(path_to_db: Path,
                    skip_checks: bool,
                    must_exist: bool) -> Union[plyvel.DB, None]:
        """
        Tries to open the LevelDB at the given path (`path_to_db`)


        :param path_to_db: Path to the LevelDB to open.
        :param skip_checks: TODO
        :param must_exist: TODO

        :return: The handle to the db.
        """

        logging.debug("Trying to open LevelDB at '%s'",
                      path_to_db)

        if not skip_checks:
            LevelDBHelper.assert_path_to_db_is_ok(path_to_db,
                                                  must_exist=must_exist)

        try:
            # bool_create_if_missing is not working even though it is suggested
            # use create_if_missing instead
            return plyvel.DB(str(path_to_db),
                             create_if_missing=not must_exist)
        except plyvel.Error as err:
            raise FvttPackerException(f"Unable to open {path_to_db} as leveldb.", err)

    @staticmethod
    def assert_path_to_db_is_ok(path_to_db: Path,
                                must_exist):

        # may have to exist
        if must_exist and not path_to_db.exists():
            raise FvttPackerException(f"Directory '{path_to_db}' does not exist")

        # if exists, has to be a directory
        if path_to_db.exists() and not path_to_db.is_dir():
            raise FvttPackerException(f"Path '{path_to_db}' exists but not as a directory.")

        # TODO: write test-case
        # if exists and dir, must be openable as LevelDB
        if path_to_db.exists() and not LevelDBHelper.test_open_as_leveldb(path_to_db):
            raise FvttPackerException(f"Path '{path_to_db}' cannot be opened as LevelDB")

    @staticmethod
    def test_open_as_leveldb(path_to_db: Path):

        if not LevelDBHelper.__check_for_necessary_files(path_to_db):
            return False

        try:
            logging.debug("Checking if '%s' can be opened as LevelDB",
                          path_to_db)
            db = plyvel.DB(str(path_to_db))
            db.close()
            logging.debug("'%s' can be opened as LevelDB",
                          path_to_db)
            return True
        except plyvel.Error as err:
            logging.debug("'%s' can not opened as LevelDB, reason: %s",
                          path_to_db,
                          err)
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

        logging.debug("Checking for necessary LevelDB files in '%s'",
                      path_to_db)

        # empty directories are ok
        children = list(path_to_db.glob("*"))

        if len(children) == 0:
            return True

        lock_found = False
        log_found = False
        current_found = False

        for child in children:
            if child.name == "LOCK":
                lock_found = True
                logging.debug("Found LOCK")
            if child.name == "LOG":
                log_found = True
                logging.debug("Found LOG")
            if child.name == "CURRENT":
                current_found = True
                logging.debug("Found CURRENT")

        return lock_found and log_found and current_found
