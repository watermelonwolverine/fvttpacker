from pathlib import Path
from typing import Iterable

from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.leveldb_helper import LevelDBHelper


class PackerAssertHelper:

    def __assert_path_to_dir_is_ok(self,
                                   path_to_dir: Path):
        # has to exist
        if not path_to_dir.exists():
            raise FvttPackerException(f"Directory '{path_to_dir}' does not exist")

        # has to be a directory
        if path_to_dir.exists() and not path_to_dir.is_dir():
            raise FvttPackerException(f"Path '{path_to_dir}' already exists but not as a directory.")

    def assert_path_to_parent_input_dir_is_ok(self,
                                              path_to_parent_input_dir: Path):
        self.__assert_path_to_dir_is_ok(path_to_parent_input_dir)

    def assert_path_to_parent_target_dir_is_ok(self,
                                               path_to_parent_target_dir: Path):
        self.__assert_path_to_dir_is_ok(path_to_parent_target_dir)

    def assert_paths_to_input_dirs_are_ok(self,
                                          paths_to_input_dirs: Iterable[Path]):
        for path_to_input_dir in paths_to_input_dirs:
            self.assert_path_to_input_dir_is_ok(path_to_input_dir)

    def assert_path_to_input_dir_is_ok(self,
                                       path_to_input_dir: Path):
        self.__assert_path_to_dir_is_ok(path_to_input_dir)

    def assert_paths_to_target_dbs_are_ok(self,
                                          paths_to_target_dbs: Iterable[Path],
                                          must_exist):
        for path_to_target_db in paths_to_target_dbs:
            self.assert_path_to_target_db_is_ok(path_to_target_db, must_exist)

    def assert_path_to_target_db_is_ok(self,
                                       path_to_target_db: Path,
                                       must_exist):

        # has to exist
        if must_exist and not path_to_target_db.exists():
            raise FvttPackerException(f"Directory '{path_to_target_db}' does not exist")

        # has to be a directory
        if path_to_target_db.exists() and not path_to_target_db.is_dir():
            raise FvttPackerException(f"Path '{path_to_target_db}' already exists but not as a directory.")

        # TODO: write test-case
        # If path exists and is a directory it must be openable as LevelDB
        if path_to_target_db.exists() and not LevelDBHelper.test_open_as_leveldb(path_to_target_db):
            raise FvttPackerException(f"Path '{path_to_target_db}' cannot be opened as LevelDB")
