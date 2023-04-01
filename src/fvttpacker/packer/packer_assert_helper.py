from pathlib import Path

from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.leveldb_tools import LevelDBTools


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

    def assert_path_to_input_dir_is_ok(self,
                                       path_to_input_dir: Path):
        self.__assert_path_to_dir_is_ok(path_to_input_dir)

    def assert_path_to_target_db_is_ok(self,
                                       path_to_target_db: Path,
                                       must_exist=True):

        # has to exist
        if must_exist and not path_to_target_db.exists():
            raise FvttPackerException(f"Directory '{path_to_target_db}' does not exist")

        # has to be a directory
        if path_to_target_db.exists() and not path_to_target_db.is_dir():
            raise FvttPackerException(f"Path '{path_to_target_db}' already exists but not as a directory.")

        # TODO: write test-case
        # If path exists and is a directory it must be openable as LevelDB
        if not LevelDBTools.test_open_as_leveldb(path_to_target_db):
            raise FvttPackerException(f"Path '{path_to_target_db}' cannot be opened as LevelDB")
