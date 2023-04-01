from pathlib import Path
from typing import Iterable

from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.leveldb_helper import LevelDBHelper


class PackerAssertHelper:

    @staticmethod
    def __assert_path_to_dir_is_ok(path_to_dir: Path):
        # has to exist
        if not path_to_dir.exists():
            raise FvttPackerException(f"Directory '{path_to_dir}' does not exist")

        # has to be a directory
        if path_to_dir.exists() and not path_to_dir.is_dir():
            raise FvttPackerException(f"Path '{path_to_dir}' already exists but not as a directory.")

    @staticmethod
    def assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir: Path):
        PackerAssertHelper.__assert_path_to_dir_is_ok()

    @staticmethod
    def assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir: Path):
        PackerAssertHelper.__assert_path_to_dir_is_ok()

    @staticmethod
    def assert_paths_to_input_dirs_are_ok(paths_to_input_dirs: Iterable[Path]):
        for path_to_input_dir in paths_to_input_dirs:
            PackerAssertHelper.assert_path_to_input_dir_is_ok(path_to_input_dir)

    @staticmethod
    def assert_path_to_input_dir_is_ok(path_to_input_dir: Path):
        PackerAssertHelper.__assert_path_to_dir_is_ok()

    @staticmethod
    def assert_paths_to_target_dbs_are_ok(paths_to_target_dbs: Iterable[Path],
                                          must_exist):
        for path_to_target_db in paths_to_target_dbs:
            PackerAssertHelper.assert_path_to_target_db_is_ok(path_to_target_db, must_exist)

    @staticmethod
    def assert_path_to_target_db_is_ok(path_to_target_db: Path,
                                       must_exist):

        # may have to exist
        if must_exist and not path_to_target_db.exists():
            raise FvttPackerException(f"Directory '{path_to_target_db}' does not exist")

        # if exists, has to be a directory
        if path_to_target_db.exists() and not path_to_target_db.is_dir():
            raise FvttPackerException(f"Path '{path_to_target_db}' already exists but not as a directory.")

        # TODO: write test-case
        # if exists and dir, must be openable as LevelDB
        if path_to_target_db.exists() and not LevelDBHelper.test_open_as_leveldb(path_to_target_db):
            raise FvttPackerException(f"Path '{path_to_target_db}' cannot be opened as LevelDB")
