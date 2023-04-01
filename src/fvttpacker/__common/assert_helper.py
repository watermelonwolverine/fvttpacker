from pathlib import Path
from typing import Iterable

from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.fvttpacker_exception import FvttPackerException


class AssertHelper:

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
        AssertHelper.__assert_path_to_dir_is_ok(path_to_parent_input_dir)

    @staticmethod
    def assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir: Path):
        AssertHelper.__assert_path_to_dir_is_ok(path_to_parent_target_dir)

    @staticmethod
    def assert_paths_to_input_dirs_are_ok(paths_to_input_dirs: Iterable[Path]):
        for path_to_input_dir in paths_to_input_dirs:
            AssertHelper.assert_path_to_input_dir_is_ok(path_to_input_dir)

    @staticmethod
    def assert_path_to_input_dir_is_ok(path_to_input_dir: Path):
        AssertHelper.__assert_path_to_dir_is_ok(path_to_input_dir)

    @staticmethod
    def assert_paths_to_target_dbs_are_ok(paths_to_target_dbs: Iterable[Path],
                                          must_exist):
        for path_to_target_db in paths_to_target_dbs:
            LevelDBHelper.assert_path_to_target_db_is_ok(path_to_target_db, must_exist)
