from pathlib import Path
from typing import Iterable

from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.fvttpacker_exception import FvttPackerException


class AssertHelper:

    @staticmethod
    def __assert_path_to_dir_is_ok(path_to_dir: Path,
                                   parent_dir_must_exist: bool,
                                   must_exist: bool):

        # parent dir may have to exist
        if parent_dir_must_exist and not path_to_dir.parent.exists():
            raise FvttPackerException(f"Directory '{path_to_dir.parent}' does not exist")

        # may have to exist
        if must_exist and not path_to_dir.exists():
            raise FvttPackerException(f"Directory '{path_to_dir}' does not exist")

        # if exists, must be a directory
        if path_to_dir.exists() and not path_to_dir.is_dir():
            raise FvttPackerException(f"Path '{path_to_dir}' already exists but not as a directory.")

    @staticmethod
    def assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir: Path) -> None:
        AssertHelper.__assert_path_to_dir_is_ok(path_to_parent_input_dir,
                                                parent_dir_must_exist=False,
                                                must_exist=True)

    @staticmethod
    def assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir: Path) -> None:
        AssertHelper.__assert_path_to_dir_is_ok(path_to_parent_target_dir,
                                                parent_dir_must_exist=False,
                                                must_exist=True)

    @staticmethod
    def assert_paths_to_input_dirs_are_ok(paths_to_input_dirs: Iterable[Path]) -> None:
        for path_to_input_dir in paths_to_input_dirs:
            AssertHelper.assert_path_to_input_dir_is_ok(path_to_input_dir)

    @staticmethod
    def assert_path_to_input_db_is_ok(path_to_input_db: Path) -> None:
        LevelDBHelper.assert_path_to_db_is_ok(path_to_input_db,
                                              must_exist=True)

    @staticmethod
    def assert_paths_to_input_dbs_are_ok(paths_to_input_dbs: Iterable[Path]) -> None:
        for path_to_input_db in paths_to_input_dbs:
            AssertHelper.assert_path_to_input_db_is_ok(path_to_input_db)

    @staticmethod
    def assert_path_to_input_dir_is_ok(path_to_input_dir: Path) -> None:
        AssertHelper.__assert_path_to_dir_is_ok(path_to_input_dir,
                                                parent_dir_must_exist=False,
                                                must_exist=True)

    @staticmethod
    def assert_paths_to_target_dbs_are_ok(paths_to_target_dbs: Iterable[Path]) -> None:
        for path_to_target_db in paths_to_target_dbs:
            LevelDBHelper.assert_path_to_db_is_ok(path_to_target_db,
                                                  must_exist=False)

    @staticmethod
    def assert_path_to_target_dir_is_ok(path_to_target_dir: Path) -> None:
        # TODO: check read, write and remove access
        AssertHelper.__assert_path_to_dir_is_ok(path_to_target_dir,
                                                parent_dir_must_exist=True,
                                                must_exist=False)

    @staticmethod
    def assert_paths_to_target_dirs_are_ok(paths_to_target_dirs: Iterable[Path]):
        for path_to_target_dir in paths_to_target_dirs:
            AssertHelper.assert_path_to_target_dir_is_ok(path_to_target_dir)
