from pathlib import Path
from typing import Dict

from fvttpacker.__common.assert_helper import AssertHelper


def check_input_dir_and_target_dir(func):
    def wrapper(x_path_to_parent_input_dir: Path,
                y_path_to_parent_target_dir: Path,
                *args,
                **kwargs):
        AssertHelper.assert_path_to_parent_input_dir_is_ok(x_path_to_parent_input_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(y_path_to_parent_target_dir)

        func(x_path_to_parent_input_dir,
             y_path_to_parent_target_dir,
             *args,
             **kwargs)

    return wrapper


def check_input_dirs_and_target_dirs(func):
    def wrapper(input_db_paths_to_target_dir_paths: Dict[Path, Path]):

        AssertHelper.assert_paths_to_input_dbs_are_ok(input_db_paths_to_target_dir_paths.keys())

        AssertHelper.assert_paths_to_target_dirs_are_ok(input_db_paths_to_target_dir_paths.values())

        func(input_db_paths_to_target_dir_paths)

    return wrapper
