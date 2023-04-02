from pathlib import Path
from typing import Dict, List, Callable

callable_type = Callable[[List[Path]], Dict[Path, bool]]


class OverrideHelper:

    @staticmethod
    def ask_and_filter_out_non_override(input_paths_to_target_paths: Dict[Path, Path],
                                        override_confirm_method: callable_type) -> Dict[Path, Path]:

        # look for existing targets
        paths_to_existing_target_dbs: List[Path] = list()
        for (path_to_input_dir, path_to_target_db) in input_paths_to_target_paths.items():
            if path_to_target_db.exists():
                paths_to_existing_target_dbs.append(path_to_target_db)

        # ask which existing targets should be overriden
        target_db_paths_to_override_answers: Dict[Path, bool] = dict()
        if len(paths_to_existing_target_dbs) > 0:
            target_db_paths_to_override_answers = \
                override_confirm_method(paths_to_existing_target_dbs)

        # filter out the targets that should not be overriden
        result: Dict[Path, Path] = dict()
        for (path_to_input_dir, path_to_target_db) in input_paths_to_target_paths.items():

            # Don't copy over the ones which shouldn't be overriden
            if path_to_target_db in paths_to_existing_target_dbs \
                    and not target_db_paths_to_override_answers[path_to_target_db]:
                continue

            result[path_to_input_dir] = path_to_target_db

        return result
