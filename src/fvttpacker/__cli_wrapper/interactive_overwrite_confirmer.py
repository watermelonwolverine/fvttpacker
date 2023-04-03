import sys
from io import TextIOWrapper
from pathlib import Path
from typing import List, Dict

from fvttpacker.__cli_wrapper.__user_interactor import UserInteractor
from fvttpacker.overwrite_confirmer import OverwriteConfirmer


class InteractiveOverwriteConfirmer(OverwriteConfirmer):

    def __init__(self,
                 text_io_wrapper_out: TextIOWrapper = sys.stdout,
                 text_io_wrapper_in: TextIOWrapper = sys.stdin):
        self.__text_io_wrapper_out = text_io_wrapper_out
        self.__user_interactor = UserInteractor(text_io_wrapper_out,
                                                text_io_wrapper_in)

    def confirm_overwrite_leveldb(self,
                                  path_to_target_db: Path) -> bool:

        question = str(f"The LevelDB '{path_to_target_db}' already exists.\n'"
                       f"Do you want to overwrite it?\n")

        result = self.__user_interactor.ask_yes_or_no_question(question)

        if result:
            self.__text_io_wrapper_out.write(f"Overwriting '{path_to_target_db}'...\n")
        else:
            self.__text_io_wrapper_out.write(f"Not overwriting '{path_to_target_db}'...\n")

        return result

    def confirm_overwrite_folder(self,
                                 path_to_target_dir: Path) -> bool:

        question = str(f"The folder '{path_to_target_dir}' already exists.\n'"
                       f"Do you want to overwrite it and its contents?\n")

        result = self.__user_interactor.ask_yes_or_no_question(question)

        if result:
            self.__text_io_wrapper_out.write(f"Overwriting '{path_to_target_dir}'.\n")
        else:
            self.__text_io_wrapper_out.write(f"Not overwriting '{path_to_target_dir}'.\n")

        return result

    def confirm_batch_overwrite_leveldb(self,
                                        paths_to_target_dbs: List[Path]) -> Dict[Path, bool]:

        return self.__confirm_overwrite_batch("The following LevelDBs already exist:\n",
                                              paths_to_target_dbs)

    def confirm_batch_overwrite_dirs(self,
                                     paths_to_target_dirs: List[Path]) -> Dict[Path, bool]:

        return self.__confirm_overwrite_batch("The following target directories already exist:\n",
                                              paths_to_target_dirs)

    def __confirm_overwrite_batch(self,
                                  question: str,
                                  paths: List[Path]) -> Dict[Path, bool]:
        path: Path

        n: int = 1
        for path in paths:
            question += f"{n}: {path}\n"
            n += 1

        question += "Press Enter if you want to overwrite all," \
                    " or enter the numbers of the entries you want to exclude from overwriting:\n"

        # Ask question
        numbers = self.__user_interactor.ask_for_comma_separated_list_of_integers(question)

        # Convert list of integers into dict
        result: Dict[Path, bool] = dict()
        n = 1
        for path in paths:
            if n in numbers:
                result[path] = False
            else:
                result[path] = True
            n += 1

        return result
