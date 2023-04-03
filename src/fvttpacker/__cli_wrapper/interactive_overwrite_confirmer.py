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
        # Build question
        question = "The following LevelDBs already exist:\n"

        n: int = 1
        for target_db in paths_to_target_dbs:
            question += f"{n}: {target_db}\n"
            n += 1

        question += "Press Enter if you want to overwrite all," \
                    " or enter the numbers of the entries you want to exclude from overwriting:\n"

        # Ask question
        numbers = self.__user_interactor.ask_for_comma_separated_list_of_integers(question)

        # Convert list of integers into dict
        result: Dict[Path, bool] = dict()
        n = 1
        for target_db in paths_to_target_dbs:
            if n in numbers:
                result[target_db] = False
            else:
                result[target_db] = True
            n += 1

        return result
