import sys
from io import TextIOWrapper
from pathlib import Path
from typing import List, Dict

from fvttpacker.__cli_wrapper.__user_interactor import UserInteractor
from fvttpacker.override_confirmer import OverrideConfirmer


class InteractiveOverrideConfirmer(OverrideConfirmer):

    def __init__(self,
                 text_io_wrapper_out: TextIOWrapper = sys.stdout,
                 text_io_wrapper_in: TextIOWrapper = sys.stdin):
        self.__text_io_wrapper_out = text_io_wrapper_out
        self.__user_interactor = UserInteractor(text_io_wrapper_out,
                                                text_io_wrapper_in)

    def confirm_override_leveldb(self,
                                 target_db: Path) -> bool:

        question = str(f"The LevelDB '{target_db}' already exists.\n'"
                       f"Do you want to override it?\n")

        result = self.__user_interactor.ask_yes_or_no_question(question)

        if result:
            self.__text_io_wrapper_out.write(f"Overriding '{target_db}'...\n")
        else:
            self.__text_io_wrapper_out.write(f"Not overriding '{target_db}'...\n")

        return result

    def confirm_override_folder(self,
                                target_folder: Path) -> bool:

        question = str(f"The folder '{target_folder}' already exists.\n'"
                       f"Do you want to override it and its contents?\n")

        result = self.__user_interactor.ask_yes_or_no_question(question)

        if result:
            self.__text_io_wrapper_out.write(f"Overriding '{target_folder}'.\n")
        else:
            self.__text_io_wrapper_out.write(f"Not overriding '{target_folder}'.\n")

        return result

    def confirm_batch_override_leveldb(self,
                                       target_dbs: List[Path]) -> Dict[Path, bool]:
        # Build question
        question = "The following LevelDBs already exist:\n"

        n: int = 1
        for target_db in target_dbs:
            question += f"{n}: {target_db}\n"
            n+=1

        question += "Press Enter if you want to override all," \
                   "or enter the numbers of the entries you want to exclude from overriding:\n"

        # Ask question
        numbers = self.__user_interactor.ask_for_comma_separated_list_of_integers(question)

        # Convert list of integers into dict
        result: Dict[Path, bool] = dict()
        n = 1
        for target_db in target_dbs:
            if n in numbers:
                result[target_db] = False
            else:
                result[target_db] = True
            n+=1

        return result
