import sys
from io import TextIOWrapper
from pathlib import Path

from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.override_confirmer import OverrideConfirmer


class InteractiveOverrideConfirmer(OverrideConfirmer):
    text_io_wrapper_out: TextIOWrapper
    text_io_wrapper_in: TextIOWrapper

    def __init__(self,
                 text_io_wrapper_out: TextIOWrapper = sys.stdout,
                 text_io_wrapper_in: TextIOWrapper = sys.stdin):
        self.text_io_wrapper_out = text_io_wrapper_out
        self.text_io_wrapper_in = text_io_wrapper_in

    def __ask_yes_or_no_question(self,
                                 question: str) -> bool:

        self.text_io_wrapper_out.write(question)

        try:
            input_str: str = ""

            while input_str.lower() != "n" and input_str.lower() != "y":
                self.text_io_wrapper_out.write("Please enter y or n:\n")
                input_str = self.text_io_wrapper_in.readline().strip()

            return input_str.lower() == "y"

        except KeyboardInterrupt:
            raise FvttPackerException("KeyboardInterrupt")

    def confirm_override_leveldb(self,
                                 target_db: Path) -> bool:
        result = self.__ask_yes_or_no_question(f"The LevelDB '{target_db}' already exists.\n'"
                                               f"Do you want to override it?\n")

        if result:
            self.text_io_wrapper_out.write(f"Overriding '{target_db}'...\n")
        else:
            self.text_io_wrapper_out.write(f"Not overriding '{target_db}'...\n")

        return result

    def confirm_override_folder(self,
                                target_folder: Path) -> bool:
        result = self.__ask_yes_or_no_question(f"The folder '{target_folder}' already exists.\n'"
                                               f"Do you want to override it and its contents?\n")

        if result:
            self.text_io_wrapper_out.write(f"Overriding '{target_folder}'.\n")
        else:
            self.text_io_wrapper_out.write(f"Not overriding '{target_folder}'.\n")

        return result
