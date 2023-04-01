from io import TextIOWrapper
from typing import List, Union

from fvttpacker.fvttpacker_exception import FvttPackerException


class UserInteractor:
    def __init__(self,
                 text_io_wrapper_out: TextIOWrapper,
                 text_io_wrapper_in: TextIOWrapper):
        self.text_io_wrapper_out = text_io_wrapper_out
        self.text_io_wrapper_in = text_io_wrapper_in

    def ask_yes_or_no_question(self,
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

    def ask_for_comma_separated_list_of_integers(self,
                                                 question: str) -> List[int]:

        self.text_io_wrapper_out.write(question)

        try:
            result = None

            while result is None:
                self.text_io_wrapper_out.write("Please enter comma separated list of integers:\n")
                input_str = self.text_io_wrapper_in.readline().strip()

                if input_str == "":
                    result = list()
                else:
                    result = self.__split_into_integer(input_str)

            return result

        except KeyboardInterrupt:
            raise FvttPackerException("KeyboardInterrupt")

    def __split_into_integer(self,
                             input_str: str) -> Union[None, List[int]]:

        splits: List[str] = input_str.split(",")
        result: List[int] = list()

        for split in splits:
            try:
                result.append(int(split))
            except ValueError:
                return None

        return result
