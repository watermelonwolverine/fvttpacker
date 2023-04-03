import logging
from pathlib import Path
from typing import Dict

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__constants import UTF_8


class DictToDirWriter:

    @staticmethod
    def write_dict_into_dir(input_dict: Dict[str, str],
                            path_to_target_dir: Path,
                            skip_checks: bool):
        """
        Packs the given dictionary (`input_dict`) into the given directory (`path_to_target_dict`).
        - Removes all entries from `to_db` that are not in `from_dict`
        - Adds missing entries to `to_db`
        - Updates entries in `to_db` if necessary

        :param input_dict: The dict to unpack into the directory
        :param path_to_target_dir: The path to the directory to unpack the dict into
        """

        if not skip_checks:
            AssertHelper.assert_path_to_target_dir_is_ok(path_to_target_dir)

        if not path_to_target_dir.exists():
            path_to_target_dir.mkdir()

        logging.info("Unpacking dict '%s' into directory '%s'",
                     hex(id(input_dict)),
                     path_to_target_dir)

        files_in_target_dir = path_to_target_dir.glob("*")

        # Remove entries
        for file_in_target_dir in files_in_target_dir:
            if file_in_target_dir.name not in input_dict.keys():
                file_in_target_dir.unlink()
                logging.info("Deleted file '%s'", file_in_target_dir)

        filename: str
        file_content: str

        for (filename, file_content) in input_dict.items():

            path_to_file = path_to_target_dir.joinpath(filename)

            if not path_to_file.exists():
                path_to_file.touch()

            current_file_content: str

            # TODO: catch exception that could happen here
            with open(path_to_file, "rt", encoding=UTF_8) as file:
                current_file_content = file.read()

            if file_content == current_file_content:
                continue

            with open(path_to_file, "wt", encoding=UTF_8) as file:
                file.write(file_content)
                logging.info("Updated file '%s'", filename)