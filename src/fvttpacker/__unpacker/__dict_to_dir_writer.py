import json
import logging
from pathlib import Path
from typing import Dict

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__constants import UTF_8


class DictToDirWriter:

    @staticmethod
    def write_dict_into_dir(input_dict: Dict[str, Dict],
                            path_to_target_dir: Path,
                            skip_checks: bool):
        """
        Packs the given dictionary (`input_dict`) into the given directory (`path_to_target_dict`).
        - Removes all entries from `path_to_target_dir` that are not in `from_dict`
        - Adds missing entries to `path_to_target_dir`
        - Updates entries in `path_to_target_dir` if necessary

        :param input_dict: The dict to unpack into the directory
        :param path_to_target_dir: The path to the directory to unpack the dict into
        :param skip_checks: TODO
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

        target_filename: str
        target_content_dict: Dict

        for (target_filename, target_content_dict) in input_dict.items():

            path_to_file = path_to_target_dir.joinpath(target_filename + ".json")

            if not path_to_file.exists():
                path_to_file.touch()

            current_content_str: str

            # TODO: catch exception that could happen here
            with open(path_to_file, "rt", encoding=UTF_8) as file:
                current_content_str = file.read()

            target_content_str = json.dumps(target_content_dict, indent="  ")

            if target_content_str == current_content_str:
                continue

            with open(path_to_file, "wt", encoding=UTF_8) as file:
                file.write(target_content_str)
                logging.info("Updated file '%s'", target_filename)
