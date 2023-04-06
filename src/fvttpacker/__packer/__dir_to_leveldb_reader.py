import json
import logging
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, Iterable

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__constants import UTF_8
from fvttpacker.fvttpacker_exception import FvttPackerException


class DirToDictReader:

    @staticmethod
    def read_dirs_as_dicts(paths_to_input_dirs: Iterable[Path]) -> Dict[Path, Dict[str, str]]:

        result: Dict[Path, Dict[str, str]] = dict()

        for path_to_input_dir in paths_to_input_dirs:
            dir_dict = DirToDictReader.read_dir_as_dict(path_to_input_dir,
                                                        skip_checks=True)

            result[path_to_input_dir] = dir_dict

            logging.debug("Read directory '%s' into dict '%s'",
                          path_to_input_dir,
                          hex(id(dir_dict)))

        return result

    @staticmethod
    def read_dir_as_dict(path_to_input_dir: Path,
                         skip_checks=False) -> Dict[str, str]:
        # May not be the best use of memory, but it's nice to have everything in a dict
        """
        Reads the given directory (`path_to_input_dir`) into memory.
        With filenames as keys and file contents as values.

        :param path_to_input_dir: e.g. "./unpacked_data/actors"
        :param skip_checks:

        :return: dict with filenames as keys and file contents as values
        """

        logging.info("Reading directory '%s' into a dict", path_to_input_dir)

        if not skip_checks:
            AssertHelper.assert_path_to_input_dir_is_ok(path_to_input_dir)

        result: Dict[str, str] = dict()

        for path_to_file in path_to_input_dir.glob("*.json"):

            logging.debug("Reading file '%s'", path_to_file)

            with open(path_to_file, "rt", encoding=UTF_8) as file:
                try:
                    json_dict = json.load(file)
                except JSONDecodeError as err:
                    raise FvttPackerException(f"Error while parsing '{path_to_file}' as json, reason:\n'{err}'")

            # remove .json at the end
            key: str = path_to_file.name[0:-5]

            result[key] = json.dumps(json_dict, separators=(",", ":"), indent=None)

        return result
