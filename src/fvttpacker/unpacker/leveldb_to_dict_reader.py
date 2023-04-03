import logging
from pathlib import Path
from typing import Iterable, Dict, Union

import plyvel

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.__constants import UTF_8


class LevelDBToDictReader:

    @staticmethod
    def read_dbs_as_dicts(paths_to_input_dbs: Iterable[Path]) -> Dict[Path, Dict[str, str]]:
        result: Dict[Path, Dict[str, str]] = dict()

        for path_to_input_db in paths_to_input_dbs:
            result[path_to_input_db] = LevelDBToDictReader.read_db_at_x_into_dict(path_to_input_db,
                                                                                  skip_checks=True)

        return result

    @staticmethod
    def read_db_at_x_into_dict(path_to_input_db: Path,
                               skip_checks: bool) -> Dict[str, str]:
        """
        Reads the given LevelDB (`path_to_db`) into memory.

        :param path_to_input_db: e.g. "./foundrydata/Data/worlds/test/actors"
        :param skip_checks:

        :return: dict with filenames as keys and file contents as values
        """

        logging.info("Reading LevelDB '%s' into a dict", path_to_input_db)

        if not skip_checks:
            AssertHelper.assert_path_to_input_db_is_ok(path_to_input_db)

        db: Union[plyvel.DB, None] = None

        try:
            db = LevelDBHelper.try_open_db(path_to_input_db,
                                           skip_checks=True,
                                           must_exist=True)

            return LevelDBToDictReader.read_db_into_dict(db)
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def read_db_into_dict(db: plyvel.DB) -> Dict[str, str]:

        result: Dict[str, str] = dict()

        for entry in db.iterator():
            key: bytes = entry[0]
            value: bytes = entry[1]

            key_str = key.decode(UTF_8)
            value_str = value.decode(UTF_8)

            result[key_str] = value_str

        return result
