import logging
from typing import Dict, Tuple

import plyvel

from fvttpacker.__constants import UTF_8


class DictToLevelDBWriter:

    @staticmethod
    def write_dict_into_db(input_dict: Dict[str, str],
                           target_db: plyvel.DB) -> int:

        """
        Packs the given dictionary (`input_dict`) into the given LevelDB (`target_db`).
        - Removes all entries from `to_db` that are not in `from_dict`
        - Adds missing entries to `to_db`
        - Updates entries in `to_db` if necessary

        :param input_dict: The dict to pack into the LevelDB
        :param target_db: The handle of the LevelDB to pack the dict into
        :return: Number of changed entries
        """

        logging.info("Packing dict '%s' into LevelDB '%s'",
                     hex(id(input_dict)),
                     hex(id(target_db)))

        # noinspection PyProtectedMember
        wb: plyvel._plyvel.WriteBatch = target_db.write_batch()
        logging.debug("Created batch")

        entry: Tuple[bytes]

        # Remove entries
        for entry in target_db.iterator():
            key_bytes: bytes = entry[0]
            key_str: str = key_bytes.decode(UTF_8)

            if key_str not in input_dict.keys():
                wb.delete(key_bytes)
                logging.info("Deleted key '%s'", key_str)

        key_str: str
        value_str: str
        nb_changes: int = 0

        for (key_str, value_str) in input_dict.items():

            key_bytes: bytes = key_str.encode(UTF_8)
            target_value_bytes: bytes = value_str.encode(UTF_8)

            current_value_bytes: bytes = target_db.get(key_bytes)

            should_put = current_value_bytes is None or current_value_bytes != target_value_bytes

            if should_put:
                wb.put(key_bytes, input_dict[key_str].encode(UTF_8))
                nb_changes += 1
                logging.info("Updated key '%s'", key_str)

        wb.write()
        logging.debug("Executing batch")

        logging.info("Number of changes in db '%s': %s",
                     hex(id(target_db)),
                     nb_changes)

        return nb_changes
