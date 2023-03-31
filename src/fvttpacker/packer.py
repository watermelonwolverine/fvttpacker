import json
import shutil

import plyvel
from plyvel import DB
from pathlib import Path

from fvttpacker.__constants import world_db_names, UTF_8


class Packer:

    def pack_world(self,
                   input_dir: Path,
                   target_dir: Path) -> None:
        """
        Similar to pack_folder_as_dbs, but only packs the sub-folders under input_dir that belong to a world and ignores the rest.
        E.g. "actors", "cards", etc. will be packed, "monster-compendia" will not be packed.
        """
        # TODO: Check if target path exists as file
        if not target_dir.exists():
            target_dir.mkdir()

        for db_name in world_db_names:
            input_subdir = input_dir.joinpath(db_name)

            target_db = target_dir.joinpath(db_name)

            self.pack_folder_as_db(input_subdir,
                                   target_db)

    def pack_folders_as_dbs(self,
                            input_dir: Path,
                            target_dir: Path) -> None:
        """
        Packs all sub-folders located in input_dir into new leveldbs located under target_dir.
        Will delete any existing folders that would collide with the newly created leveldb folders.

        :param input_dir: e.g. ".unpacked_dbs"
        :param target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        """

        # TODO: Check if target path exists as file
        if not target_dir.exists():
            target_dir.mkdir()

        for db_name in input_dir.glob("*/"):
            input_subdir = input_dir.joinpath(db_name)

            target_db = target_dir.joinpath(db_name)

            self.pack_folder_as_db(input_subdir,
                                   target_db)

    def pack_folder_as_db(self,
                          input_dir: Path,
                          target_db: Path) -> None:
        """
        Packs the given input directory (input_dir) into a new leveldb at the given location (target_db).
        Each json file will be converted into a db entry.
        The filename will be the key encoded as UTF-8.
        The file content will be the json string without indentations also encoded as UTF-8.

        :param input_dir: e.g. "./unpacked_dbs/actors"
        :param target_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        """

        # TODO: Handle this better
        if target_db.exists():
            shutil.rmtree(target_db)

        # bool_create_if_missing is not working even though it is suggested
        db = plyvel.DB(str(target_db),
                       create_if_missing=True)

        self.__pack_folder_into_db(input_dir,
                                   db)

        db.close()

    def __pack_folder_into_db(self,
                              input_dir: Path,
                              db: DB) -> None:

        for path_to_file in input_dir.glob("*.json"):
            with open(path_to_file, "rt", encoding=UTF_8) as file:
                json_dict = json.load(file)

            # remove .json at the end
            key: str = path_to_file.name[0:-5]

            db.put(key.encode(UTF_8), json.dumps(json_dict).encode(UTF_8))
