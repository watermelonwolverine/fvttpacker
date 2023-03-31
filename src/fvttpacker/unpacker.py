import json

import plyvel
from plyvel import DB
from pathlib import Path

from fvttpacker.__constants import world_db_names, UTF_8


class Unpacker:

    def unpack_world(self,
                     path_to_world_data: Path,
                     output_dir: Path) -> None:
        """
        Similar to `unpack_folder`, but only unpacks the dbs it that belong to a world and ignores the rest.
        :param path_to_world_data: "./foundrydata/Data/worlds/test/data"
        :param output_dir: "./unpack_result"
        """

        for db_name in world_db_names:
            path_to_db = path_to_world_data.joinpath(db_name)

            self.unpack_db(path_to_db,
                           output_dir)

    def unpack_folder(self,
                      path_to_dbs: Path,
                      output_dir) -> None:
        """
        Unpacks all leveldbs under path_to_dbs into output_dir.
        :param path_to_dbs: e.g. "./foundrydata/Data/modules/shared-module/packs"
        :param output_dir: e.g. "unpack_result"
        """

        if not output_dir.exists():
            output_dir.mkdir()

        for path_to_db in path_to_dbs.glob("*/"):
            target_dir = output_dir.joinpath(path_to_db.name)

            self.unpack_db(path_to_db,
                           target_dir)

    def unpack_db(self,
                  path_to_db: Path,
                  target_dir: Path) -> None:
        """
        Unpacks the leveldb at the given `path_to_db` into a new folder under the given `output_dir`
        :param path_to_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        :param target_dir: e.g. "./unpack_result/actors"
        """

        if not target_dir.exists():
            target_dir.mkdir()

        db = plyvel.DB(str(path_to_db))

        self.__unpack_db_into_folder(db,
                                     target_dir)

        db.close()

    def __unpack_db_into_folder(self,
                                db: DB,
                                target_dir: Path) -> None:
        """
        Unpacks the given `db` into the given `target_dir`
        :param db: leveldb to unpack
        :param target_dir: e.g. "./unpack_result/actors"
        """

        for entry in db.iterator():
            key: bytes = entry[0]
            value: bytes = entry[1]

            json_dict = json.loads(value.decode(UTF_8))

            target_file = target_dir.joinpath(key.decode(UTF_8) + ".json")

            with open(target_file, "wt+", encoding=UTF_8) as file:
                json.dump(json_dict,
                          file,
                          indent="  ")
