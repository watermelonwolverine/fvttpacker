import json

import plyvel
from plyvel import DB
from pathlib import Path

world_db_names = ["actors",
                  "cards",
                  "combats",
                  "drawings",
                  "fog",
                  "folders",
                  "items",
                  "journal",
                  "macros",
                  "messages",
                  "playlists",
                  "scenes",
                  "settings",
                  "tables",
                  "users"]


class Unpacker:

    def unpack_world(self,
                     path_to_world: Path,
                     output_dir: Path):

        for db_name in world_db_names:
            path_to_db = path_to_world.joinpath(db_name)

            self.__unpack_db(db_name,
                             path_to_db,
                             output_dir)

    def __unpack_db(self,
                    db_name: str,
                    path_to_db: Path,
                    output_dir: Path):

        if not output_dir.exists():
            output_dir.mkdir()

        db = plyvel.DB(str(path_to_db))

        self.__unpack_db_into_folder(db,
                                     output_dir.joinpath(db_name))

        db.close()

    def __unpack_db_into_folder(self,
                                db: DB,
                                target_dir: Path):
        if not target_dir.exists():
            target_dir.mkdir()

        for entry in db.iterator():
            key: bytes = entry[0]
            value: bytes = entry[1]

            json_dict = json.loads(value.decode("UTF-8"))

            target_file = target_dir.joinpath(key.decode("UTF-8") + ".json")

            with open(target_file, "wt+") as file:
                json.dump(json_dict,
                          file,
                          indent="  ")
