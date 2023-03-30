import json

import plyvel
from plyvel import DB
from pathlib import Path

input_folder = Path("leveldb_files")
output_folder = Path("out")

db_names = ["actors",
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


def extract_to_jsons(db: DB,
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


def process_db(db_name: str,
               path_to_db: Path,
               output_dir: Path):
    if not output_dir.exists():
        output_dir.mkdir()

    db = plyvel.DB(str(path_to_db))

    extract_to_jsons(db,
                     output_dir.joinpath(db_name))

    db.close()


for db_name in db_names:
    path_to_db = input_folder.joinpath(db_name)

    process_db(db_name,
               path_to_db,
               output_folder)
