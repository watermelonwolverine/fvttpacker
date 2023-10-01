import logging
import os
import shutil
from pathlib import Path
from typing import List

from plyvel import DB

from fvttpacker.__packer.packer import Packer
from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.__unpacker.unpacker import Unpacker

leveldb_files = Path("leveldb_files")
unpack_dir = Path("unpack_results")
pack_dir = Path("pack_results")

shutil.rmtree(unpack_dir)
shutil.rmtree(pack_dir)

os.mkdir(unpack_dir)
os.mkdir(pack_dir)

# logging.getLogger().setLevel(logging.DEBUG)

print("Unpacking original")

Unpacker.unpack_all_dbs_under_x_into_dirs_under_y(leveldb_files,
                                                  unpack_dir)
print("Repacking unpacked result")

Packer.pack_dirs_under_x_into_dbs_under_y(unpack_dir,
                                          pack_dir)

print("Comparing original with repacked result")

for db_name in pack_dir.glob("*/"):
    db_name = db_name.name

    print(f"Comparing {db_name}")

    path_to_original_dir = leveldb_files.joinpath(db_name)
    path_to_repacked_dir = pack_dir.joinpath(db_name)

    original_db: DB = LevelDBHelper.try_open_db(path_to_original_dir,
                                                skip_checks=False,
                                                must_exist=True)

    repacked_db: DB = LevelDBHelper.try_open_db(path_to_repacked_dir,
                                                skip_checks=False,
                                                must_exist=True)

    original_keys: List[bytes] = list()
    repacked_keys: List[bytes] = list()

    for entry in original_db.iterator():
        key: bytes = entry[0]

        original_keys.append(key)

    for entry in repacked_db.iterator():
        key: bytes = entry[0]

        repacked_keys.append(key)

    for key in original_keys:
        if key not in repacked_keys:
            raise Exception("Keys don't match")

    for key in repacked_keys:
        if key not in original_keys:
            raise Exception("Keys don't match")

    print("Keys match")

    for entry in original_db.iterator():
        key: bytes = entry[0]
        value_original: bytes = entry[1]
        value_repacked: bytes = repacked_db.get(key)

        if value_original != value_repacked:
            raise Exception("Values don't match")

    print("Values match")

print("Successfully finished full circle test")