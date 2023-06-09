from pathlib import Path

from fvttpacker.unpacker import Unpacker

path_to_world = Path("leveldb_files")
output_dir = Path("unpack_results")

unpacker = Unpacker()

unpacker.unpack_world_into_dirs_under(path_to_world,
                                      output_dir)
