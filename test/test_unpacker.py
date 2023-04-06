from pathlib import Path

from fvttpacker.__cli_wrapper.__interactive_overwrite_confirmer import InteractiveOverwriteConfirmer
from fvttpacker.__unpacker.unpacker import Unpacker

path_to_world = Path("leveldb_files")
output_dir = Path("unpack_results")

Unpacker.unpack_world_dbs_under_x_into_dirs_under_y(path_to_world,
                                                    output_dir,
                                                    overwrite_confirmer=InteractiveOverwriteConfirmer())
