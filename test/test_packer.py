import logging
from pathlib import Path

from fvttpacker.__cli_wrapper.interactive_overwrite_confirmer import InteractiveOverwriteConfirmer
from fvttpacker.packer.packer import Packer

target_dir = Path("leveldb_files")
input_dir = Path("unpack_results")

logging.getLogger().setLevel(logging.DEBUG)

Packer.pack_world_dirs_under_x_into_dbs_under_y(input_dir,
                                                target_dir,
                                                overwrite_confirmer=InteractiveOverwriteConfirmer())
