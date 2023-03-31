import logging
from pathlib import Path

from fvttpacker.__cli_wrapper.cli_override_confirmer import InteractiveOverrideConfirmer
from fvttpacker.packer import Packer

target_dir = Path("pack_results")
input_dir = Path("unpack_results")

logging.getLogger().setLevel(logging.DEBUG)

packer = Packer(override_confirmer=InteractiveOverrideConfirmer())

packer.pack_world(input_dir,
                  target_dir)
