from pathlib import Path

from fvttpacker.packer import Packer

target_dir = Path("pack_results")
input_dir = Path("unpack_results")

packer = Packer()

packer.pack_world(input_dir,
                  target_dir)
