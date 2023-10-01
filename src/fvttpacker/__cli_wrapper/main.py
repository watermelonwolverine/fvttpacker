import logging
from pathlib import Path

import click

from fvttpacker.__cli_wrapper import __args
from fvttpacker.__cli_wrapper.__interactive_overwrite_confirmer import InteractiveOverwriteConfirmer
from fvttpacker.__packer.packer import Packer
from fvttpacker.__unpacker.unpacker import Unpacker
from fvttpacker.overwrite_confirmer import AllYesOverwriteConfirmer


@click.group()
@click.pass_context
@click.option(__args.verbosity_option, type=click.Choice(__args.verbosity_choices, case_sensitive=False))
@click.option(__args.no_interaction_option, is_flag=True)
def cli(context: click.Context,
        verbosity: str = None,
        no_interaction: bool = False) -> None:
    if verbosity is not None:
        logging.getLogger().setLevel(verbosity.upper())

    context.obj[__args.no_interaction_option] = no_interaction


def get_overwrite_confirmer(context: click.Context):
    if context.obj[__args.no_interaction_option]:
        return AllYesOverwriteConfirmer()
    else:
        return InteractiveOverwriteConfirmer()


@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def unpack_world(context: click.Context,
                 source_dir: str,
                 target_dir: str) -> None:
    Unpacker.unpack_world_dbs_under_x_into_dirs_under_y(
        Path(source_dir),
        Path(target_dir),
        get_overwrite_confirmer(context)
    )


@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def pack_world(context: click.Context,
               source_dir: str,
               target_dir: str) -> None:
    Packer.pack_world_dirs_under_x_into_dbs_under_y(
        Path(source_dir),
        Path(target_dir),
        get_overwrite_confirmer(context)
    )


@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def unpack_all(context: click.Context,
               source_dir: str,
               target_dir: str) -> None:
    Unpacker.unpack_all_dbs_under_x_into_dirs_under_y(
        Path(source_dir),
        Path(target_dir),
        get_overwrite_confirmer(context)
    )


@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def pack_all(context: click.Context,
             source_dir: str,
             target_dir: str) -> None:
    Packer.pack_dirs_under_x_into_dbs_under_y(
        Path(source_dir),
        Path(target_dir),
        get_overwrite_confirmer(context)
    )


@cli.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def pack(source_dir: str,
         target_dir: str) -> None:
    Packer.pack_dir_at_x_into_db_at_y(
        Path(source_dir),
        Path(target_dir)
    )


@cli.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def unpack(source_dir: str,
           target_dir: str) -> None:
    Unpacker.unpack_db_at_x_into_dir_at_y(
        Path(source_dir),
        Path(target_dir)
    )


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
