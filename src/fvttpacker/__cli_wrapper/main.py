from pathlib import Path

import click

from fvttpacker.__cli_wrapper import __args


@click.group()
@click.option(__args.verbosity_option, type=click.Choice(__args.verbosity_choices, case_sensitive=False))
@click.pass_context
def cli(context: click.Context,
        verbosity: str = None) -> None:
    context.obj[__args.verbosity_option] = verbosity

    print("cli() executed")

@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def unpack_world(context: dict,
           source_dir: Path,
           target_dir: Path) -> None:
    print(f"Verbosity is set to {context.obj[__args.verbosity_option]}")

    print("Executing unpack")

@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def pack_world(context: dict,
           source_dir: Path,
           target_dir: Path) -> None:
    print(f"Verbosity is set to {context.obj[__args.verbosity_option]}")

    print("Executing unpack")


@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def unpack_all(context: dict,
           source_dir: Path,
           target_dir: Path) -> None:
    print(f"Verbosity is set to {context.obj[__args.verbosity_option]}")

    print("Executing unpack")

@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def pack_all(context: dict,
           source_dir: Path,
           target_dir: Path) -> None:
    print(f"Verbosity is set to {context.obj[__args.verbosity_option]}")

    print("Executing unpack")

@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def pack(context: dict,
           source_dir: Path,
           target_dir: Path) -> None:
    print(f"Verbosity is set to {context.obj[__args.verbosity_option]}")

    print("Executing unpack")

@cli.command()
@click.pass_context
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path(exists=True))
def unpack(context: dict,
           source_dir: Path,
           target_dir: Path) -> None:
    print(f"Verbosity is set to {context.obj[__args.verbosity_option]}")

    print("Executing unpack")

if __name__ == "__main__":
    cli(obj={})
