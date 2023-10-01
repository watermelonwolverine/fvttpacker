import logging
from pathlib import Path
from typing import Dict, List, Iterable

from plyvel import DB

from fvttpacker.__common.__directory_checker_decorators import check_input_dir_and_target_dir, \
    check_input_dbs_and_target_dirs, check_input_dirs_and_target_dbs
from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.__common.overwrite_helper import OverwriteHelper
from fvttpacker.__constants import world_db_names
from fvttpacker.__packer.__dict_to_leveldb_writer import DictToLevelDBWriter
from fvttpacker.__packer.__dir_to_leveldb_reader import DirToDictReader
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.overwrite_confirmer import OverwriteConfirmer, AllYesOverwriteConfirmer


class Packer:

    @staticmethod
    @check_input_dir_and_target_dir
    def pack_world_dirs_under_x_into_dbs_under_y(
            x_path_to_parent_input_dir: Path,
            y_path_to_parent_target_dir: Path,
            overwrite_confirmer: OverwriteConfirmer = AllYesOverwriteConfirmer()) -> None:
        """
        Similar to `pack_dirs_under_x_into_dbs_under_y`, but only packs the sub-directories under the given directory
        (`x_path_to_parent_input_dir`) that belong to a world.
        I.e. "actors", "cards", etc. will be packed, "monster-compendia" will not be packed.

        :param x_path_to_parent_input_dir: e.g. "./unpacked_dbs/test-world"
        :param y_path_to_parent_target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        :param overwrite_confirmer: TODO
        """

        Packer.pack_given_dirs_under_x_into_dbs_under_y(
            x_path_to_parent_input_dir,
            y_path_to_parent_target_dir,
            world_db_names,
            overwrite_confirmer)

    @staticmethod
    @check_input_dir_and_target_dir
    def pack_dirs_under_x_into_dbs_under_y(
            x_path_to_parent_input_dir: Path,
            y_path_to_parent_target_dir: Path,
            overwrite_confirmer: OverwriteConfirmer = AllYesOverwriteConfirmer()) -> None:
        """
        Packs all sub-folders located under the given directory (`x_path_to_parent_input_dir`) into LevelDBs located
        under the given target directory (`parent_target_dir`).

        :param x_path_to_parent_input_dir: e.g. "./unpacked_dbs"
        :param y_path_to_parent_target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        :param overwrite_confirmer: TODO
        """

        db_names: List[str] = list()

        for db_name in x_path_to_parent_input_dir.glob("*/"):
            db_names.append(db_name.name)

        Packer.pack_given_dirs_under_x_into_dbs_under_y(
            x_path_to_parent_input_dir,
            y_path_to_parent_target_dir,
            db_names,
            overwrite_confirmer)

    @staticmethod
    @check_input_dir_and_target_dir
    def pack_given_dirs_under_x_into_dbs_under_y(
            x_path_to_parent_input_dir: Path,
            y_path_to_parent_target_dir: Path,
            db_names: Iterable[str],
            overwrite_confirmer: OverwriteConfirmer = AllYesOverwriteConfirmer()) -> None:

        # mapping input dirs to target dbs
        input_dir_paths_to_target_db_paths: Dict[Path, Path] = dict()

        for db_name in db_names:
            path_to_input_dir = x_path_to_parent_input_dir.joinpath(db_name)

            # all input paths must exist and be dirs
            if not path_to_input_dir.is_dir():
                raise FvttPackerException(f"Missing directory '{db_name}' under '{path_to_input_dir}'.")

            path_to_target_db = y_path_to_parent_target_dir.joinpath(db_name)

            input_dir_paths_to_target_db_paths[path_to_input_dir] = path_to_target_db

        # ask which existing dbs should be overwritten and filter out the dbs that should not be
        input_dir_paths_to_target_db_paths = OverwriteHelper.ask_and_filter_out_non_overwrite(
            input_dir_paths_to_target_db_paths,
            overwrite_confirmer.confirm_batch_overwrite_leveldb)

        Packer.pack_dirs_into_dbs(input_dir_paths_to_target_db_paths,
                                  skip_input_checks=True)

    @staticmethod
    @check_input_dirs_and_target_dbs
    def pack_dirs_into_dbs(
            input_dir_paths_to_target_db_paths: Dict[Path, Path]) -> None:
        """
        Packs all the given directories (keys). Each into its respective LevelDB at the given path (values).

        :param input_dir_paths_to_target_db_paths: Contains the paths to the input directories as keys
        and the paths to the target LevelDBs as values
        """

        path_to_input_dir: Path
        path_to_target_db: Path

        # read all input directories -> fail fast
        input_dir_paths_to_dicts = DirToDictReader.read_dirs_as_dicts(input_dir_paths_to_target_db_paths.keys())

        input_dir_paths_to_dbs: Dict[Path, DB] = dict()

        nb_changes: int = 0

        try:
            # open all the dbs -> fail fast
            for (path_to_input_dir, path_to_target_db) in input_dir_paths_to_target_db_paths.items():
                db = LevelDBHelper.try_open_db(path_to_target_db, skip_checks=True,
                                               must_exist=False)
                input_dir_paths_to_dbs[path_to_input_dir] = db
                logging.debug("Opened LevelDB at '%s' as '%s'",
                              path_to_target_db,
                              hex(id(db)))
            # coming this far means:
            # - all input directories were successfully read into dicts
            # - all target dbs were successfully opened as LevelDBs

            # pack all the folders into
            for (path_to_input_dir, target_db) in input_dir_paths_to_dbs.items():
                nb_changes += DictToLevelDBWriter.write_dict_into_db(input_dir_paths_to_dicts[path_to_input_dir],
                                                                     target_db)
        finally:
            # close all the dbs
            for target_db in input_dir_paths_to_dbs.values():
                target_db.close()

        logging.info("Total number of changes: %s", nb_changes)

    @staticmethod
    def pack_dir_at_x_into_db_at_y(
            x_path_to_input_dir: Path,
            y_path_to_target_db: Path) -> None:
        """
        Packs the given directory (`path_to_input_dir`) into the leveldb at the given location (`path_to_target_db`).
        If the `path_to_target_db` does not point to an existing LevelDB a new one will be created.
        Each json file will be converted into a db entry.
        The filename will be the key encoded as UTF-8.
        The file content will be the json string without indentations also encoded as UTF-8.


        :param x_path_to_input_dir: e.g. "./unpacked_dbs/actors"
        :param y_path_to_target_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        """

        Packer.pack_dirs_into_dbs({x_path_to_input_dir: y_path_to_target_db})
