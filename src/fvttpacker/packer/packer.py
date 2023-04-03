from pathlib import Path
from typing import Dict

from plyvel import DB

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.__common.overwrite_helper import OverwriteHelper
from fvttpacker.__constants import world_db_names
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.overwrite_confirmer import OverwriteConfirmer, AllYesOverwriteConfirmer
from fvttpacker.packer.dict_to_leveldb_writer import DictToLevelDBWriter
from fvttpacker.packer.dir_to_leveldb_reader import DirToDictReader


class Packer:

    @staticmethod
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

        AssertHelper.assert_path_to_parent_target_dir_is_ok(y_path_to_parent_target_dir)
        AssertHelper.assert_path_to_parent_input_dir_is_ok(x_path_to_parent_input_dir)

        input_dir_paths_to_target_db_paths: Dict[Path, Path] = dict()

        for db_name in world_db_names:
            path_to_input_dir = x_path_to_parent_input_dir.joinpath(db_name)

            if not path_to_input_dir.is_dir():
                raise FvttPackerException(f"Missing world data directory {db_name} under {path_to_input_dir}.")

            path_to_target_db = y_path_to_parent_target_dir.joinpath(db_name)

            input_dir_paths_to_target_db_paths[path_to_input_dir] = path_to_target_db

        # ask which existing dbs should be overwritten and filter out the dbs that should not be
        input_dir_paths_to_target_db_paths = OverwriteHelper.ask_and_filter_out_non_overwrite(
            input_dir_paths_to_target_db_paths,
            overwrite_confirmer.confirm_batch_overwrite_leveldb)

        Packer.pack_dirs_into_dbs(input_dir_paths_to_target_db_paths,
                                  skip_input_checks=True)

    @staticmethod
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

        AssertHelper.assert_path_to_parent_target_dir_is_ok(y_path_to_parent_target_dir)
        AssertHelper.assert_path_to_parent_input_dir_is_ok(x_path_to_parent_input_dir)

        input_dir_paths_to_target_db_paths: Dict[Path, Path] = dict()

        for db_name in x_path_to_parent_input_dir.glob("*/"):
            # Remove trailing /
            db_name = db_name[:-1]

            path_to_input_dir = x_path_to_parent_input_dir.joinpath(db_name)

            path_to_target_db = y_path_to_parent_target_dir.joinpath(db_name)

            input_dir_paths_to_target_db_paths[path_to_input_dir] = path_to_target_db

        # ask which existing dbs should be overwritten and filter out the dbs that should not be
        input_dir_paths_to_target_db_paths = OverwriteHelper.ask_and_filter_out_non_overwrite(
            input_dir_paths_to_target_db_paths,
            overwrite_confirmer.confirm_batch_overwrite_leveldb)

        Packer.pack_dirs_into_dbs(input_dir_paths_to_target_db_paths,
                                  skip_input_checks=True)

    @staticmethod
    def pack_dirs_into_dbs(
            input_dir_paths_to_target_db_paths: Dict[Path, Path],
            skip_input_checks=False,
            skip_target_checks=False) -> None:
        """
        Packs all the given directories (keys). Each into its respective LevelDB at the given path (values).

        :param input_dir_paths_to_target_db_paths: Contains the paths to the input directories as keys
        and the paths to the target LevelDBs as values
        :param skip_input_checks:
        :param skip_target_checks:
        """

        path_to_input_dir: Path
        path_to_target_db: Path

        # check input dir paths
        if not skip_input_checks:
            AssertHelper.assert_paths_to_input_dirs_are_ok(input_dir_paths_to_target_db_paths.keys())

        # check target db paths
        if not skip_target_checks:
            AssertHelper.assert_paths_to_target_dbs_are_ok(input_dir_paths_to_target_db_paths.values())

        # read all input directories -> fail fast
        input_dir_paths_to_dicts = DirToDictReader.read_dirs_as_dicts(input_dir_paths_to_target_db_paths.keys())

        # open all the dbs -> fail fast
        input_dir_paths_to_dbs: Dict[Path, DB] = dict()
        for (path_to_input_dir, path_to_target_db) in input_dir_paths_to_target_db_paths.items():
            input_dir_paths_to_dbs[path_to_input_dir] = LevelDBHelper.try_open_db(path_to_target_db,
                                                                                  skip_checks=True,
                                                                                  must_exist=False)

        # coming this far means:
        # - all input directories were successfully read into dicts
        # - all target dbs were successfully opened as LevelDBs
        try:
            # pack all the folders into
            for (path_to_input_dir, target_db) in input_dir_paths_to_dbs.items():
                DictToLevelDBWriter.write_dict_into_db(input_dir_paths_to_dicts[path_to_input_dir],
                                                       target_db)
        finally:
            # close all the dbs
            for target_db in input_dir_paths_to_dbs.values():
                target_db.close()

    @staticmethod
    def pack_dir_into_db_at(
            path_to_input_dir: Path,
            path_to_target_db: Path,
            skip_input_checks=False,
            skip_target_checks=False) -> None:
        """
        Packs the given directory (`path_to_input_dir`) into the leveldb at the given location (`path_to_target_db`).
        If the `path_to_target_db` does not point to an existing LevelDB a new one will be created.
        Each json file will be converted into a db entry.
        The filename will be the key encoded as UTF-8.
        The file content will be the json string without indentations also encoded as UTF-8.


        :param path_to_input_dir: e.g. "./unpacked_dbs/actors"
        :param path_to_target_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        :param skip_input_checks: TODO
        :param skip_target_checks: TODO
        """

        Packer.pack_dirs_into_dbs({path_to_input_dir: path_to_target_db},
                                  skip_input_checks=skip_input_checks,
                                  skip_target_checks=skip_target_checks)
