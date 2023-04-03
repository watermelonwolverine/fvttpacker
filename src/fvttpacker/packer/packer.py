import logging
from pathlib import Path
from typing import Dict

from plyvel import DB

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__common.leveldb_helper import LevelDBHelper
from fvttpacker.__common.override_helper import OverrideHelper
from fvttpacker.__constants import world_db_names
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.override_confirmer import OverrideConfirmer, AllYesOverrideConfirmer
from fvttpacker.packer.dict_to_leveldb_writer import DictToLevelDBWriter
from fvttpacker.packer.dir_to_leveldb_reader import DirToDictReader


class Packer:

    def __init__(self,
                 override_confirmer: OverrideConfirmer = AllYesOverrideConfirmer()):

        self.override_confirmer = override_confirmer

    def pack_world_into_dbs_under(self,
                                  path_to_parent_input_dir: Path,
                                  path_to_parent_target_dir: Path) -> None:
        """
        Similar to `pack_subdirs_into_dbs_under`, but only packs the sub-folders under the given directory
        (`path_to_parent_input_dir`) that belong to a world.
        E.g. "actors", "cards", etc. will be packed, "monster-compendia" will not be packed.
        :param path_to_parent_input_dir: e.g. "./unpacked_dbs/test-world"
        :param path_to_parent_target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        """

        AssertHelper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)
        AssertHelper.assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir)

        input_dir_paths_to_target_db_paths: Dict[Path, Path] = dict()

        for db_name in world_db_names:
            path_to_input_dir = path_to_parent_input_dir.joinpath(db_name)

            if not path_to_input_dir.is_dir():
                raise FvttPackerException(f"Missing world data directory {db_name} under {path_to_input_dir}.")

            path_to_target_db = path_to_parent_target_dir.joinpath(db_name)

            input_dir_paths_to_target_db_paths[path_to_input_dir] = path_to_target_db

        self.pack_dirs_into_dbs(input_dir_paths_to_target_db_paths,
                                skip_input_checks=True)

    def pack_subdirs_into_dbs_under(self,
                                    path_to_parent_input_dir: Path,
                                    path_to_parent_target_dir: Path) -> None:
        """
        Packs all sub-folders located under the given directory (`parent_input_dir`) into LevelDBs located under the
        given target directory (`parent_target_dir`).

        :param path_to_parent_input_dir: e.g. "./unpacked_dbs"
        :param path_to_parent_target_dir: e.g. "./foundrydata/Data/worlds/test/data"
        """

        AssertHelper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)
        AssertHelper.assert_path_to_parent_input_dir_is_ok(path_to_parent_input_dir)

        input_dir_paths_to_target_db_paths: Dict[Path, Path] = dict()

        for db_name in path_to_parent_input_dir.glob("*/"):
            # Remove trailing /
            db_name = db_name[:-1]

            path_to_input_dir = path_to_parent_input_dir.joinpath(db_name)

            path_to_target_db = path_to_parent_target_dir.joinpath(db_name)

            input_dir_paths_to_target_db_paths[path_to_input_dir] = path_to_target_db

        self.pack_dirs_into_dbs(input_dir_paths_to_target_db_paths,
                                skip_input_checks=True)

    def pack_dirs_into_dbs(self,
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

        # ask which existing dbs should be overriden and filter out the dbs that should not be overriden
        input_dir_paths_to_target_db_paths = OverrideHelper.ask_and_filter_out_non_override(
            input_dir_paths_to_target_db_paths,
            self.override_confirmer.confirm_batch_override_leveldb)

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

    def pack_dir_into_db_at(self,
                            path_to_input_dir: Path,
                            path_to_target_db: Path,
                            skip_input_checks=False,
                            skip_target_checks=False) -> None:
        """
        Packs the given directory (`input_dir`) into the leveldb at the given location (`path_to_target_db`).
        If the `path_to_target_db` does not point to an existing LevelDB a new one will be created.
        Each json file will be converted into a db entry.
        The filename will be the key encoded as UTF-8.
        The file content will be the json string without indentations also encoded as UTF-8.


        :param path_to_input_dir: e.g. "./unpacked_dbs/actors"
        :param path_to_target_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        :param skip_input_checks:
        :param skip_target_checks:
        """

        logging.info("Packing directory %s into db at %s",
                     path_to_input_dir,
                     path_to_target_db)

        if not skip_input_checks:
            AssertHelper.assert_path_to_input_dir_is_ok(path_to_input_dir)
        if not skip_target_checks:
            LevelDBHelper.assert_path_to_db_is_ok(path_to_target_db,
                                                  must_exist=False)

        target_db = LevelDBHelper.try_open_db(path_to_target_db,
                                              skip_checks=True,
                                              must_exist=False)

        logging.debug("Opened LevelDB at '%s' as '%s'",
                      path_to_target_db,
                      hex(id(target_db)))

        self.pack_dir_into_db(path_to_input_dir,
                              target_db,
                              skip_input_checks=True)

        target_db.close()

    @staticmethod
    def pack_dir_into_db(path_to_input_dir: Path,
                         target_db: DB,
                         skip_input_checks=False) -> None:
        """
        Packs the given directory (`input_dir`) into the given LevelDB `target_db`
        :param path_to_input_dir: The directory to pack
        :param target_db: The LevelDB to pack the `input_dir` into
        :param skip_input_checks:
        :return:
        """

        logging.info("Packing directory '%s' into db '%s'",
                     path_to_input_dir,
                     hex(id(target_db)))

        if not skip_input_checks:
            AssertHelper.assert_path_to_input_dir_is_ok(path_to_input_dir)

        # read folder as a whole.
        input_dict = DirToDictReader.read_dir_as_dict(path_to_input_dir,
                                                      skip_checks=True)

        logging.debug("Read directory '%s' as dict '%s'",
                      path_to_input_dir,
                      hex(id(input_dict)))

        # avoid rewriting whole database everytime
        # instead only re-write updated entries
        DictToLevelDBWriter.write_dict_into_db(input_dict,
                                               target_db)
