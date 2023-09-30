from pathlib import Path
from typing import Dict, Iterable, List

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__common.overwrite_helper import OverwriteHelper
from fvttpacker.__constants import world_db_names
from fvttpacker.__unpacker.__dict_to_dir_writer import DictToDirWriter
from fvttpacker.__unpacker.__leveldb_to_dict_reader import LevelDBToDictReader
from fvttpacker.__unpacker.__unpacker_decorator import check_input_dir_and_target_dir, check_input_dirs_and_target_dirs
from fvttpacker.fvttpacker_exception import FvttPackerException
from fvttpacker.overwrite_confirmer import OverwriteConfirmer, AllYesOverwriteConfirmer


class Unpacker:

    @staticmethod
    @check_input_dir_and_target_dir
    def unpack_world_dbs_under_x_into_dirs_under_y(
            x_path_to_parent_input_dir: Path,
            y_path_to_parent_target_dir: Path,
            overwrite_confirmer: OverwriteConfirmer = AllYesOverwriteConfirmer()) -> None:
        """
        Similar to `unpack_dbs_under_x_into_dirs_under_y`, but only unpacks the LevelDBs under the given directory
        (`x_path_to_parent_input_dir`) that belong to a world and ignores the rest.

        :param x_path_to_parent_input_dir: e.g. "./foundrydata/Data/worlds/test/data"
        :param y_path_to_parent_target_dir: e.g. "./unpack_result"
        :param overwrite_confirmer: TODO
        """

        Unpacker.unpack_given_dbs_under_x_into_dirs_under_y(x_path_to_parent_input_dir,
                                                            y_path_to_parent_target_dir,
                                                            world_db_names,
                                                            overwrite_confirmer)

    @staticmethod
    @check_input_dir_and_target_dir
    def unpack_all_dbs_under_x_into_dirs_under_y(
            x_path_to_parent_input_dir: Path,
            y_path_to_parent_target_dir: Path,
            overwrite_confirmer: OverwriteConfirmer = AllYesOverwriteConfirmer()) -> None:
        """
        Unpacks all LevelDBs in the given directory (`x_path_to_parent_input_dir`) into sub-folders of the given target
        directory (`y_path_to_parent_target_dir').

        :param x_path_to_parent_input_dir: e.g. "./foundrydata/Data/modules/shared-module/packs"
        :param y_path_to_parent_target_dir: e.g. "unpack_result"
        :param overwrite_confirmer: TODO
        """

        db_names: List[str] = list()

        for db_name in x_path_to_parent_input_dir.glob("*/"):
            # remove trailing /
            db_names.append(db_name[:-1])

        Unpacker.unpack_given_dbs_under_x_into_dirs_under_y(x_path_to_parent_input_dir,
                                                            y_path_to_parent_target_dir,
                                                            db_names,
                                                            overwrite_confirmer)

    @staticmethod
    @check_input_dir_and_target_dir
    def unpack_given_dbs_under_x_into_dirs_under_y(
            x_path_to_parent_input_dir: Path,
            y_path_to_parent_target_dir: Path,
            db_names: Iterable[str],
            overwrite_confirmer: OverwriteConfirmer = AllYesOverwriteConfirmer()) -> None:

        # map input dbs to target directories
        input_db_paths_to_target_dir_paths: Dict[Path, Path] = dict()

        for db_name in db_names:

            path_to_input_db = x_path_to_parent_input_dir.joinpath(db_name)
            path_to_target_dir = y_path_to_parent_target_dir.joinpath(db_name)

            # input dbs must all be directories
            if not path_to_input_db.is_dir():
                raise FvttPackerException(f"Missing LevelDB {db_name} under {x_path_to_parent_input_dir}.")

            input_db_paths_to_target_dir_paths[path_to_input_db] = path_to_target_dir

        # done mapping input dbs to target directories

        # ask which existing dirs should be overwritten and filter out the dirs that should not be
        input_db_paths_to_target_dir_paths = OverwriteHelper.ask_and_filter_out_non_overwrite(
            input_db_paths_to_target_dir_paths,
            overwrite_confirmer.confirm_batch_overwrite_dirs)

        # finally, do the unpacking
        Unpacker.unpack_dbs_into_dirs(input_db_paths_to_target_dir_paths)

    @staticmethod
    @check_input_dirs_and_target_dirs
    def unpack_dbs_into_dirs(
            input_db_paths_to_target_dir_paths: Dict[Path, Path]):
        """
        Unpacks all the given LevelDB at the given Paths (keys).
        Each into its respective directory at the given Path (values).

        :param input_db_paths_to_target_dir_paths: Contains the paths to the input LevelDBs as keys
        and the paths to the target directories as values
        """

        path_to_input_db: Path
        path_to_target_dir: Path

        # read all input dbs -> fail fast
        input_db_paths_to_dicts: Dict[Path, Dict[str, Dict]] = LevelDBToDictReader.read_dbs_as_dicts(
            input_db_paths_to_target_dir_paths.keys())

        for (path_to_input_db, path_to_target_dir) in input_db_paths_to_target_dir_paths.items():
            DictToDirWriter.write_dict_into_dir(input_db_paths_to_dicts[path_to_input_db],
                                                path_to_target_dir,
                                                skip_checks=True)

    @staticmethod
    def unpack_db_at_x_into_dir_at_y(x_path_to_input_db: Path,
                                     y_path_to_target_dir: Path,
                                     skip_input_checks: bool,
                                     skip_target_checks: bool) -> None:
        """
        Unpacks the leveldb at the LevelDB at the given Path (`x_path_to_input_db`) into the directory at the given
        target Path (`y_path_to_target_dir`).

        :param x_path_to_input_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        :param y_path_to_target_dir: e.g. "./unpack_result/actors"
        :param skip_input_checks: TODO
        :param skip_target_checks: TODO
        """

        Unpacker.unpack_dbs_into_dirs({x_path_to_input_db: y_path_to_target_dir},
                                      skip_input_checks=skip_input_checks,
                                      skip_target_checks=skip_target_checks)
