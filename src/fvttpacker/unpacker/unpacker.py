import json
from pathlib import Path

import plyvel
from plyvel import DB

from fvttpacker.__common.assert_helper import AssertHelper
from fvttpacker.__constants import world_db_names, UTF_8


class Unpacker:

    def unpack_world(self,
                     path_to_input_world_data_dir: Path,
                     path_to_parent_target_dir: Path) -> None:
        """
        Similar to `unpack_dbs_into_dirs_under`, but only unpacks the dbs under the given directory
        (`path_to_input_world_data_dir`) that belong to a world and ignores the rest.
        :param path_to_input_world_data_dir: "./foundrydata/Data/worlds/test/data"
        :param path_to_parent_target_dir: "./unpack_result"
        """

        AssertHelper.assert_path_to_parent_input_dir_is_ok(path_to_input_world_data_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)

        for db_name in world_db_names:
            path_to_db = path_to_input_world_data_dir.joinpath(db_name)
            target_dir = path_to_parent_target_dir.joinpath(db_name)

            self.unpack_db(path_to_db,
                           target_dir)

    def unpack_dbs_into_dirs_under(self,
                                   path_to_input_dbs_parent_dir: Path,
                                   path_to_parent_target_dir: Path) -> None:
        """
        Unpacks all LevelDBs in the given directory (`path_to_input_dbs_parent_dir`) into sub-folders of the given target
        directory (`path_to_parent_target_dir').
        :param path_to_input_dbs_parent_dir: e.g. "./foundrydata/Data/modules/shared-module/packs"
        :param path_to_parent_target_dir: e.g. "unpack_result"
        """

        AssertHelper.assert_path_to_parent_input_dir_is_ok(path_to_input_dbs_parent_dir)
        AssertHelper.assert_path_to_parent_target_dir_is_ok(path_to_parent_target_dir)

        for path_to_db in path_to_input_dbs_parent_dir.glob("*/"):
            target_dir = path_to_parent_target_dir.joinpath(path_to_db.name)

            self.unpack_db(path_to_db,
                           target_dir)

    def unpack_db(self,
                  path_to_db: Path,
                  target_dir: Path) -> None:
        """
        Unpacks the leveldb at the given `path_to_db` into a new folder under the given `output_dir`
        :param path_to_db: e.g. "./foundrydata/Data/worlds/test/data/actors"
        :param target_dir: e.g. "./unpack_result/actors"
        """

        if not target_dir.exists():
            target_dir.mkdir()

        db = plyvel.DB(str(path_to_db))

        self.__unpack_db_into_folder(db,
                                     target_dir)

        db.close()

    def __unpack_db_into_folder(self,
                                db: DB,
                                target_dir: Path) -> None:
        """
        Unpacks the given `db` into the given `target_dir`
        :param db: leveldb to unpack
        :param target_dir: e.g. "./unpack_result/actors"
        """

        for entry in db.iterator():
            key: bytes = entry[0]
            value: bytes = entry[1]

            json_dict = json.loads(value.decode(UTF_8))

            target_file = target_dir.joinpath(key.decode(UTF_8) + ".json")

            with open(target_file, "wt+", encoding=UTF_8) as file:
                json.dump(json_dict,
                          file,
                          indent="  ")
