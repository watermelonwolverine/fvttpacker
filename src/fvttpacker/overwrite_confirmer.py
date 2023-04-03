from abc import ABC
from pathlib import Path
from typing import List, Dict


class OverwriteConfirmer(ABC):

    def confirm_overwrite_leveldb(self,
                                  path_to_target_db: Path) -> bool:
        raise NotImplemented()

    def confirm_overwrite_folder(self,
                                 path_to_target_dir: Path) -> bool:
        raise NotImplemented()

    def confirm_batch_overwrite_leveldb(self,
                                        paths_to_target_dbs: List[Path]) -> Dict[Path, bool]:
        raise NotImplemented()

    def confirm_batch_overwrite_dirs(self,
                                     paths_to_target_dirs: List[Path]) -> Dict[Path, bool]:
        raise NotImplemented()


class AllYesOverwriteConfirmer(OverwriteConfirmer):
    def confirm_overwrite_leveldb(self,
                                  path_to_target_db: Path) -> bool:
        return True

    def confirm_overwrite_folder(self,
                                 path_to_target_dir: Path) -> bool:
        return True

    def confirm_batch_overwrite_leveldb(self,
                                        paths_to_target_dbs: List[Path]) -> Dict[Path, bool]:

        result: Dict[Path, bool] = dict()

        for target_db in paths_to_target_dbs:
            result[target_db] = True

        return result

    def confirm_batch_overwrite_dirs(self,
                                     paths_to_target_dirs: List[Path]) -> Dict[Path, bool]:

        result: Dict[Path, bool] = dict()

        for path_to_target_dir in paths_to_target_dirs:
            result[path_to_target_dir] = True

        return result
