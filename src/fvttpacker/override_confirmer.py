import sys
from abc import ABC
from io import TextIOWrapper
from pathlib import Path
from typing import List, Dict


class OverrideConfirmer(ABC):

    def confirm_override_leveldb(self,
                                 target_db: Path) -> bool:
        raise NotImplemented()

    def confirm_override_folder(self,
                                target_folder: Path) -> bool:
        raise NotImplemented()

    def confirm_batch_override_leveldb(self,
                                       target_dbs: List[Path]) -> Dict[Path, bool]:
        raise NotImplemented()


class AllYesOverrideConfirmer(OverrideConfirmer):
    def confirm_override_leveldb(self,
                                 target_db: Path) -> bool:
        return True

    def confirm_override_folder(self,
                                target_folder: Path) -> bool:
        return True

    def confirm_batch_override_leveldb(self,
                                       target_dbs: List[Path]) -> Dict[Path, bool]:

        result: Dict[Path, bool] = dict()

        for target_db in target_dbs:
            result[target_db] = True

        return result
