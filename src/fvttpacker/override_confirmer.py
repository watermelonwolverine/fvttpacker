import sys
from abc import ABC
from io import TextIOWrapper
from pathlib import Path


class OverrideConfirmer(ABC):

    def confirm_override_leveldb(self,
                                 target_db: Path) -> bool:
        raise NotImplemented()

    def confirm_override_folder(self,
                                target_folder: Path) -> bool:
        raise NotImplemented()


class AllYesOverrideConfirmer(OverrideConfirmer):
    def confirm_override_leveldb(self,
                                 target_db: Path) -> bool:
        return True

    def confirm_override_folder(self,
                                target_folder: Path) -> bool:
        return True
