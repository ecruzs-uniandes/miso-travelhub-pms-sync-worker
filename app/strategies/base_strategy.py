from abc import ABC, abstractmethod
from sqlalchemy.orm import Session
from app.schemas.sync_command import SyncCommand


class BaseStrategy(ABC):
    @abstractmethod
    def execute(self, command: SyncCommand, db: Session) -> None:
        pass
