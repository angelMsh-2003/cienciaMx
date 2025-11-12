from dataclasses import dataclass
from datetime import date
from typing import Optional, Any, List

@dataclass
class RepositoryDTO:
    id: int
    name: str
    url: str
    last_update: date
    alive: bool