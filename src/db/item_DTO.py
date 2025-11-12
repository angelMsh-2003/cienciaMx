from dataclasses import dataclass
from datetime import date
from typing import Optional, Any, List

@dataclass
class ItemDTO:
    id: int
    id_repository: int
    title: Optional[str]
    creator: Optional[str]
    subject: Optional[str]
    description: Optional[str]
    publisher: Optional[str]
    contributor: Optional[str]
    date: Optional[date]
    type: Optional[str]
    format: Optional[str]
    identifier: Optional[str]
    source: Optional[str]
    language: Optional[str]
    relation: Optional[str]
    coverage: Optional[str]
    rights: Optional[str]