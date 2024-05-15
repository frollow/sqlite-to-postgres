import uuid
from dataclasses import dataclass, field
from datetime import date, datetime


# Data classes for database models
@dataclass
class Genre:
    name: str
    description: str = ""
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
    modified: datetime = field(default_factory=datetime.now())


@dataclass
class Person:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
    modified: datetime = field(default_factory=datetime.now())


@dataclass
class Filmwork:
    title: str
    description: str = ""
    rating: float = field(default=0.0)
    type: str = "MOV"
    certificate: str = ""
    file_path: str = ""
    creation_date: date = None
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
    modified: datetime = field(default_factory=datetime.now())


@dataclass
class GenreFilmwork:
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())


@dataclass
class PersonFilmwork:
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    created: datetime = field(default_factory=datetime.now())
