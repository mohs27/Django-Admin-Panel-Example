import uuid
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class FilmWork:
    created: datetime
    modified: datetime
    title: str
    description: str
    type: str
    creation_date: datetime
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Genre:
    created: datetime
    modified: datetime
    id: uuid.UUID
    name: str
    description: str


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    full_name: str
    created: datetime
    modified: datetime
    gender: str


@dataclass(frozen=True)
class PersonFilmWork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created: datetime


@dataclass(frozen=True)
class GenreFilmWork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created: datetime
