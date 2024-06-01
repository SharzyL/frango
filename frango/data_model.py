from dataclasses import dataclass, field
from dataclass_wizard import JSONWizard, LoadMixin
from typing import List


@dataclass
class Article(JSONWizard, LoadMixin):
    id: str
    timestamp: int
    aid: int
    title: str
    category: str
    abstract: str
    articleTags: str
    authors: str
    language: str
    image: List[str]
    video: List[str]

    @staticmethod
    def load_to_iterable(o, base_type, elem_parser):
        return base_type([elem_parser(elem) for elem in o.split(',') if len(elem) > 0])


@dataclass
class User(JSONWizard):
    timestamp: int
    id: str
    uid: int
    name: str
    gender: str
    email: str
    phone: str
    dept: str
    grade: str
    language: str
    region: str
    role: str
    preferTags: str
    obtainedCredits: int


@dataclass
class Read(JSONWizard):
    timestamp: int
    id: str
    uid: int
    aid: int
    readTimeLength: int
    agreeOrNot: bool
    commentOrNot: bool
    shareOrNot: bool
    commentDetail: str
