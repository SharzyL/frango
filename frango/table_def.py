from dataclasses import dataclass
from dataclass_wizard import LoadMixin

from frango.sql_adaptor import SQLDef


@dataclass
class Article(LoadMixin, SQLDef):
    id: str
    timestamp: int
    aid: int
    title: str
    category: str
    abstract: str
    articleTags: str
    authors: str
    language: str
    image: str
    video: str

    @staticmethod
    def __primary_key__():
        return "aid"


@dataclass
class User(SQLDef):
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

    @staticmethod
    def __primary_key__():
        return "uid"


@dataclass
class Read(SQLDef):
    timestamp: int
    id: str
    uid: int
    aid: int
    readTimeLength: int
    agreeOrNot: bool
    commentOrNot: bool
    shareOrNot: bool
    commentDetail: str
