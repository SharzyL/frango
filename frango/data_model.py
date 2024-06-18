from dataclasses import dataclass
from dataclass_wizard import JSONWizard, LoadMixin
from typing import List, get_args, get_origin

import sqlglot.expressions as exp


def anno_to_type(anno) -> exp.DataType.Type:
    if anno == str:
        return exp.DataType.Type.TEXT
    elif anno == int:
        return exp.DataType.Type.INT
    elif get_origin(anno) == list or get_origin(anno) == List:
        return exp.DataType.Type.TEXT  # we will join list with ','


class SQLDef(JSONWizard):
    @classmethod
    def insert_sql_schema(cls) -> exp.Schema:
        cls_annotations = cls.__annotations__
        name = cls.__name__
        schema = exp.Schema(this=exp.Table(this=name, quoted=False))
        for field, anno in cls_annotations.items():
            schema.append('expressions', exp.Identifier(this=field, quoted=False))
        return schema

    def insert_sql_tuple(self) -> exp.Tuple:
        cls_annotations = self.__annotations__
        tuple_ = exp.Tuple()
        for field in cls_annotations:
            val = getattr(self, field)
            if isinstance(val, str):
                tuple_.append('expressions', exp.Literal(this=val, is_string=True))
            elif isinstance(val, int) or isinstance(val, float):
                tuple_.append('expressions', exp.Literal(this=str(val), is_string=False))
            elif isinstance(val, list):
                val_string = ','.join(str(val_entry) for val_entry in val)
                tuple_.append('expressions', exp.Literal(this=val_string, is_string=True))
            else:
                assert False, f'unsupported value type for {repr(tuple_)}'

        return tuple_

    @classmethod
    def create_sql(cls) -> exp.Create:
        pkey = None
        if hasattr(cls, "__primary_key__"):
            pkey = cls.__primary_key__()
        cls_annotations = cls.__annotations__
        table_name = cls.__name__
        table_ident = exp.Identifier(this=table_name, quooted=False)
        schema = exp.Schema(this=exp.Table(this=table_ident, quoted=False))
        for field, anno in cls_annotations.items():
            ident = exp.Identifier(this=field, quooted=False)
            kind = anno_to_type(anno)
            column_def = exp.ColumnDef(this=ident, kind=exp.DataType(this=kind, nested=False))
            if field == pkey:
                column_def.append('constraints', exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint()))
            schema.append('expressions', column_def)
        return exp.Create(this=schema, kind="TABLE")


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
    image: List[str]
    video: List[str]

    @staticmethod
    def load_to_iterable(o, base_type, elem_parser):
        return base_type([elem_parser(elem) for elem in o.split(',') if len(elem) > 0])

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


import unittest


class TestSQLGen(unittest.TestCase):
    def test_basic(self):
        insert_schema = Article.insert_sql_schema()
        self.assertIsInstance(insert_schema, exp.Schema)
        self.assertEqual(insert_schema.sql(),
                         'Article (id, timestamp, aid, title, category, '
                         'abstract, articleTags, authors, language, image, video)')

        create_sql = Article.create_sql()
        self.assertIsInstance(create_sql, exp.Create)
        self.assertEqual(create_sql.sql(), 'CREATE TABLE Article '
                                           '(id TEXT, timestamp INT, aid INT PRIMARY KEY, title TEXT, category TEXT, '
                                           'abstract TEXT, articleTags TEXT, authors TEXT, language TEXT, image TEXT, '
                                           'video TEXT)')

        article = Article(id="a1", timestamp=1, aid=1, title="Hello", category="World",
                          abstract="Hello", articleTags="tag", authors="John", language="English",
                          image=['img1', 'img2'], video=['video1', 'video2'])
        insert_tuple = article.insert_sql_tuple()
        self.assertIsInstance(insert_tuple, exp.Tuple)
        self.assertEqual(insert_tuple.sql(), "('a1', 1, 1, 'Hello', 'World', 'Hello', 'tag', 'John', "
                                             "'English', 'img1,img2', 'video1,video2')")


if __name__ == '__main__':
    unittest.main()
