import unittest
from typing import Dict

import sqlglot.expressions as exp

from frango.table_def import Article
from frango.sql_adaptor import sql_eval, sql_parse_one, SQLVal, sql_to_str


# noinspection SqlNoDataSourceInspection
class TestSQLGen(unittest.TestCase):
    def test_eval(self) -> None:
        item: Dict[str, SQLVal] = {"id": 1, "age": 4, "gender": "male"}
        self.assertEqual(sql_eval(sql_parse_one("id == 1"), item), True)
        self.assertEqual(sql_eval(sql_parse_one("id == 1 AND age < 5"), item), True)
        self.assertEqual(sql_eval(sql_parse_one("id > 3 AND age < 5"), item), False)
        self.assertEqual(sql_eval(sql_parse_one("gender == 'male'"), item), True)
        self.assertEqual(sql_eval(sql_parse_one("gender != 'female'"), item), True)

    def test_basic(self) -> None:
        insert_schema = Article.sql_insert_schema()
        self.assertIsInstance(insert_schema, exp.Schema)
        self.assertEqual(sql_to_str(insert_schema),
                         'Article (id, timestamp, aid, title, category, '
                         'abstract, articleTags, authors, language, image, video)')

        create_sql = Article.sql_create()
        self.assertIsInstance(create_sql, exp.Create)
        self.assertEqual(sql_to_str(create_sql),
                         'CREATE TABLE Article (id TEXT, timestamp INTEGER, aid INTEGER PRIMARY KEY, title TEXT, '
                         'category TEXT, abstract TEXT, articleTags TEXT, authors TEXT, language TEXT, image TEXT, '
                         'video TEXT)')

        article = Article(id="a1", timestamp=1, aid=1, title="Hello", category="World",
                          abstract="Hello", articleTags="tag", authors="John", language="English",
                          image='img1,img2', video='video1,video2')
        insert_tuple = article.sql_insert_tuple()
        self.assertIsInstance(insert_tuple, exp.Tuple)
        self.assertEqual(sql_to_str(insert_tuple), "('a1', 1, 1, 'Hello', 'World', 'Hello', 'tag', 'John', "
                                                   "'English', 'img1,img2', 'video1,video2')")

        insert_sql_with_placeholder = Article.sql_insert_with_placeholder()
        print('\n')
        print(insert_sql_with_placeholder.__repr__())
        print(sql_to_str(insert_sql_with_placeholder))


if __name__ == '__main__':
    unittest.main()
