import unittest
from dataclasses import dataclass

from frango.config import Partition
from frango.sql_adaptor import SQLDef
from frango.node.sql_schedule import RegularTableSplitter, Scheduler, sql_to_str, sql_parse_one, _sql_eval


# noinspection SqlNoDataSourceInspection
class TestSplit(unittest.TestCase):
    # noinspection SqlResolve
    def test_basic(self):
        splitter = RegularTableSplitter(Partition(type="regular", filter={
            1: "id > 4",
            2: "id <= 4"
        }))
        query = sql_parse_one("SELECT id, timestamp FROM Article WHERE id > 3 AND name == 'harry'")
        splits = splitter.partition_select_query(query)
        self.assertEqual(sql_to_str(splits[1]),
                         "SELECT id, timestamp FROM Article WHERE (id > 3 AND name = 'harry') AND id > 4")
        self.assertEqual(sql_to_str(splits[2]),
                         "SELECT id, timestamp FROM Article WHERE (id > 3 AND name = 'harry') AND id <= 4")

    def test_eval(self):
        item = {"id": 1, "age": 4, "gender": "male"}
        self.assertEqual(_sql_eval(sql_parse_one("id == 1"), item), True)
        self.assertEqual(_sql_eval(sql_parse_one("id == 1 AND age < 5"), item), True)
        self.assertEqual(_sql_eval(sql_parse_one("id > 3 AND age < 5"), item), False)
        self.assertEqual(_sql_eval(sql_parse_one('gender == "male"'), item), True)
        self.assertEqual(_sql_eval(sql_parse_one("gender != 'female'"), item), True)

    def test_find_partition(self):
        splitter = RegularTableSplitter(Partition(type="regular", filter={
            1: "id > 4",
            2: "id <= 4 AND gender == 'male'",
            3: "id <= 4 AND gender != 'male'",
        }))
        self.assertEqual(splitter.get_belonging_nodes({"id": 5, "gender": "male"}), [1])
        self.assertEqual(splitter.get_belonging_nodes({"id": 0, "gender": "male"}), [2])
        self.assertEqual(splitter.get_belonging_nodes({"id": 0, "gender": "female"}), [3])


# noinspection SqlNoDataSourceInspection
class TestSchedule(unittest.TestCase):
    # noinspection SqlResolve
    def test_basic(self):
        partitions = {
            "Person": Partition(type="regular", filter={
                1: "id > 4",
                2: "id <= 4 AND gender == 'male'",
                3: "id <= 4 AND gender != 'male'",
            }),
            "Article": Partition(type="regular", filter={
                1: "aid > 100",
                2: "aid <= 100"
            })
        }
        sch = Scheduler(partitions, node_id_list=[1, 2, 3])
        plan = sch.schedule_query('''
            SELECT id, timestamp FROM Article WHERE category == "music";
            INSERT INTO Article (aid, category) VALUES (100, "music"), (200, "politics");
        ''')
        self.assertEqual(len(plan[1]), 2)
        self.assertEqual(len(plan[2]), 2)
        self.assertEqual(len(plan[3]), 0)
        self.assertIn("politics", sql_to_str(plan[1][1]))
        self.assertIn("music", sql_to_str(plan[2][1]))

    def test_dependent(self):
        @dataclass
        class Person(SQLDef):
            id: int
            gender: str

        @dataclass
        class Read(SQLDef):
            id: int
            date: str

        partitions = {
            "Person": Partition(type="regular", filter={
                1: "id > 4",
                2: "id <= 4 AND gender == 'male'",
                3: "id <= 4 AND gender != 'male'",
            }),
            "Read": Partition(type="dependent", dependentKey="id", dependentTable="Person")
        }
        sch = Scheduler(partitions, node_id_list=[1, 2, 3])
        persons = [
            Person(1, "male"),
            Person(2, "female"),
            Person(4, "female"),
            Person(5, "male"),
            Person(7, "female")
        ]

        reads = [
            Read(1, "2001"),
            Read(5, "2002"),
            Read(2, "2003"),
            Read(2, "2005"),
            Read(4, "2004"),
        ]

        def verify_plan(node_id, person_inserts, read_inserts):
            plan = sch.schedule_bulk_load_for_node({"Person": persons, "Read": reads}, node_id)
            self.assertEqual(len(plan[0].expression.expressions), person_inserts)
            self.assertEqual(len(plan[1].expression.expressions), read_inserts)

        verify_plan(1, 2, 1)
        verify_plan(2, 1, 1)
        verify_plan(3, 2, 3)


if __name__ == '__main__':
    unittest.main()
