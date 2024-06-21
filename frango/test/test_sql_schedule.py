from typing import Dict
import unittest
from dataclasses import dataclass

import sqlglot.expressions as exp

from frango.config import Config
from frango.sql_adaptor import SQLDef
from frango.node.scheduler import (
    RegularTableSplitter, Scheduler, sql_to_str, sql_parse_one, _sql_eval,
    SerialExecutionPlan, DistributedExecutionPlan, LocalExecutionPlan, SQLVal
)


# noinspection SqlNoDataSourceInspection
class TestSplit(unittest.TestCase):
    # noinspection SqlResolve
    def test_basic(self) -> None:
        splitter = RegularTableSplitter(Config.Partition(type="regular", filter={
            1: "id > 4",
            2: "id <= 4"
        }), table_name="User")
        query = sql_parse_one("SELECT id, timestamp FROM Article WHERE id > 3 AND name == 'harry'")
        assert isinstance(query, exp.Select)
        splits = splitter.partition_select_query(query)
        self.assertEqual(sql_to_str(splits[1]),
                         "SELECT id, timestamp FROM Article WHERE (id > 3 AND name = 'harry') AND id > 4")
        self.assertEqual(sql_to_str(splits[2]),
                         "SELECT id, timestamp FROM Article WHERE (id > 3 AND name = 'harry') AND id <= 4")

    def test_eval(self) -> None:
        item: Dict[str, SQLVal] = {"id": 1, "age": 4, "gender": "male"}
        self.assertEqual(_sql_eval(sql_parse_one("id == 1"), item), True)
        self.assertEqual(_sql_eval(sql_parse_one("id == 1 AND age < 5"), item), True)
        self.assertEqual(_sql_eval(sql_parse_one("id > 3 AND age < 5"), item), False)
        self.assertEqual(_sql_eval(sql_parse_one('gender == "male"'), item), True)
        self.assertEqual(_sql_eval(sql_parse_one("gender != 'female'"), item), True)

    def test_find_partition(self) -> None:
        splitter = RegularTableSplitter(Config.Partition(type="regular", filter={
            1: "id > 4",
            2: "id <= 4 AND gender == 'male'",
            3: "id <= 4 AND gender != 'male'",
        }), table_name="User")
        self.assertEqual(splitter.get_belonging_nodes({"id": 5, "gender": "male"}), [1])
        self.assertEqual(splitter.get_belonging_nodes({"id": 0, "gender": "male"}), [2])
        self.assertEqual(splitter.get_belonging_nodes({"id": 0, "gender": "female"}), [3])


# noinspection SqlNoDataSourceInspection
class TestSchedule(unittest.TestCase):
    # noinspection SqlResolve
    def test_basic(self) -> None:
        partitions = {
            "Person": Config.Partition(type="regular", filter={
                1: "id > 4",
                2: "id <= 4 AND gender == 'male'",
                3: "id <= 4 AND gender != 'male'",
            }),
            "Article": Config.Partition(type="regular", filter={
                1: "aid > 100",
                2: "aid <= 100"
            })
        }
        sch = Scheduler(partitions, node_id_list=[1, 2, 3])
        plan = sch.schedule_query('''
            SELECT id, timestamp FROM Article WHERE category == "music";
            INSERT INTO Article (aid, category) VALUES (100, "music"), (200, "politics");
        ''')
        assert isinstance(plan, SerialExecutionPlan)  # not using assertIsInstance since mypy does not support
        insertion_plan = plan.steps[1]
        assert isinstance(insertion_plan, DistributedExecutionPlan)
        self.assertIn(1, insertion_plan.queries_for_node)
        self.assertIn(2, insertion_plan.queries_for_node)
        self.assertNotIn(3, insertion_plan.queries_for_node)
        self.assertIn("politics", sql_to_str(insertion_plan.queries_for_node[1]))
        self.assertIn("music", sql_to_str(insertion_plan.queries_for_node[2]))

    def test_dependent(self) -> None:
        @dataclass
        class Person(SQLDef):
            id: int
            gender: str

        @dataclass
        class Read(SQLDef):
            id: int
            date: str

        partitions = {
            "Person": Config.Partition(type="regular", filter={
                1: "id > 4",
                2: "id <= 4 AND gender == 'male'",
                3: "id <= 4 AND gender != 'male'",
            }),
            "Read": Config.Partition(type="dependent", dependentKey="id", dependentTable="Person")
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

        def verify_plan(node_id: int, person_inserts: int, read_inserts: int) -> None:
            plan = sch.schedule_bulk_load_for_node({"Person": persons, "Read": reads}, node_id)
            assert isinstance(plan, SerialExecutionPlan)
            step_person = plan.steps[0]
            assert isinstance(step_person, LocalExecutionPlan)
            self.assertEqual(len(step_person.query.args['_params']), person_inserts)

            step_read = plan.steps[1]
            assert isinstance(step_read, LocalExecutionPlan)
            self.assertEqual(len(step_read.query.args['_params']), read_inserts)

        verify_plan(1, 2, 1)
        verify_plan(2, 1, 1)
        verify_plan(3, 2, 3)


if __name__ == '__main__':
    unittest.main()
