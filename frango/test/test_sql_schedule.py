from typing import Dict
import unittest
from dataclasses import dataclass

import sqlglot.expressions as exp

from frango.config import Config
from frango.sql_adaptor import SQLDef, PARAMS_ARG_KEY, sql_to_str, sql_parse_one
from frango.node.scheduler import (
    RegularTableSplitter, Scheduler,
    SerialExecutionPlan, DistributedExecutionPlan, LocalExecutionPlan,
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
            SELECT id, timestamp FROM Article WHERE category == 'music';
            INSERT INTO Article (aid, category) VALUES (100, 'music'), (200, 'politics');
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

        def verify_bulk_load_plan(node_id: int, person_inserts: int, read_inserts: int) -> None:
            plan = sch.schedule_bulk_load_for_node({"Person": persons, "Read": reads}, node_id)
            assert isinstance(plan, SerialExecutionPlan)
            step_person = plan.steps[0]
            assert isinstance(step_person, LocalExecutionPlan)
            self.assertEqual(len(step_person.query.args[PARAMS_ARG_KEY]), person_inserts)

            step_read = plan.steps[1]
            assert isinstance(step_read, LocalExecutionPlan)
            self.assertEqual(len(step_read.query.args[PARAMS_ARG_KEY]), read_inserts)

        verify_bulk_load_plan(1, 2, 1)
        verify_bulk_load_plan(2, 1, 1)
        verify_bulk_load_plan(3, 2, 3)

        insert_plan = sch.schedule_query("INSERT INTO Read (id, date) VALUES (100, '2001')")
        assert isinstance(insert_plan, SerialExecutionPlan)
        self.assertEqual(len(insert_plan.steps), 1)
        step0 = insert_plan.steps[0]
        assert isinstance(step0, DistributedExecutionPlan)
        self.assertEqual(
            sql_to_str(step0.queries_for_node[1]),
            "INSERT INTO Read (id, date) SELECT 100, '2001' FROM Person WHERE id = 100 AND Person.id > 4"
        )
        self.assertEqual(
            sql_to_str(step0.queries_for_node[2]),
            "INSERT INTO Read (id, date) SELECT 100, '2001' FROM Person WHERE id = 100 AND Person.id <= 4 AND Person.gender = 'male'"
        )
        self.assertEqual(
            sql_to_str(step0.queries_for_node[3]),
            "INSERT INTO Read (id, date) SELECT 100, '2001' FROM Person WHERE id = 100 AND Person.id <= 4 AND Person.gender <> 'male'"
        )


if __name__ == '__main__':
    unittest.main()
