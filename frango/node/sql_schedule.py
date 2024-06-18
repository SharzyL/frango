from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Iterable

import sqlglot
import sqlglot.expressions as exp

from frango.config import Partition
from frango.data_model import SQLDef


def _getattr(v, k):
    if isinstance(v, dict):
        return v[k]
    else:
        return getattr(v, k)


def _sql_eval(expr: exp.Expression, item: Optional[SQLDef | dict]) -> int | float | bool | str:
    if isinstance(expr, exp.Column):
        assert isinstance(expr.this, exp.Identifier)
        if expr.this.quoted:
            return str(expr.this.this)
        else:
            assert item is not None
            return _getattr(item, expr.this.this)
    elif isinstance(expr, exp.Literal):
        if expr.is_string:
            return expr.this
        elif '.' in expr.this:
            return float(expr.this)
        else:
            return int(expr.this)
    elif isinstance(expr, exp.Boolean):
        return expr.this

    # unary op
    elif isinstance(expr, exp.Neg):
        return -_sql_eval(expr.this, item)

    # binary op
    elif isinstance(expr, exp.EQ):
        return _sql_eval(expr.this, item) == _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.NEQ):
        return _sql_eval(expr.this, item) != _sql_eval(expr.expression, item)

    elif isinstance(expr, exp.GT):
        return _sql_eval(expr.this, item) > _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.LT):
        return _sql_eval(expr.this, item) < _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.GTE):
        return _sql_eval(expr.this, item) >= _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.LTE):
        return _sql_eval(expr.this, item) <= _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.And):
        return _sql_eval(expr.this, item) and _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.Or):
        return _sql_eval(expr.this, item) or _sql_eval(expr.expression, item)
    elif isinstance(expr, exp.Xor):
        return _sql_eval(expr.this, item) ^ _sql_eval(expr.expression, item)
    else:
        return NotImplemented(f'`{expr}` is not supported')


def sql_parse_one(stmt) -> exp.Expression:
    return sqlglot.parse_one(stmt, dialect='sqlite')


def sql_parse(stmts) -> list[exp.Expression | None]:
    return sqlglot.parse(stmts, dialect='sqlite')


def sql_to_str(sql: exp.Expression | Iterable[exp.Expression]) -> str:
    if isinstance(sql, exp.Expression):
        return sql.sql(dialect='sqlite')
    elif hasattr(sql, '__iter__'):
        return ';'.join(stmt.sql(dialect="sqlite") for stmt in sql)
    else:
        assert False


def eval_literal(expr: exp.Expression):
    if isinstance(expr, exp.Column):
        assert isinstance(expr.this, exp.Identifier)
        assert expr.this.quoted
        return str(expr.this.this)
    elif isinstance(expr, exp.Literal):
        if expr.is_string:
            return expr.this
        elif '.' in expr.this:
            return float(expr.this)
        else:
            return int(expr.this)
    elif isinstance(expr, exp.Boolean):
        return expr.this
    else:
        assert False, f'unsupported expression {repr(expr)}'


class RegularTableSplitter:
    # `rules` maps the node id to its filter string, e.g. `NAME == 'bob' AND AGE > 4'
    def __init__(self, partition: Partition):
        assert partition.type == "regular"
        self.rules: Dict[int, exp.Expression] = {
            node_id: sql_parse_one(cond) for node_id, cond in partition.filter.items()
        }

    def partition_select_query(self, query) -> Dict[int, exp.Select]:
        return {node_id: self._restrict_select_with_rule(query, rule) for node_id, rule in self.rules.items()}

    def get_belonging_nodes(self, item) -> list[int]:
        nodes = []
        for node_id, rule in self.rules.items():
            if _sql_eval(rule, item):
                nodes.append(node_id)
        if not nodes:
            raise RuntimeError(f'No matching rule found for {item}')
        return nodes

    # returns a map from node id to its subquery string
    @staticmethod
    def _restrict_select_with_rule(select: exp.Select, rule: exp.Expression) -> exp.Select:
        assert isinstance(select, exp.Select)
        return select.where(rule)


class Scheduler:
    def __init__(self, partitions: dict[str, Partition], node_id_list: list[int]):
        self.regular_table_partitioners = {
            table: RegularTableSplitter(partition)
            for table, partition in partitions.items()
            if partition.type == "regular"
        }
        self.dependent_partitions = {k: v for k, v in partitions.items() if v.type != "regular"}
        self.node_id_list = node_id_list

    def _schedule_dependent_bulk_load(self, table_name: str, data: list[SQLDef], dependent_table_name: str,
                                      dependent_data: list[SQLDef], node_id: int) -> exp.Insert:
        assert table_name in self.dependent_partitions
        assert dependent_table_name in self.regular_table_partitioners  # not supporting recursive dependency now

        partition = self.dependent_partitions[table_name]
        table_cls = data[0].__class__
        tuples: list[exp.Tuple] = []

        dependent_key = partition.dependentKey
        dependent_index = {_getattr(item, dependent_key): item for item in dependent_data}
        for item in data:
            dependent_key_val = _getattr(item, dependent_key)
            dependent_item = dependent_index[dependent_key_val]
            nodes = self.regular_table_partitioners[dependent_table_name].get_belonging_nodes(dependent_item)
            if node_id in nodes:
                tuples.append(item.insert_sql_tuple())
        insert_stmt = exp.Insert(this=table_cls.insert_sql_schema(), expression=exp.Values(expressions=tuples))

        return insert_stmt

    def schedule_bulk_load_for_node(self, input_tables: dict[str, list[SQLDef]], node_id: int) -> list[exp.Insert]:
        regular_tables = {k: v for k, v in input_tables.items() if k in self.regular_table_partitioners}
        dependent_tables = {k: v for k, v in input_tables.items() if k in self.dependent_partitions}
        assert len(regular_tables) + len(dependent_tables) == len(input_tables)

        stmts = []

        for table_name, table_items in regular_tables.items():
            partitioner = self.regular_table_partitioners[table_name]
            table_cls = table_items[0].__class__
            tuples: list[exp.Tuple] = []
            for item in table_items:
                if node_id in partitioner.get_belonging_nodes(item):
                    tuples.append(item.insert_sql_tuple())
            insert_stmt = exp.Insert(this=table_cls.insert_sql_schema(), expression=exp.Values(expressions=tuples))
            stmts.append(insert_stmt)

        for table_name, table_items in dependent_tables.items():
            dependent_table_name = self.dependent_partitions[table_name].dependentTable
            dependent_table = regular_tables[dependent_table_name]
            insert_stmt = self._schedule_dependent_bulk_load(
                table_name, table_items, dependent_table_name, dependent_table, node_id
            )
            stmts.append(insert_stmt)

        return stmts

    # schedule a query to partitioned database
    def schedule_query(self, query: str) -> dict[int, list[exp.Expression]]:
        stmt_list = sql_parse(query)
        stmt_for_node: dict[int, list[exp.Expression]] = dict()
        for node_id in self.node_id_list:
            stmt_for_node[node_id] = []
        for stmt in stmt_list:
            # CREATE is for every node
            if isinstance(stmt, exp.Create):
                for node_id in self.node_id_list:
                    stmt_for_node[node_id].append(stmt)

            # SELECT and DELETE are thrown to all belonging nodes
            elif isinstance(stmt, exp.Select) or isinstance(stmt, exp.Delete):
                table_name = stmt.args['from'].this.this.this  # From -> Table -> Identifier -> str
                if table_name in self.regular_table_partitioners:
                    for node_id in self.regular_table_partitioners[table_name].rules.keys():
                        stmt_for_node[node_id].append(stmt)
                elif table_name in self.dependent_partitions:
                    dependent_table = self.dependent_partitions[table_name].dependentTable
                    for node_id in self.regular_table_partitioners[dependent_table].rules.keys():
                        stmt_for_node[node_id].append(stmt)
                else:
                    assert False, f'unknown table {repr(table_name)}'

            elif isinstance(stmt, exp.Insert):
                schema = stmt.this
                assert (isinstance(schema, exp.Schema) and isinstance(schema.this, exp.Table)
                        and isinstance(schema.this.this, exp.Identifier))

                table_name = schema.this.this.this  # Schema -> Table -> Identifier -> str
                assert table_name in self.regular_table_partitioners  # not supporting dependent insert now
                schema_map = [identifier.this for identifier in schema.expressions]

                assert (isinstance(stmt.expression, exp.Values))
                tuples_for_node = dict()
                for node_id in self.node_id_list:
                    tuples_for_node.setdefault(node_id, [])
                for tuple_ in stmt.expression.expressions:  # Insert -> Values -> Tuples
                    assert isinstance(tuple_, exp.Tuple)
                    tuple_vals = tuple_.expressions
                    item = {field_name: eval_literal(val) for field_name, val in zip(schema_map, tuple_vals)}
                    p = self.regular_table_partitioners[table_name].get_belonging_nodes(item)
                    for node_id in p:
                        tuples_for_node[node_id].append(tuple_)
                for node_id in self.node_id_list:
                    if tuples_for_node[node_id]:
                        new_stmt = stmt.copy()
                        new_stmt.set("expression", exp.Values(expressions=tuples_for_node[node_id]))
                        stmt_for_node[node_id].append(new_stmt)

            elif isinstance(stmt, exp.Update):
                # TODO:
                raise NotImplemented

            else:
                raise NotImplemented

        return stmt_for_node


import unittest


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
