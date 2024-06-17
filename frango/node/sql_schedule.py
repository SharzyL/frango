from __future__ import annotations

from typing import Dict

import sqlglot
import sqlglot.expressions as exp

from frango.config import Partition


def _getattr(v, k):
    if isinstance(v, dict):
        return v[k]
    else:
        return getattr(v, k)


def sql_parse_one(stmt) -> exp.Expression:
    return sqlglot.parse_one(stmt, dialect='sqlite')


def sql_parse(stmts) -> list[exp.Expression | None]:
    return sqlglot.parse(stmts, dialect='sqlite')


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


class SQLSplitterSingleTable:
    # `rules` maps the node id to its filter string, e.g. `NAME == 'bob' AND AGE > 4'
    def __init__(self, partition: Partition):
        assert partition.type == "regular"
        self.rules: Dict[int, exp.Expression] = {
            node_id: sql_parse_one(cond) for node_id, cond in partition.filter.items()
        }

    def partition_select_query(self, query) -> Dict[int, str]:
        return {node_id: self._split_one(query, rule) for node_id, rule in self.rules.items()}

    def get_belonging_nodes(self, item) -> list[int]:
        nodes = []
        for node_id, rule in self.rules.items():
            if self._eval(rule, item):
                nodes.append(node_id)
        if not nodes:
            raise RuntimeError(f'No matching rule found for {item}')
        return nodes

    # returns a map from node id to its subquery string
    @staticmethod
    def _split_one(query: str, rule: exp.Expression) -> str:
        sql = sql_parse_one(query)
        assert isinstance(sql, exp.Select)
        return sql.where(rule).sql()

    @staticmethod
    def _eval(rule, item):
        _eval = SQLSplitterSingleTable._eval  # for easier self reference
        if isinstance(rule, exp.Column):
            assert isinstance(rule.this, exp.Identifier)
            if rule.this.quoted:
                return str(rule.this.this)
            else:
                return _getattr(item, rule.this.this)
        elif isinstance(rule, exp.Literal):
            if rule.is_string:
                return rule.this
            elif '.' in rule.this:
                return float(rule.this)
            else:
                return int(rule.this)
        elif isinstance(rule, exp.Boolean):
            return rule.this

        # unary op
        elif isinstance(rule, exp.Neg):
            return -_eval(rule.this, item)

        # binary op
        elif isinstance(rule, exp.EQ):
            return _eval(rule.this, item) == _eval(rule.expression, item)
        elif isinstance(rule, exp.NEQ):
            return _eval(rule.this, item) != _eval(rule.expression, item)

        elif isinstance(rule, exp.GT):
            return _eval(rule.this, item) > _eval(rule.expression, item)
        elif isinstance(rule, exp.LT):
            return _eval(rule.this, item) < _eval(rule.expression, item)
        elif isinstance(rule, exp.GTE):
            return _eval(rule.this, item) >= _eval(rule.expression, item)
        elif isinstance(rule, exp.LTE):
            return _eval(rule.this, item) <= _eval(rule.expression, item)
        elif isinstance(rule, exp.And):
            return _eval(rule.this, item) and _eval(rule.expression, item)
        elif isinstance(rule, exp.Or):
            return _eval(rule.this, item) or _eval(rule.expression, item)
        elif isinstance(rule, exp.Xor):
            return _eval(rule.this, item) ^ _eval(rule.expression, item)
        else:
            return NotImplemented(f'`{rule}` is not supported')


class Scheduler:
    def __init__(self, partitions: dict[str, Partition], node_id_list: list[int]):
        self.regular_table_splitters = {
            table: SQLSplitterSingleTable(partition)
            for table, partition in partitions.items()
            if partition.type == "regular"
        }
        self.dependent_partitions = {k: v for k, v in partitions.items() if v.type != "regular"}
        self.node_id_list = node_id_list

    def schedule_dependent_bulk_load(self, table_name: str, data: list, dependent_table_name: str,
                                     dependent_data: list):
        assert table_name in self.dependent_partitions
        assert dependent_table_name in self.regular_table_splitters  # not supporting recursive dependency now

        items_for_node = dict()
        for node_id in self.node_id_list:
            items_for_node[node_id] = []

        partition = self.dependent_partitions[table_name]
        dependent_key = partition.dependentKey
        dependent_index = {_getattr(item, dependent_key): item for item in dependent_data}
        for item in data:
            dependent_key_val = _getattr(item, dependent_key)
            dependent_item = dependent_index[dependent_key_val]
            nodes = self.regular_table_splitters[table_name].get_belonging_nodes(item)
            for node_id in nodes:
                items_for_node[node_id].append(item)

        return items_for_node

    def schedule_bulk_load(self, table_name: str, data: list):
        pass

    # schedule a query to partitioned database
    def schedule(self, query):
        stmt_list = sql_parse(query)
        stmt_for_node = dict()
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
                if table_name in self.regular_table_splitters:
                    for node_id in self.regular_table_splitters[table_name].rules.keys():
                        stmt_for_node[node_id].append(stmt)
                elif table_name in self.dependent_partitions:
                    dependent_table = self.dependent_partitions[table_name].dependentTable
                    for node_id in self.regular_table_splitters[dependent_table].rules.keys():
                        stmt_for_node[node_id].append(stmt)
                else:
                    assert False, f'unknown table {repr(table_name)}'

            elif isinstance(stmt, exp.Insert):
                schema = stmt.this
                assert (isinstance(schema, exp.Schema) and isinstance(schema.this, exp.Table)
                        and isinstance(schema.this.this, exp.Identifier))

                table_name = schema.this.this.this  # Schema -> Table -> Identifier -> str
                assert table_name in self.regular_table_splitters  # not supporting dependent insert now
                schema_map = [identifier.this for identifier in schema.expressions]

                assert (isinstance(stmt.expression, exp.Values))
                tuples_for_node = dict()
                for node_id in self.node_id_list:
                    tuples_for_node.setdefault(node_id, [])
                for tuple_ in stmt.expression.expressions:  # Insert -> Values -> Tuples
                    assert isinstance(tuple_, exp.Tuple)
                    tuple_vals = tuple_.expressions
                    item = {field_name: eval_literal(val) for field_name, val in zip(schema_map, tuple_vals)}
                    p = self.regular_table_splitters[table_name].get_belonging_nodes(item)
                    for node_id in p:
                        tuples_for_node[node_id].append(tuple_)
                # print(tuples_for_node)
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

        return {node_id: [stmt.sql() for stmt in stmt_list] for node_id, stmt_list in stmt_for_node.items()}


import unittest


# noinspection SqlNoDataSourceInspection
class TestSplit(unittest.TestCase):
    def test_basic(self):
        splitter = SQLSplitterSingleTable(Partition(type="regular", filter={
            1: "id > 4",
            2: "id <= 4"
        }))
        query = "SELECT id, timestamp FROM Article WHERE id > 3 AND name == 'harry'"
        splits = splitter.partition_select_query(query)
        self.assertEqual(splits[1], "SELECT id, timestamp FROM Article WHERE (id > 3 AND name = 'harry') AND id > 4")
        self.assertEqual(splits[2], "SELECT id, timestamp FROM Article WHERE (id > 3 AND name = 'harry') AND id <= 4")

    def test_eval(self):
        _eval = SQLSplitterSingleTable._eval
        item = {"id": 1, "age": 4, "gender": "male"}
        self.assertEqual(_eval(sql_parse_one("id == 1"), item), True)
        self.assertEqual(_eval(sql_parse_one("id == 1 AND age < 5"), item), True)
        self.assertEqual(_eval(sql_parse_one("id > 3 AND age < 5"), item), False)
        self.assertEqual(_eval(sql_parse_one('gender == "male"'), item), True)
        self.assertEqual(_eval(sql_parse_one("gender != 'female'"), item), True)

    def test_find_partition(self):
        splitter = SQLSplitterSingleTable(Partition(type="regular", filter={
            1: "id > 4",
            2: "id <= 4 AND gender == 'male'",
            3: "id <= 4 AND gender != 'male'",
        }))
        self.assertEqual(splitter.get_belonging_nodes({"id": 5, "gender": "male"}), [1])
        self.assertEqual(splitter.get_belonging_nodes({"id": 0, "gender": "male"}), [2])
        self.assertEqual(splitter.get_belonging_nodes({"id": 0, "gender": "female"}), [3])


class TestSchedule(unittest.TestCase):
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
        plan = sch.schedule('''
            CREATE TABLE Person (id int PRIMARY KEY, age int NOT NULL, gender string NOT NULL);
            CREATE TABLE Article (aid int NOT NULL, category string NOT NULL);
            SELECT id, timestamp FROM Article WHERE category == "music";
            INSERT INTO Article (aid, category) VALUES (100, "music"), (200, "politics");
        ''')
        self.assertEqual(len(plan[1]), 4)
        self.assertEqual(len(plan[2]), 4)
        self.assertIn("politics", plan[1][3])
        self.assertIn("music", plan[2][3])
        self.assertEqual(len(plan[3]), 2)
        # for node_id, node_plan in plan.items():
        #     print(f'Node {node_id}:')
        #     for stmt in node_plan:
        #         print(f'\t{stmt}')


if __name__ == '__main__':
    unittest.main()
