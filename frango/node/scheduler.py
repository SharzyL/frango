from typing import Dict, Optional, Iterable, TypeAlias, cast, List, Set, Sequence, Callable, Tuple
import collections

import sqlglot.expressions as exp

from frango.config import Config
from frango.sql_adaptor import SQLDef, PARAMS_ARG_KEY, sql_parse, sql_parse_one, sql_eval, sql_eval_literal, SQLVal, \
    sql_new_literal, sql_to_str

ResultClsType: TypeAlias = Optional[type | str]


class ExecutionPlan:
    def __init__(self, result_cls: ResultClsType = None):
        # result_cls is a hint of how the execution output is parsed
        # When it is a string, it represents the table name that we do not know how to parse
        self.result_cls = result_cls

    def _print(self, indent: int) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self._print(indent=0)


class EmptyExecutionPlan(ExecutionPlan):
    def _print(self, indent: int) -> str:
        return ""


class LocalExecutionPlan(ExecutionPlan):
    def __init__(self, query: exp.Expression, auto_commit: bool = False, result_cls: ResultClsType = None):
        super().__init__(result_cls)
        self.query = query
        self.auto_commit = auto_commit

    def _print(self, indent: int) -> str:
        return ' ' * indent + '\033[90m' + sql_to_str(self.query) + '\033[0m'


class DistributedExecutionPlan(ExecutionPlan):
    def __init__(self, queries_for_node: Dict[int, exp.Expression], auto_commit: bool = False,
                 result_cls: ResultClsType = None):
        super().__init__(result_cls)
        self.queries_for_node = queries_for_node
        self.auto_commit = auto_commit

    def _print(self, indent: int) -> str:
        builder: List[str] = ['DistributedExecutionPlan']
        for node, query in self.queries_for_node.items():
            builder.append(' ' * (indent + 2) + f'Node [{node}]: \033[90m{query}\033[0m')
        return '\n'.join(builder)


class SerialExecutionPlan(ExecutionPlan):
    def __init__(self, steps: Optional[Sequence[ExecutionPlan]] = None,
                 auto_commit: bool = True, result_cls: ResultClsType = None):
        super().__init__(result_cls)
        self.steps: List[ExecutionPlan] = list(steps) if steps is not None else []
        self.auto_commit: bool = auto_commit

    def extend(self, plan: ExecutionPlan | Sequence[ExecutionPlan]) -> None:
        if isinstance(plan, collections.abc.Sequence):
            self.steps.extend(plan)
        elif isinstance(plan, SerialExecutionPlan):
            self.steps.extend(plan.steps)
        else:
            self.steps.append(plan)

    def _print(self, indent: int) -> str:
        builder: List[str] = [f'SerialExecutionPlan (autocommit={self.auto_commit})']
        for i, step in enumerate(self.steps):
            builder.append(' ' * (indent + 2) + f'Step {i}: {step._print(indent + 2)}')
        return '\n'.join(builder)


def identifiers_in_exp(e: exp.Expression) -> List[str]:
    ident_list: List[str] = []
    for node in e.dfs(prune=lambda _: False):
        if isinstance(node, exp.Column):
            assert isinstance(node.this, exp.Identifier)
            if not node.this.quoted:
                ident_list.append(node.this.this)
    return ident_list


class RegularTableSplitter:
    # `rules` maps the node id to its filter string, e.g. `NAME == 'bob' AND AGE > 4'
    def __init__(self, partition: Config.Partition, table_name: str):
        assert partition.type == "regular"
        self.table_name = table_name
        self.rules: Dict[int, exp.Expression] = {
            node_id: sql_parse_one(cond) for node_id, cond in partition.filter.items()
        }
        self.ident_list: set[str] = set()
        for cond in self.rules.values():
            self.ident_list.update(identifiers_in_exp(cond))

    def partition_select_query(self, query: exp.Select) -> Dict[int, exp.Select]:
        return {node_id: self._restrict_select_with_rule(query, rule) for node_id, rule in self.rules.items()}

    def get_belonging_nodes(self, item: Dict[str, SQLVal] | SQLDef) -> list[int]:
        nodes = []
        for node_id, rule in self.rules.items():
            if sql_eval(rule, item):
                nodes.append(node_id)
        if not nodes:
            raise RuntimeError(f'No matching rule of table `{self.table_name}` found for {item}')
        return nodes

    # returns a map from node id to its subquery string
    @staticmethod
    def _restrict_select_with_rule(select: exp.Select, rule: exp.Expression) -> exp.Select:
        assert isinstance(select, exp.Select)
        return select.where(rule)


class Scheduler:
    def __init__(self, partitions: Dict[str, Config.Partition], node_id_list: List[int],
                 known_classes: Optional[Dict[str, type]] = None):
        self.regular_table_partitioners: dict[str, RegularTableSplitter] = {}
        self.dependent_partitions: Dict[str, Config.Partition] = {}
        self.replicate_partitions: Set[str] = set()
        for table_name, partition in partitions.items():
            if partition.type == "regular":
                self.regular_table_partitioners[table_name] = RegularTableSplitter(partition, table_name)
            elif partition.type == "dependent":
                self.dependent_partitions[table_name] = partition
            elif partition.type == "replicate":
                self.replicate_partitions.add(table_name)
            else:
                assert False, f'unsupported partition type {partition.type}'
        self.node_id_list: List[int] = node_id_list
        self.known_classes: Dict[str, type] = known_classes or dict()

    def _find_data_to_load_from_dependent(self, table_name: str, data: Sequence[SQLDef], dependent_table_name: str,
                                          dependent_data: Sequence[SQLDef], node_id: int) -> Sequence[SQLDef]:
        assert table_name in self.dependent_partitions
        assert dependent_table_name in self.regular_table_partitioners  # not supporting recursive dependency now

        partition = self.dependent_partitions[table_name]
        data_to_load: list[SQLDef] = []

        dependent_key = partition.dependentKey
        dependent_index = {getattr(item, dependent_key): item for item in dependent_data}
        for item in data:
            dependent_key_val = getattr(item, dependent_key)
            dependent_item = dependent_index[dependent_key_val]
            nodes = self.regular_table_partitioners[dependent_table_name].get_belonging_nodes(dependent_item)
            if node_id in nodes:
                data_to_load.append(item)
        return data_to_load

    def _bulk_load_regular(self, table_name: str, table_items: Sequence[SQLDef], node_id: int) \
            -> Sequence[ExecutionPlan]:
        if len(table_items) == 0:
            return ()
        partitioner = self.regular_table_partitioners[table_name]
        table_cls = table_items[0].__class__
        data_to_load: List[SQLDef] = []
        for item in table_items:
            if node_id in partitioner.get_belonging_nodes(item):
                data_to_load.append(item)
        insert_stmt = table_cls.sql_insert_with_placeholder()
        insert_stmt.set(PARAMS_ARG_KEY, [d.to_dict() for d in data_to_load])
        return (LocalExecutionPlan(insert_stmt),)

    def _bulk_load_dependent(self, table_name: str, table_items: Sequence[SQLDef], node_id: int,
                             input_tables: Dict[str, Sequence[SQLDef]]) -> Sequence[ExecutionPlan]:

        if len(table_items) == 0:
            return ()
        dependent_table_name = self.dependent_partitions[table_name].dependentTable
        dependent_table = input_tables[dependent_table_name]
        table_cls = table_items[0].__class__
        data_to_load: Sequence[SQLDef] = self._find_data_to_load_from_dependent(
            table_name, table_items, dependent_table_name, dependent_table, node_id
        )
        insert_stmt = table_cls.sql_insert_with_placeholder()
        insert_stmt.set(PARAMS_ARG_KEY, [d.to_dict() for d in data_to_load])
        return (LocalExecutionPlan(insert_stmt),)

    @staticmethod
    def _bulk_load_replicate(table_items: Sequence[SQLDef]) -> Sequence[ExecutionPlan]:
        if len(table_items) == 0:
            return ()
        table_cls = table_items[0].__class__
        insert_stmt = table_cls.sql_insert_with_placeholder()
        insert_stmt.set(PARAMS_ARG_KEY, [d.to_dict() for d in table_items])
        return (LocalExecutionPlan(insert_stmt),)

    @staticmethod
    def _bulk_load_hook(table_items: Sequence[SQLDef]) -> Sequence[ExecutionPlan]:
        if len(table_items) == 0:
            return ()
        table_cls = table_items[0].__class__
        if hasattr(table_cls, 'sql_hook_bulk_load'):
            hook = cast(Callable[[Sequence[SQLDef]], exp.Expression], getattr(table_cls, 'sql_hook_bulk_load'))
            hook_exp = hook(table_items)

            # Note: here we assume that the hook is executed locally, which fits the case of BeRead
            # in the future we may support re-scheduling this hook
            return (LocalExecutionPlan(hook_exp),)
        else:
            return ()  # TODO

    def schedule_bulk_load_for_node(self, input_tables: Dict[str, Sequence[SQLDef]], node_id: int) -> ExecutionPlan:
        steps = SerialExecutionPlan()

        for table_name, table_items in input_tables.items():
            if table_name in self.regular_table_partitioners:
                steps.extend(self._bulk_load_regular(table_name, table_items, node_id))
            elif table_name in self.dependent_partitions:
                steps.extend(self._bulk_load_dependent(table_name, table_items, node_id, input_tables))
            elif table_name in self.replicate_partitions:
                steps.extend(self._bulk_load_replicate(table_items))
            else:
                assert False, f'unsupported table {table_name}'

            steps.extend(self._bulk_load_hook(table_items))

        return steps

    def _schedule_simply_distributable(self, stmt: exp.Expression) -> ExecutionPlan:
        return DistributedExecutionPlan({
            node_id: stmt for node_id in self.node_id_list
        })

    def _schedule_distributed(self, stmt: exp.Expression) -> ExecutionPlan:
        # TODO: handle update hook
        # sanity check, since we only supported simple statement now
        from_ = stmt.this if isinstance(stmt, exp.Update) else stmt.args['from'].this
        assert isinstance(from_, exp.Table) and isinstance(from_.this, exp.Identifier), \
            f'{stmt.type} from {from_.type} not supported yet'
        table_name = from_.this.this  # From -> Table -> Identifier -> str

        # for UPDATE, we need to check whether the update changes partition key
        if isinstance(stmt, exp.Update):
            partition_keys: set[str] = set()
            if table_name in self.regular_table_partitioners:
                partition_keys = self.regular_table_partitioners[table_name].ident_list
            elif table_name in self.dependent_partitions:
                partition_keys = set(self.dependent_partitions[table_name].dependentKey)
            elif table_name in self.replicate_partitions:
                partition_keys = set()  # no partition, no worry
            else:
                assert False, f'not knowing the partition key of {table_name}'

            for update_eq in stmt.expressions:
                assert isinstance(update_eq, exp.EQ), f'Update with {update_eq.type} not supported yet'
                assert isinstance(update_eq.this, exp.Column)
                assert isinstance(update_eq.this.this, exp.Identifier)
                column_name = update_eq.this.this.this
                assert column_name not in partition_keys, \
                    f'do not support update partition key `{column_name}` for table `{table_name}`'

        # only Select stmt has returns
        result_cls = table_name if isinstance(stmt, exp.Select) else None
        if table_name in self.regular_table_partitioners:
            belonging_nodes: Iterable[int] = self.regular_table_partitioners[table_name].rules.keys()
            return DistributedExecutionPlan({
                node_id: stmt for node_id in belonging_nodes
            }, result_cls=result_cls)
        elif table_name in self.dependent_partitions:
            dependent_table = self.dependent_partitions[table_name].dependentTable

            dep_belonging_nodes: Iterable[int] = self.regular_table_partitioners[dependent_table].rules.keys()
            return DistributedExecutionPlan({
                node_id: stmt for node_id in dep_belonging_nodes
            }, result_cls=result_cls)
        elif table_name in self.replicate_partitions:
            if isinstance(stmt, exp.Select):  # for select, we can do on any node
                return LocalExecutionPlan(stmt, result_cls=result_cls)
            else:  # for update and delete, we need to do on every node
                return DistributedExecutionPlan({
                    node_id: stmt for node_id in self.node_id_list
                }, result_cls=result_cls)
        else:
            assert False, f'unknown table {repr(table_name)}'

    def _schedule_insert(self, stmt: exp.Expression) -> ExecutionPlan:
        assert isinstance(stmt, exp.Insert)

        table_name, schema_map = self._parse_insert_schema(stmt)

        if table_name in self.regular_table_partitioners:
            partitioner = self.regular_table_partitioners[table_name]

            # collect tuples for each node
            tuples_for_node: Dict[int, list[exp.Tuple]] = dict()
            for node_id in self.node_id_list:
                tuples_for_node.setdefault(node_id, [])
            for tuple_ in self._parse_insert_tuples(stmt):
                tuple_vals = tuple_.expressions
                item = {field_name: sql_eval_literal(val) for field_name, val in zip(schema_map, tuple_vals)}

                p = partitioner.get_belonging_nodes(item)
                for node_id in p:
                    tuples_for_node[node_id].append(tuple_)

            # generate query for each node
            queries_for_node: Dict[int, exp.Expression] = dict()
            for node_id in self.node_id_list:
                if tuples_for_node[node_id]:
                    new_stmt = stmt.copy()  # type: ignore[no-untyped-call]
                    new_stmt.set("expression", exp.Values(expressions=tuples_for_node[node_id]))
                    queries_for_node[node_id] = new_stmt
            return DistributedExecutionPlan(queries_for_node)

        elif table_name in self.dependent_partitions:
            partition = self.dependent_partitions[table_name]
            dependent_table_name, dependent_key = partition.dependentTable, partition.dependentKey
            partition_rule: dict[int, exp.Expression] = self.regular_table_partitioners[dependent_table_name].rules

            tuples: List[exp.Tuple] = self._parse_insert_tuples(stmt)
            items: List[Dict[str, SQLVal]] = [
                {field_name: sql_eval_literal(val) for field_name, val in zip(schema_map, t.expressions)}
                for t in tuples
            ]
            dependent_table = exp.Table(this=exp.Identifier(this=dependent_table_name))

            steps = SerialExecutionPlan()
            for tuple_, item in zip(tuples, items):
                insert_queries_for_node: Dict[int, exp.Expression] = dict()
                for node in partition_rule.keys():
                    rule = partition_rule[node]

                    def transform_rule(node_: exp.Expression) -> exp.Expression:
                        if isinstance(node_, exp.Column) and not node_.table:
                            return exp.Column(this=node_.this, table=dependent_table.this)
                        return node_

                    rule_transformed = rule.transform(transform_rule)

                    select_expressions = list(tuple_)
                    select_from = exp.From(this=dependent_table)
                    select_expression_key_eq = exp.EQ(
                        this=exp.Column(this=exp.Identifier(this=dependent_key)),
                        expression=sql_new_literal(item[dependent_key])
                    )
                    insert_select = exp.Select(expressions=select_expressions).from_(select_from).where(
                        exp.And(this=select_expression_key_eq, expression=rule_transformed)
                    )
                    insert_queries_for_node[node] = exp.Insert(this=stmt.this, expression=insert_select)
                steps.extend(DistributedExecutionPlan(queries_for_node=insert_queries_for_node))

            return steps

        elif table_name in self.replicate_partitions:
            return DistributedExecutionPlan({
                node_id: stmt for node_id in self.node_id_list
            })

        else:
            assert False, f'unknown table {repr(table_name)}'

    @staticmethod
    def _parse_insert_schema(stmt: exp.Insert) -> Tuple[str, List[str]]:  # (table_name, schema_map)
        schema = stmt.this

        # only simple insert with schema are supported now
        assert (isinstance(schema, exp.Schema) and isinstance(schema.this, exp.Table)
                and isinstance(schema.this.this, exp.Identifier))

        table_name = schema.this.this.this  # Schema -> Table -> Identifier -> str
        return table_name, [identifier.this for identifier in schema.expressions]

    @staticmethod
    def _parse_insert_tuples(stmt: exp.Insert) -> List[exp.Tuple]:
        tuples: List[exp.Tuple] = list()

        for tuple_ in stmt.expression.expressions:  # Insert -> Values -> Tuples
            assert isinstance(tuple_, exp.Tuple)
            tuples.append(tuple_)

        return tuples

    def _schedule_insert_hook(self, stmt: exp.Insert) -> ExecutionPlan:
        table_name, schema_map = self._parse_insert_schema(stmt)
        cls = self.known_classes.get(table_name, None)
        if cls is None:
            return EmptyExecutionPlan()
        assert issubclass(cls, SQLDef)

        if not hasattr(cls, 'sql_hook_insert'):
            return EmptyExecutionPlan()
        hook = cast(Callable[[SQLDef], exp.Expression], getattr(cls, 'sql_hook_insert'))

        tuples = self._parse_insert_tuples(stmt)

        def parse_tuple(tuple_: exp.Tuple) -> SQLDef:
            tuple_vals = tuple_.expressions
            item_dict = {field_name: sql_eval_literal(val) for field_name, val in zip(schema_map, tuple_vals)}
            return cast(SQLDef, cls.from_dict(item_dict))

        steps = SerialExecutionPlan()
        for t in tuples:
            item = parse_tuple(t)
            hook_query = hook(item)
            hook_plan = self.schedule_query(hook_query)
            steps.extend(hook_plan)

        return steps

    # schedule a query to partitioned database
    def schedule_query(self, query: str | exp.Expression | List[exp.Expression]) -> ExecutionPlan:
        stmt_list: Sequence[exp.Expression] = sql_parse(query) if isinstance(query, str) \
            else [query] if isinstance(query, exp.Expression) \
            else query

        plan = SerialExecutionPlan(steps=[], auto_commit=True)

        for stmt in stmt_list:
            # CREATE is for every node
            if isinstance(stmt, exp.Create):
                plan.extend(self._schedule_simply_distributable(stmt))

            # SELECT, UPDATE and DELETE are thrown to all belonging nodes
            elif isinstance(stmt, exp.Select) or isinstance(stmt, exp.Delete) or isinstance(stmt, exp.Update):
                plan.extend(self._schedule_distributed(stmt))

            # distribute insert objects to all belonging nodes
            elif isinstance(stmt, exp.Insert):
                plan.extend(self._schedule_insert(stmt))
                plan.extend(self._schedule_insert_hook(stmt))

            else:
                raise NotImplementedError(f'{type(stmt)} is not supported')

        return plan
