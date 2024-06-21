from typing import List, get_origin, Any, Optional, cast, Dict, Sequence, Iterable
from inspect import get_annotations

from dataclass_wizard import JSONWizard
import sqlglot
import sqlglot.expressions as exp

"""
this is used to insert into sqlglot expression to specify the params.
the params can be either a dict or a list of dict. For the later case, sqlite executemany is used
the way to insert params is `ast_node.set(PARAMS_ARG_KEY, params)`
the way to get params is `ast_node.args.get(PARAMS_ARG_KEY)`
"""
PARAMS_ARG_KEY = "_params"


def anno_to_type(anno: Any) -> exp.DataType.Type:
    if anno == str or anno == 'str':
        return exp.DataType.Type.TEXT
    elif anno == int or anno == 'int':
        return exp.DataType.Type.INT
    elif anno == bool or anno == 'bool':
        return exp.DataType.Type.BOOLEAN
    elif get_origin(anno) == list or get_origin(anno) == List:
        return exp.DataType.Type.TEXT  # we will join list with ','
    else:
        raise NotImplementedError(f'{repr(anno)} ({type(anno)}) is not supported')


SQLVal = int | float | bool | str


class SQLDef(JSONWizard):  # type: ignore[misc]
    @classmethod
    def sql_insert_schema(cls) -> exp.Schema:
        cls_annotations = get_annotations(cls)
        name = cls.__name__
        schema = exp.Schema(this=exp.Table(this=name, quoted=False))
        for field, anno in cls_annotations.items():
            schema.append('expressions', exp.Identifier(this=field, quoted=False))
        return schema

    @classmethod
    def sql_insert_with_placeholder(cls) -> exp.Insert:
        cls_annotations = get_annotations(cls)
        insert_stmt = exp.Insert(this=cls.sql_insert_schema())
        placeholders = [exp.Placeholder(this=name) for name in cls_annotations.keys()]
        insert_stmt.set('expression', exp.Values(expressions=[exp.Tuple(expressions=placeholders)]))
        return insert_stmt

    def sql_insert_tuple(self) -> exp.Tuple:
        cls_annotations = get_annotations(self.__class__)
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
    def sql_create(cls) -> exp.Create:
        pkey = None
        if hasattr(cls, "__primary_key__"):
            pkey = cls.__primary_key__()
        cls_annotations = get_annotations(cls)
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

    @classmethod
    def sql_drop_if_exists(cls) -> exp.Drop:
        return exp.Drop(
            exists=True,
            this=exp.Table(
                this=exp.Identifier(this=cls.__name__, quooted=False)
            ),
            kind='table'
        )


def _ensure_numeral(v: SQLVal) -> int | float | bool:
    assert isinstance(v, int) or isinstance(v, float) or isinstance(v, bool)
    return v


def _ensure_bool(v: SQLVal) -> bool:
    assert isinstance(v, bool)
    return v


def _ensure_str(v: SQLVal) -> str:
    assert isinstance(v, str)
    return v


def sql_eval(expr: exp.Expression, item: Optional[SQLDef | Dict[str, SQLVal]]) -> SQLVal:
    def _getattr(v: Any, k: str) -> Any:
        if isinstance(v, dict):
            return v[k]
        else:
            return getattr(v, k)

    if isinstance(expr, exp.Column):
        assert isinstance(expr.this, exp.Identifier)
        assert item is not None
        return cast(SQLVal, _getattr(item, expr.this.this))
    elif isinstance(expr, exp.Literal):
        if expr.is_string:
            return _ensure_str(expr.this)
        elif '.' in expr.this:
            return float(expr.this)
        else:
            return int(expr.this)
    elif isinstance(expr, exp.Boolean):
        return cast(bool, expr.this)

    # unary op
    elif isinstance(expr, exp.Neg):
        val = sql_eval(expr.this, item)
        assert isinstance(val, int) or isinstance(val, float)
        return -val

    # binary op
    elif isinstance(expr, exp.EQ):
        return sql_eval(expr.this, item) == sql_eval(expr.expression, item)
    elif isinstance(expr, exp.NEQ):
        return sql_eval(expr.this, item) != sql_eval(expr.expression, item)

    elif isinstance(expr, exp.GT):
        return _ensure_numeral(sql_eval(expr.this, item)) > _ensure_numeral(sql_eval(expr.expression, item))
    elif isinstance(expr, exp.LT):
        return _ensure_numeral(sql_eval(expr.this, item)) < _ensure_numeral(sql_eval(expr.expression, item))
    elif isinstance(expr, exp.GTE):
        return _ensure_numeral(sql_eval(expr.this, item)) >= _ensure_numeral(sql_eval(expr.expression, item))
    elif isinstance(expr, exp.LTE):
        return _ensure_numeral(sql_eval(expr.this, item)) <= _ensure_numeral(sql_eval(expr.expression, item))
    elif isinstance(expr, exp.And):
        return _ensure_bool(sql_eval(expr.this, item)) and _ensure_bool(sql_eval(expr.expression, item))
    elif isinstance(expr, exp.Or):
        return _ensure_bool(sql_eval(expr.this, item)) or _ensure_bool(sql_eval(expr.expression, item))
    elif isinstance(expr, exp.Xor):
        return _ensure_bool(sql_eval(expr.this, item)) ^ _ensure_bool(sql_eval(expr.expression, item))
    else:
        raise NotImplementedError(f'`{expr}` is not supported')


def sql_parse_one(stmt: str) -> exp.Expression:
    return sqlglot.parse_one(stmt, dialect='sqlite')


def sql_parse(stmts: str) -> Sequence[exp.Expression]:
    parsed = sqlglot.parse(stmts, dialect='sqlite')
    assert all(stmt is not None for stmt in parsed)
    return cast(Sequence[exp.Expression], parsed)


def sql_to_str(sql: exp.Expression | Iterable[exp.Expression]) -> str:
    if isinstance(sql, exp.Expression):
        return sql.sql(dialect='sqlite')
    elif hasattr(sql, '__iter__'):
        return ';'.join(stmt.sql(dialect="sqlite") for stmt in sql)
    else:
        assert False


def sql_eval_literal(expr: exp.Expression) -> SQLVal:
    if isinstance(expr, exp.Literal):
        if expr.is_string:
            return _ensure_str(expr.this)
        elif '.' in expr.this:
            return float(expr.this)
        else:
            return int(expr.this)
    elif isinstance(expr, exp.Boolean):
        return _ensure_bool(expr.this)
    else:
        assert False, f'unsupported expression {repr(expr)} for sql_eval_literal'


def sql_new_literal(val: SQLVal) -> exp.Expression:
    if isinstance(val, str):
        return exp.Literal(this=val, is_string=True)
    else:
        return exp.Literal(this=val, is_string=False)
