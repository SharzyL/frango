from typing import List, get_origin, Any

from dataclass_wizard import JSONWizard
import sqlglot.expressions as exp


def anno_to_type(anno: Any) -> exp.DataType.Type:
    if anno == str:
        return exp.DataType.Type.TEXT
    elif anno == int:
        return exp.DataType.Type.INT
    elif anno == bool:
        return exp.DataType.Type.BOOLEAN
    elif get_origin(anno) == list or get_origin(anno) == List:
        return exp.DataType.Type.TEXT  # we will join list with ','
    else:
        raise NotImplementedError(f'{anno} is not supported')


class SQLDef(JSONWizard):  # type: ignore[misc]
    @classmethod
    def sql_insert_schema(cls) -> exp.Schema:
        cls_annotations = cls.__annotations__
        name = cls.__name__
        schema = exp.Schema(this=exp.Table(this=name, quoted=False))
        for field, anno in cls_annotations.items():
            schema.append('expressions', exp.Identifier(this=field, quoted=False))
        return schema

    @classmethod
    def sql_insert_with_placeholder(cls) -> exp.Insert:
        cls_annotations = cls.__annotations__
        insert_stmt = exp.Insert(this=cls.sql_insert_schema())
        placeholders = [exp.Placeholder(this=name) for name in cls_annotations.keys()]
        insert_stmt.set('expression', exp.Values(expressions=[exp.Tuple(expressions=placeholders)]))
        return insert_stmt

    def sql_insert_tuple(self) -> exp.Tuple:
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
    def sql_create(cls) -> exp.Create:
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

    @classmethod
    def sql_drop_if_exists(cls) -> exp.Drop:
        return exp.Drop(
            exists=True,
            this=exp.Table(
                this=exp.Identifier(this=cls.__name__, quooted=False)
            ),
            kind='table'
        )
