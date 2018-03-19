import itertools

from pyspark.sql import DataFrame

import hail
import hail as hl
from hail.expr.expr_ast import *
from hail.expr.expressions import *
from hail.expr.types import *
from hail.typecheck import *
from hail.utils import wrap_to_list, storage_level, LinkedList, Struct
from hail.utils.java import *
from hail.utils.misc import get_nice_field_error, get_nice_attr_error, check_collisions, check_field_uniqueness

table_type = lazy()


class Ascending(object):
    def __init__(self, col):
        self.col = col

    def _j_obj(self):
        return scala_package_object(Env.hail().table).asc(self.col)


class Descending(object):
    def __init__(self, col):
        self.col = col

    def _j_obj(self):
        return scala_package_object(Env.hail().table).desc(self.col)


@typecheck(col=oneof(Expression, str))
def asc(col):
    """Sort by `col` ascending."""

    return Ascending(col)


@typecheck(col=oneof(Expression, str))
def desc(col):
    """Sort by `col` descending."""

    return Descending(col)

class ExprContainer(object):
    def __init__(self):
        self._fields: Dict[str, Expression] = {}
        self._fields_inverse: Dict[Expression, str] = {}
        super(ExprContainer, self).__init__()

    def _set_field(self, key, value):
        self._fields[key] = value
        self._fields_inverse[value] = key
        if key in dir(self):
            warn(f"Name collision: field {repr(key)} already in object dict. "
                 "This field must be referenced with indexing syntax")
        else:
            self.__dict__[key] = value

    def _get_field(self, item) -> Expression:
        if item in self._fields:
            return self._fields[item]
        else:
            raise LookupError(get_nice_field_error(self, item))

    def __iter__(self):
        raise TypeError(f"'{self.__class__.__name__}' object is not iterable")

    def __delattr__(self, item):
        if not item[0] == '_':
            raise NotImplementedError(f"'{self.__class__.__name__}' object is not mutable")

    def __setattr__(self, key, value):
        if not key[0] == '_':
            raise NotImplementedError(f"'{self.__class__.__name__}' object is not mutable")
        self.__dict__[key] = value

    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        else:
            raise AttributeError(get_nice_attr_error(self, item))

    def _copy_fields_from(self, other: 'ExprContainer'):
        self._fields = other._fields
        self._fields_inverse = other._fields_inverse

class GroupedTable(ExprContainer):
    """Table that has been grouped.

    There are only two operations on a grouped table, :meth:`.GroupedTable.partition_hint`
    and :meth:`.GroupedTable.aggregate`.

    .. testsetup ::

        table1 = hl.import_table('data/kt_example1.tsv', impute=True, key='ID')

    """

    def __init__(self, parent: 'Table', groups):
        super(GroupedTable, self).__init__()
        self._groups = groups
        self._parent = parent
        self._npartitions = None

        self._copy_fields_from(parent)


    @typecheck_method(n=int)
    def partition_hint(self, n):
        """Set the target number of partitions for aggregation.

        Examples
        --------

        Use `partition_hint` in a :meth:`.Table.group_by` / :meth:`.GroupedTable.aggregate`
        pipeline:

        >>> table_result = (table1.group_by(table1.ID)
        ...                       .partition_hint(5)
        ...                       .aggregate(meanX = agg.mean(table1.X), sumZ = agg.sum(table1.Z)))

        Notes
        -----
        Until Hail's query optimizer is intelligent enough to sample records at all
        stages of a pipeline, it can be necessary in some places to provide some
        explicit hints.

        The default number of partitions for :meth:`.GroupedTable.aggregate` is the
        number of partitions in the upstream table. If the aggregation greatly
        reduces the size of the table, providing a hint for the target number of
        partitions can accelerate downstream operations.

        Parameters
        ----------
        n : int
            Number of partitions.

        Returns
        -------
        :class:`.GroupedTable`
            Same grouped table with a partition hint.
        """
        self._npartitions = n
        return self

    def aggregate(self, **named_exprs):
        """Aggregate by group, used after :meth:`.Table.group_by`.

        Examples
        --------
        Compute the mean value of `X` and the sum of `Z` per unique `ID`:

        >>> table_result = (table1.group_by(table1.ID)
        ...                       .aggregate(meanX = agg.mean(table1.X), sumZ = agg.sum(table1.Z)))

        Group by a height bin and compute sex ratio per bin:

        >>> table_result = (table1.group_by(height_bin = table1.HT // 20)
        ...                       .aggregate(fraction_female = agg.fraction(table1.SEX == 'F')))

        Parameters
        ----------
        named_exprs : varargs of :class:`.Expression`
            Aggregation expressions.

        Returns
        -------
        :class:`.Table`
            Aggregated table.
        """
        named_exprs = {k: to_expr(v) for k, v in named_exprs.items()}

        strs = []
        base, cleanup = self._parent._process_joins(
            *itertools.chain((v for _, v in self._groups), named_exprs.values()))
        for k, v in named_exprs.items():
            analyze('GroupedTable.aggregate', v, self._parent._global_indices, {self._parent._row_axis})
            strs.append('{} = {}'.format(escape_id(k), v._ast.to_hql()))

        group_strs = ',\n'.join('{} = {}'.format(escape_id(k), v._ast.to_hql()) for k, v in self._groups)
        return cleanup(
            Table(base._jt.aggregate(group_strs, ",\n".join(strs), joption(self._npartitions))))


class Table(ExprContainer):
    """Hail's distributed implementation of a dataframe or SQL table.
    
    Use :func:`.read_table` to read a table that was written with
    :meth:`.Table.write`.

    Examples
    --------

    The examples below use ``table1`` and ``table2``, which are imported
    from text files using :func:`.import_table`.

    >>> table1 = hl.import_table('data/kt_example1.tsv', impute=True, key='ID')
    >>> table1.show()

    .. code-block:: text

        +-------+-------+-----+-------+-------+-------+-------+-------+
        |    ID |    HT | SEX |     X |     Z |    C1 |    C2 |    C3 |
        +-------+-------+-----+-------+-------+-------+-------+-------+
        | int32 | int32 | str | int32 | int32 | int32 | int32 | int32 |
        +-------+-------+-----+-------+-------+-------+-------+-------+
        |     1 |    65 | M   |     5 |     4 |     2 |    50 |     5 |
        |     2 |    72 | M   |     6 |     3 |     2 |    61 |     1 |
        |     3 |    70 | F   |     7 |     3 |    10 |    81 |    -5 |
        |     4 |    60 | F   |     8 |     2 |    11 |    90 |   -10 |
        +-------+-------+-----+-------+-------+-------+-------+-------+

    >>> table2 = hl.import_table('data/kt_example2.tsv', impute=True, key='ID')
    >>> table2.show()

    .. code-block:: text

        +-------+-------+--------+
        |    ID |     A | B      |
        +-------+-------+--------+
        | int32 | int32 | str    |
        +-------+-------+--------+
        |     1 |    65 | cat    |
        |     2 |    72 | dog    |
        |     3 |    70 | mouse  |
        |     4 |    60 | rabbit |
        +-------+-------+--------+

    Define new annotations:

    >>> height_mean_m = 68
    >>> height_sd_m = 3
    >>> height_mean_f = 65
    >>> height_sd_f = 2.5
    >>>
    >>> def get_z(height, sex):
    ...    return hl.cond(sex == 'M',
    ...                  (height - height_mean_m) / height_sd_m,
    ...                  (height - height_mean_f) / height_sd_f)
    >>>
    >>> table1 = table1.annotate(height_z = get_z(table1.HT, table1.SEX))
    >>> table1 = table1.annotate_globals(global_field_1 = [1, 2, 3])

    Filter rows of the table:

    >>> table2 = table2.filter(table2.B != 'rabbit')

    Compute global aggregation statistics:

    >>> t1_stats = table1.aggregate(hl.struct(mean_c1 = agg.mean(table1.C1),
    ...                                       mean_c2 = agg.mean(table1.C2),
    ...                                       stats_c3 = agg.stats(table1.C3)))
    >>> print(t1_stats)

    Group by a field and aggregate to produce a new table:

    >>> table3 = (table1.group_by(table1.SEX)
    ...                 .aggregate(mean_height_data = agg.mean(table1.HT)))
    >>> table3.show()

    Join tables together inside an annotation expression:

    >>> table2 = table2.key_by('ID')
    >>> table1 = table1.annotate(B = table2[table1.ID].B)
    >>> table1.show()
    """

    def __init__(self, jt):
        super(Table, self).__init__()

        self._jt = jt

        self._row_axis = 'row'

        self._global_indices = Indices(axes=set(), source=self)
        self._row_indices = Indices(axes={self._row_axis}, source=self)

        self._global_type = HailType._from_java(jt.globalSignature())
        self._row_type = HailType._from_java(jt.signature())

        assert isinstance(self._global_type, tstruct)
        assert isinstance(self._row_type, tstruct)

        self._globals = construct_expr(TopLevelReference('global', self._global_indices), self._global_type,
                                       indices=self._global_indices)
        self._row = construct_expr(TopLevelReference('row', self._row_indices), self._row_type,
                                   indices=self._row_indices)

        self._key = hail.struct(
            **{k: self._row[k] for k in jiterable_to_list(jt.key())})

        for k, v in itertools.chain(self._globals.items(),
                                    self._row.items()):
            self._set_field(k, v)


    def __getitem__(self, item):
        if isinstance(item, str):
            return self._get_field(item)
        elif isinstance(item, slice):
            s = item
            if not (s.start is None and s.stop is None and s.step is None):
                raise ExpressionException(
                    "Expect unbounded slice syntax ':' to indicate global table join, found unexpected attributes {}".format(
                        ', '.join(x for x in ['start' if s.start is not None else None,
                                              'stop' if s.stop is not None else None,
                                              'step' if s.step is not None else None] if x is not None)
                    )
                )

            return self.index_globals()
        else:
            exprs = item if isinstance(item, tuple) else (item,)
            return self.index(*exprs)

    @property
    def key(self) -> StructExpression:
        """Row key struct.

        Examples
        --------

        List of key field names:

        >>> list(table1.key)
        ['ID']

        Number of key fields:

        >>> len(table1.key)
        1


        Returns
        -------
        :class:`.StructExpression`
        """
        return self._key

    def n_partitions(self):
        """Returns the number of partitions in the table.

        Returns
        -------
        :obj:`int`
        """
        return self._jt.nPartitions()

    def count(self):
        """Count the number of rows in the table.

        Examples
        --------

        >>> table1.count()
        4

        Returns
        -------
        :obj:`int`
        """
        return self._jt.count()

    def _force_count(self):
        return self._jt.forceCount()

    @classmethod
    @typecheck_method(rows=anytype,
                      schema=tstruct,
                      key=oneof(str, listof(str)),
                      n_partitions=nullable(int))
    def parallelize(cls, rows, schema, key=[], n_partitions=None):
        rows = to_expr(rows, hl.tarray(schema))
        if not isinstance(rows.dtype.element_type, tstruct):
            raise TypeError("'parallelize' expects an array with element type 'struct', found '{}'"
                            .format(rows.dtype))
        rows = rows.value
        return Table(
            Env.hail().table.Table.parallelize(
                Env.hc()._jhc, [schema._convert_to_j(r) for r in rows],
                schema._jtype, wrap_to_list(key), joption(n_partitions)))

    @typecheck_method(keys=oneof(str, Expression))
    def key_by(self, *keys):
        """Change the key.

        Examples
        --------
        Assume `table1` is a :class:`.Table` with three fields: `C1`, `C2`
        and `C3`.

        Change key fields:

        >>> table_result = table1.key_by('C2', 'C3')

        >>> table_result = table1.key_by(table1.C1)

        Set to empty key:

        >>> table_result = table1.key_by()

        Parameters
        ----------
        keys : varargs of type :obj:`str`
            Field(s) to key by.

        Returns
        -------
        :class:`.Table`
            Table with a new key.
        """
        str_keys = []
        for k in keys:
            if isinstance(k, Expression):
                if k not in self._fields_inverse:
                    raise ExpressionException("'key_by' permits only top-level fields of the table")
                elif k._indices != self._row_indices:
                    raise ExpressionException("key_by' expects row fields, found index {}"
                                              .format(list(k._indices.axes)))
                str_keys.append(self._fields_inverse[k])
            else:
                if k not in self._fields:
                    raise LookupError(get_nice_field_error(self, k))
                if not self._fields[k]._indices == self._row_indices:
                    raise ValueError("'{}' is not a row field".format(k))
                str_keys.append(k)

        return Table(self._jt.keyBy(str_keys))

    def annotate_globals(self, **named_exprs):
        """Add new global fields.

        Examples
        --------

        Add a new global field:

        >>> table_result = table1.annotate(pops = ['EUR', 'AFR', 'EAS', 'SAS'])

        Note
        ----
        This method does not support aggregation.

        Parameters
        ----------
        named_exprs : varargs of :class:`.Expression`
            Annotation expressions.

        Returns
        -------
        :class:`.Table`
            Table with new global field(s).
        """

        exprs = []
        named_exprs = {k: to_expr(v) for k, v in named_exprs.items()}
        base, cleanup = self._process_joins(*named_exprs.values())
        for k, v in named_exprs.items():
            analyze('Table.annotate_globals', v, self._global_indices)
            check_collisions(self._fields, k, self._global_indices)
            exprs.append('{k} = {v}'.format(k=escape_id(k), v=v._ast.to_hql()))

        m = Table(base._jt.annotateGlobalExpr(",\n".join(exprs)))
        return cleanup(m)

    def select_globals(self, *exprs, **named_exprs):
        """Select existing global fields or create new fields by name, dropping the rest.

        Examples
        --------
        Select one existing field and compute a new one:

        >>> table_result = table1.select_globals(table1.global_field_1,
        ...                                      another_global=['AFR', 'EUR', 'EAS', 'AMR', 'SAS'])

        Notes
        -----
        This method creates new global fields. If a created field shares its name
        with a row-indexed field of the table, the method will fail.

        Note
        ----

        See :meth:`.Table.select` for more information about using ``select`` methods.

        Note
        ----
        This method does not support aggregation.

        Parameters
        ----------
        exprs : variable-length args of :obj:`str` or :class:`.Expression`
            Arguments that specify field names or nested field reference expressions.
        named_exprs : keyword args of :class:`.Expression`
            Field names and the expressions to compute them.

        Returns
        -------
        :class:`.Table`
            Table with specified global fields.
        """

        exprs = [self[e] if not isinstance(e, Expression) else e for e in exprs]
        named_exprs = {k: to_expr(v) for k, v in named_exprs.items()}
        strs = []
        all_exprs = []
        base, cleanup = self._process_joins(*itertools.chain(exprs, named_exprs.values()))

        ids = []
        for e in exprs:
            all_exprs.append(e)
            analyze('Table.select_globals', e, self._global_indices)
            if e._ast.search(lambda ast: not isinstance(ast, TopLevelReference) and not isinstance(ast, Select)):
                raise ExpressionException("method 'select_globals' expects keyword arguments for complex expressions")
            strs.append(e._ast.to_hql())
            ids.append(e._ast.expand()[0].name)
        for k, e in named_exprs.items():
            all_exprs.append(e)
            analyze('Table.select_globals', e, self._global_indices)
            check_collisions(self._fields, k, self._global_indices)
            strs.append('{} = {}'.format(escape_id(k), to_expr(e)._ast.to_hql()))
            ids.append(k)
        check_field_uniqueness(ids)
        return cleanup(Table(base._jt.selectGlobal(strs)))

    def transmute_globals(self, **named_exprs):
        raise NotImplementedError()

    def transmute(self, **named_exprs):
        """Add new fields and drop fields referenced.

        Examples
        --------

        Create a single field from an expression of `C1`, `C2`, and `C3`.

        .. testsetup::

            table4 = hl.import_table('data/kt_example4.tsv', impute=True,
                                  types={'B': hl.tstruct(B0=hl.tbool, B1=hl.tstr),
                                 'D': hl.tstruct(cat=hl.tint32, dog=hl.tint32),
                                 'E': hl.tstruct(A=hl.tint32, B=hl.tint32)})

        .. doctest::

            >>> table4.show()
            +-------+------+-------+-------+-------+-------+-------+-------+
            |     A | B.B0 | B.B1  | C     | D.cat | D.dog |   E.A |   E.B |
            +-------+------+-------+-------+-------+-------+-------+-------+
            | int32 | bool | str   | bool  | int32 | int32 | int32 | int32 |
            +-------+------+-------+-------+-------+-------+-------+-------+
            |    32 | true | hello | false |     5 |     7 |     5 |     7 |
            +-------+------+-------+-------+-------+-------+-------+-------+

            >>> table_result = table4.transmute(F=table4.A + 2 * table4.E.B)
            >>> table_result.show()
            +------+-------+-------+-------+-------+-------+
            | B.B0 | B.B1  | C     | D.cat | D.dog |     F |
            +------+-------+-------+-------+-------+-------+
            | bool | str   | bool  | int32 | int32 | int32 |
            +------+-------+-------+-------+-------+-------+
            | true | hello | false |     5 |     7 |    46 |
            +------+-------+-------+-------+-------+-------+

        Notes
        -----
        This method functions to create new row-indexed fields and consume
        fields found in the expressions in `named_exprs`.

        All row-indexed top-level fields found in an expression are dropped
        after the new fields are created.

        Warning
        -------
        References to fields inside a top-level struct will remove the entire
        struct, as field `E` was removed in the example above since `E.B` was
        referenced.

        Note
        ----
        This method does not support aggregation.

        Parameters
        ----------
        named_exprs : keyword args of :class:`.Expression`
            New field expressions.

        Returns
        -------
        :class:`.Table`
            Table with transmuted fields.
        """
        named_exprs = {k: to_expr(v) for k, v in named_exprs.items()}
        exprs = []
        base, cleanup = self._process_joins(*named_exprs.values())
        fields_referenced = set()
        for k, v in named_exprs.items():
            analyze('Table.transmute', v, self._row_indices)
            check_collisions(self._fields, k, self._row_indices)
            exprs.append('{k} = {v}'.format(k=escape_id(k), v=v._ast.to_hql()))
            for name, inds in get_refs(v):
                if inds == self._row_indices:
                    fields_referenced.add(name)
        fields_referenced = fields_referenced - set(named_exprs.keys())

        return cleanup(Table(base._jt
                             .annotate(",\n".join(exprs))
                             .drop(list(fields_referenced))))

    def annotate(self, **named_exprs):
        """Add new fields.

        Examples
        --------

        Add field `Y` by computing the square of `X`:

        >>> table_result = table1.annotate(Y = table1.X ** 2)

        Add multiple fields simultaneously:

        >>> table_result = table1.annotate(A = table1.X / 2,
        ...                                B = table1.X + 21)

        Parameters
        ----------
        named_exprs : keyword args of :class:`.Expression`
            Expressions for new fields.

        Returns
        -------
        :class:`.Table`
            Table with new fields.
        """
        named_exprs = {k: to_expr(v) for k, v in named_exprs.items()}
        exprs = []
        base, cleanup = self._process_joins(*named_exprs.values())
        for k, v in named_exprs.items():
            analyze('Table.annotate', v, self._row_indices)
            check_collisions(self._fields, k, self._row_indices)
            exprs.append('{k} = {v}'.format(k=escape_id(k), v=v._ast.to_hql()))
        return cleanup(Table(base._jt.annotate(",\n".join(exprs))))

    @typecheck_method(expr=anytype,
                      keep=bool)
    def filter(self, expr, keep=True):
        """Filter rows.

        Examples
        --------

        Keep rows where ``C1`` equals 5:

        >>> table_result = table1.filter(table1.C1 == 5)

        Remove rows where ``C1`` equals 10:

        >>> table_result = table1.filter(table1.C1 == 10, keep=False)

        Notes
        -----

        The expression `expr` will be evaluated for every row of the table. If `keep`
        is ``True``, then rows where `expr` evaluates to ``False`` will be removed (the
        filter keeps the rows where the predicate evaluates to ``True``). If `keep` is
        ``False``, then rows where `expr` evaluates to ``False`` will be removed (the
        filter removes the rows where the predicate evaluates to ``True``).

        Warning
        -------
        When `expr` evaluates to missing, the row will be removed regardless of `keep`.

        Note
        ----
        This method does not support aggregation.

        Parameters
        ----------
        expr : bool or :class:`.BooleanExpression`
            Filter expression.
        keep : bool
            Keep rows where `expr` is true.

        Returns
        -------
        :class:`.Table`
            Filtered table.
        """
        expr = to_expr(expr)
        analyze('Table.filter', expr, self._row_indices)
        base, cleanup = self._process_joins(expr)
        if expr.dtype != tbool:
            raise TypeError("method 'filter' expects an expression of type 'bool', found '{}'"
                            .format(expr.dtype))

        return cleanup(Table(base._jt.filter(expr._ast.to_hql(), keep)))

    @typecheck_method(exprs=oneof(Expression, str),
                      named_exprs=anytype)
    def select(self, *exprs, **named_exprs):
        """Select existing fields or create new fields by name, dropping the rest.

        Examples
        --------
        Select a few old fields and compute a new one:

        >>> table_result = table1.select(table1.ID, table1.C1, Y=table1.Z - table1.X)

        Notes
        -----
        This method creates new row-indexed fields. If a created field shares its name
        with a global field of the table, the method will fail.

        Note
        ----

        **Using select**

        Select and its sibling methods (:meth:`.Table.select_globals`,
        :meth:`.MatrixTable.select_globals`, :meth:`.MatrixTable.select_rows`,
        :meth:`.MatrixTable.select_cols`, and :meth:`.MatrixTable.select_entries`) accept
        both variable-length (``f(x, y, z)``) and keyword (``f(a=x, b=y, c=z)``)
        arguments.

        Variable-length arguments can be either strings or expressions that reference a
        (possibly nested) field of the table. Keyword arguments can be arbitrary
        expressions.

        **The following three usages are all equivalent**, producing a new table with
        fields `C1` and `C2` of `table1`.

        First, variable-length string arguments:

        >>> table_result = table1.select('C1', 'C2')

        Second, field reference variable-length arguments:

        >>> table_result = table1.select(table1.C1, table1.C2)

        Last, expression keyword arguments:

        >>> table_result = table1.select(C1 = table1.C1, C2 = table1.C2)

        Additionally, the variable-length argument syntax also permits nested field
        references. Given the following struct field `s`:

        >>> table3 = table1.annotate(s = hl.struct(x=table1.X, z=table1.Z))

        The following two usages are equivalent, producing a table with one field, `x`.:

        >>> table3_result = table3.select(table3.s.x)

        >>> table3_result = table3.select(x = table3.s.x)

        The keyword argument syntax permits arbitrary expressions:

        >>> table_result = table1.select(foo=table1.X ** 2 + 1)

        These syntaxes can be mixed together, with the stipulation that all keyword arguments
        must come at the end due to Python language restrictions.

        >>> table_result = table1.select(table1.X, 'Z', bar = [table1.C1, table1.C2])

        Note
        ----
        This method does not support aggregation.

        Parameters
        ----------
        exprs : variable-length args of :obj:`str` or :class:`.Expression`
            Arguments that specify field names or nested field reference expressions.
        named_exprs : keyword args of :class:`.Expression`
            Field names and the expressions to compute them.

        Returns
        -------
        :class:`.Table`
            Table with specified fields.
        """
        exprs = [self[e] if not isinstance(e, Expression) else e for e in exprs]
        named_exprs = {k: to_expr(v) for k, v in named_exprs.items()}
        strs = []
        all_exprs = []
        base, cleanup = self._process_joins(*itertools.chain(exprs, named_exprs.values()))

        ids = []
        for e in exprs:
            all_exprs.append(e)
            analyze('Table.select', e, self._row_indices)
            if e._ast.search(lambda ast: not isinstance(ast, TopLevelReference) and not isinstance(ast, Select)):
                raise ExpressionException("method 'select' expects keyword arguments for complex expressions")
            strs.append(e._ast.to_hql())
            ids.append(e._ast.expand()[0].name)
        for k, e in named_exprs.items():
            all_exprs.append(e)
            analyze('Table.select', e, self._row_indices)
            check_collisions(self._fields, k, self._row_indices)
            strs.append('{} = {}'.format(escape_id(k), to_expr(e)._ast.to_hql()))
            ids.append(k)

        check_field_uniqueness(ids)
        return cleanup(Table(base._jt.select(strs)))

    @typecheck_method(exprs=oneof(str, Expression))
    def drop(self, *exprs):
        """Drop fields from the table.

        Examples
        --------

        Drop fields `C1` and `C2` using strings:

        >>> table_result = table1.drop('C1', 'C2')

        Drop fields `C1` and `C2` using field references:

        >>> table_result = table1.drop(table1.C1, table1.C2)

        Drop a list of fields:

        >>> fields_to_drop = ['C1', 'C2']
        >>> table_result = table1.drop(*fields_to_drop)

        Notes
        -----

        This method can be used to drop global or row-indexed fields. The arguments
        can be either strings (``'field'``), or top-level field references
        (``table.field`` or ``table['field']``).

        Parameters
        ----------
        exprs : varargs of :obj:`str` or :class:`.Expression`
            Names of fields to drop or field reference expressions.

        Returns
        -------
        :class:`.Table`
            Table without specified fields.
        """
        all_field_exprs = {e: k for k, e in self._fields.items()}
        fields_to_drop = set()
        for e in exprs:
            if isinstance(e, Expression):
                if e in all_field_exprs:
                    fields_to_drop.add(all_field_exprs[e])
                else:
                    raise ExpressionException("method 'drop' expects string field names or top-level field expressions"
                                              " (e.g. table['foo'])")
            else:
                assert isinstance(e, str)
                if e not in self._fields:
                    raise IndexError("table has no field '{}'".format(e))
                fields_to_drop.add(e)

        table = self
        if any(self._fields[field]._indices == self._global_indices for field in fields_to_drop):
            # need to drop globals
            new_global_fields = [f for f in table.globals if
                                 f not in fields_to_drop]
            table = table.select_globals(*new_global_fields)

        if any(self._fields[field]._indices == self._row_indices for field in fields_to_drop):
            # need to drop row fields
            new_row_fields = [f for f in table.row if
                              f not in fields_to_drop]
            table = table.select(*new_row_fields)

        return table

    def export(self, output, types_file=None, header=True, parallel=None):
        """Export to a TSV file.

        Examples
        --------
        Export to a tab-separated file:

        >>> table1.export('output/table1.tsv.bgz')

        Note
        ----
        It is highly recommended to export large files with a ``.bgz`` extension,
        which will use a block gzipped compression codec. These files can be
        read natively with any Hail method, as well as with Python's ``gzip.open``
        and R's ``read.table``.

        Parameters
        ----------
        output : str
            URI at which to write exported file.
        types_file : str or None
            URI at which to write file containing field type information.
        header : bool
            Include a header in the file.
        parallel : str or None
            If None, a single file is produced, otherwise a
            folder of file shards is produced. If 'separate_header',
            the header file is output separately from the file shards. If
            'header_per_shard', each file shard has a header. If set to None
            the export will be slower.
        """

        self._jt.export(output, types_file, header, Env.hail().utils.ExportType.getExportType(parallel))

    def group_by(self, *exprs, **named_exprs):
        """Group by a new key for use with :meth:`.GroupedTable.aggregate`.

        Examples
        --------
        Compute the mean value of `X` and the sum of `Z` per unique `ID`:

        >>> table_result = (table1.group_by(table1.ID)
        ...                       .aggregate(meanX = agg.mean(table1.X), sumZ = agg.sum(table1.Z)))

        Group by a height bin and compute sex ratio per bin:

        >>> table_result = (table1.group_by(height_bin = table1.HT // 20)
        ...                       .aggregate(fraction_female = agg.fraction(table1.SEX == 'F')))

        Notes
        -----
        This function is always followed by :meth:`.GroupedTable.aggregate`. Follow the
        link for documentation on the aggregation step.

        Note
        ----
        **Using group_by**

        **group_by** and its sibling methods (:meth:`.MatrixTable.group_rows_by` and
        :meth:`.MatrixTable.group_cols_by`) accept both variable-length (``f(x, y, z)``)
        and keyword (``f(a=x, b=y, c=z)``) arguments.

        Variable-length arguments can be either strings or expressions that reference a
        (possibly nested) field of the table. Keyword arguments can be arbitrary
        expressions.

        **The following three usages are all equivalent**, producing a
        :class:`.GroupedTable` grouped by fields `C1` and `C2` of `table1`.

        First, variable-length string arguments:

        >>> table_result = (table1.group_by('C1', 'C2')
        ...                       .aggregate(meanX = agg.mean(table1.X)))

        Second, field reference variable-length arguments:

        >>> table_result = (table1.group_by(table1.C1, table1.C2)
        ...                       .aggregate(meanX = agg.mean(table1.X)))

        Last, expression keyword arguments:

        >>> table_result = (table1.group_by(C1 = table1.C1, C2 = table1.C2)
        ...                       .aggregate(meanX = agg.mean(table1.X)))

        Additionally, the variable-length argument syntax also permits nested field
        references. Given the following struct field `s`:

        >>> table3 = table1.annotate(s = hl.struct(x=table1.X, z=table1.Z))

        The following two usages are equivalent, grouping by one field, `x`:

        >>> table_result = (table3.group_by(table3.s.x)
        ...                       .aggregate(meanX = agg.mean(table3.X)))

        >>> table_result = (table3.group_by(x = table3.s.x)
        ...                       .aggregate(meanX = agg.mean(table3.X)))

        The keyword argument syntax permits arbitrary expressions:

        >>> table_result = (table1.group_by(foo=table1.X ** 2 + 1)
        ...                       .aggregate(meanZ = agg.mean(table1.Z)))

        These syntaxes can be mixed together, with the stipulation that all keyword arguments
        must come at the end due to Python language restrictions.

        >>> table_result = (table1.group_by(table1.C1, 'C2', height_bin = table1.HT // 20)
        ...                       .aggregate(meanX = agg.mean(table1.X)))

        Note
        ----
        This method does not support aggregation in key expressions.

        Arguments
        ---------
        exprs : varargs of type str or :class:`.Expression`
            Field names or field reference expressions.
        named_exprs : keyword args of type :class:`.Expression`
            Field names and expressions to compute them.

        Returns
        -------
        :class:`.GroupedTable`
            Grouped table; use :meth:`.GroupedTable.aggregate` to complete the aggregation.
        """
        groups = []
        for e in exprs:
            if isinstance(e, str):
                e = self[e]
            else:
                e = to_expr(e)
            analyze('Table.group_by', e, self._row_indices)
            ast = e._ast.expand()
            if any(not isinstance(a, TopLevelReference) and not isinstance(a, Select) for a in ast):
                raise ExpressionException("method 'group_by' expects keyword arguments for complex expressions")
            key = ast[0].name
            groups.append((key, e))
        for k, e in named_exprs.items():
            e = to_expr(e)
            analyze('Table.group_by', e, self._row_indices)
            groups.append((k, e))

        return GroupedTable(self, groups)

    def aggregate(self, expr):
        """Aggregate over rows into a local value.

        Examples
        --------
        Aggregate over rows:

        .. doctest::

            >>> table1.aggregate(hl.struct(fraction_male=agg.fraction(table1.SEX == 'M'),
            ...                            mean_x=agg.mean(table1.X)))
            Struct(fraction_male=0.5, mean_x=6.5)

        Note
        ----
        This method supports (and expects!) aggregation over rows.

        Parameters
        ----------
        expr : :class:`.Expression`
            Aggregation expression.

        Returns
        -------
        any
            Aggregated value dependent on `expr`.
        """
        expr = to_expr(expr)
        base, _ = self._process_joins(expr)
        analyze('Table.aggregate', expr, self._global_indices, {self._row_axis})

        result_json = base._jt.queryJSON(expr._ast.to_hql())
        return expr.dtype._from_json(result_json)

    @typecheck_method(output=str,
                      overwrite=bool,
                      _codec_spec=nullable(str))
    def write(self, output, overwrite=False, _codec_spec=None):
        """Write to disk.

        Examples
        --------

        >>> table1.write('output/table1.kt')

        Note
        ----
        The write path must end in ".kt".

        Warning
        -------
        Do not write to a path that is being read from in the same computation.

        Parameters
        ----------
        output : str
            Path at which to write.
        overwrite : bool
            If ``True``, overwrite an existing file at the destination.
        """

        self._jt.write(output, overwrite, _codec_spec)

    @typecheck_method(n=int, width=int, truncate=nullable(int), types=bool)
    def show(self, n=10, width=90, truncate=None, types=True):
        """Print the first few rows of the table to the console.

        Examples
        --------
        Show the first lines of the table:

        .. doctest::

            >>> table1.show()
            +-------+-------+-----+-------+-------+-------+-------+-------+
            |    ID |    HT | SEX |     X |     Z |    C1 |    C2 |    C3 |
            +-------+-------+-----+-------+-------+-------+-------+-------+
            | int32 | int32 | str | int32 | int32 | int32 | int32 | int32 |
            +-------+-------+-----+-------+-------+-------+-------+-------+
            |     1 |    65 | M   |     5 |     4 |     2 |    50 |     5 |
            |     2 |    72 | M   |     6 |     3 |     2 |    61 |     1 |
            |     3 |    70 | F   |     7 |     3 |    10 |    81 |    -5 |
            |     4 |    60 | F   |     8 |     2 |    11 |    90 |   -10 |
            +-------+-------+-----+-------+-------+-------+-------+-------+

        Parameters
        ----------
        n : :obj:`int`
            Maximum number of rows to show.
        width : :obj:`int`
            Horizontal width at which to break fields.
        truncate : :obj:`int`, optional
            Truncate each field to the given number of characters. If
            ``None``, truncate fields to the given `width`.
        types : :obj:`bool`
            Print an extra header line with the type of each field.
        """
        to_print = self._jt.showString(n, joption(truncate), types, width)
        print(to_print)

    def index(self, *exprs):
        exprs = tuple(exprs)
        if not len(exprs) > 0:
            raise ValueError('Require at least one expression to index a table')
        non_exprs = list(filter(lambda e: not isinstance(e, Expression), exprs))
        if non_exprs:
            raise TypeError(f"'Table.index': arguments must be expressions, found {non_exprs}")

        from hail.matrixtable import MatrixTable
        indices, aggregations, joins = unify_all(*exprs)
        src = indices.source

        if src is None or len(indices.axes) == 0:
            # FIXME: this should be OK: table[m.global_index_into_table]
            raise ExpressionException('Cannot index table with a scalar expression')

        def types_compatible(left, right):
            left = list(left)
            right = list(right)
            return (types_match(left, right)
                    or (isinstance(src, MatrixTable)
                        and len(left) == 1
                        and len(right) == 1
                        and isinstance(left[0].dtype, tinterval)
                        and left[0].dtype.point_type == right[0].dtype))

        if not types_compatible(self.key.values(), exprs):
            if (len(exprs) == 1
                    and isinstance(exprs[0], TupleExpression)
                    and types_compatible(self.key.values(), exprs[0])):
                return self.index(*exprs[0])
            elif (len(exprs) == 1
                  and isinstance(exprs[0], StructExpression)
                  and types_compatible(self.key.values(), exprs[0].values())):
                return self.index(*exprs[0].values())
            elif len(exprs) != len(self.key):
                raise ExpressionException(f'Key mismatch: table has {len(self.key)} key fields, '
                                          f'found {len(exprs)} index expressions.')
            else:
                raise ExpressionException(f"Key type mismatch: cannot index table with given expressions:\n"
                                          f"  Table key:         {', '.join(str(t) for t in self.key.dtype.values())}\n"
                                          f"  Index Expressions: {', '.join(str(e.dtype) for e in exprs)}")

        uid = Env.get_uid()

        key_set = set(self.key)
        new_schema = tstruct(**{f: t for f, t in self.row.dtype.items() if f not in key_set})

        if isinstance(src, Table):
            for e in exprs:
                analyze('Table.index', e, src._row_indices)

            right = self
            right_keys = [right[k] for k in right.key]
            right = right.select(*right_keys, **{uid: right.row})
            uids = [Env.get_uid() for i in range(len(exprs))]
            full_key_strs = ',\n'.join('{}={}'.format(uids[i], exprs[i]._ast.to_hql()) for i in range(len(exprs)))

            def joiner(left):
                left = Table(left._jt.annotate(full_key_strs)).key_by(*uids)
                left = Table(left._jt.join(right.distinct()._jt, 'left'))
                return left

            all_uids = uids[:]
            all_uids.append(uid)
            return construct_expr(Select(TopLevelReference('row', src._row_indices), uid), new_schema, indices, aggregations,
                                  joins.push(Join(joiner, all_uids, uid, exprs)))
        elif isinstance(src, MatrixTable):
            for e in exprs:
                analyze('Table.index', e, src._entry_indices)

            right = self
            # match on indices to determine join type
            if indices == src._entry_indices:
                raise NotImplementedError('entry-based matrix joins')
            elif indices == src._row_indices:

                is_row_key = len(exprs) == len(src.row_key) and all(
                    exprs[i] is src._fields[list(src.row_key)[i]] for i in range(len(exprs)))
                is_partition_key = len(exprs) == len(src.partition_key) and all(
                    exprs[i] is src._fields[list(src.partition_key)[i]] for i in range(len(exprs)))

                if is_row_key or is_partition_key:
                    # no vds_key (way faster)
                    def joiner(left):
                        return MatrixTable(left._jvds.annotateRowsTable(
                            right._jt, uid, False))

                    return construct_expr(Select(TopLevelReference('va', src._row_indices), uid), new_schema,
                                          indices, aggregations, joins.push(Join(joiner, [uid], uid, exprs)))
                else:
                    # use vds_key
                    uids = [Env.get_uid() for _ in range(len(exprs))]

                    def joiner(left):
                        from hail.expr import aggregators as agg

                        rk_uids = [Env.get_uid() for _ in left.row_key]
                        k_uid = Env.get_uid()

                        # extract v, key exprs
                        left2 = left.select_rows(*list(left.row_key), **{uid: e for uid, e in zip(uids, exprs)})
                        lrt = (left2.rows()
                            .rename({name: u for name, u in zip(left2.row_key, rk_uids)})
                            .key_by(*uids))

                        vt = lrt.join(right)
                        # group uids
                        vt = vt.annotate(**{k_uid: Struct(**{u: vt[u] for u in uids})})
                        vt = vt.drop(*uids)
                        # group by v and index by the key exprs
                        vt = (vt.group_by(*rk_uids)
                            .aggregate(values=agg.collect(
                            Struct(**{c: vt[c] for c in vt.row if c not in rk_uids}))))
                        vt = vt.annotate(values=hl.index(vt.values, k_uid))

                        jl = left._jvds.annotateRowsTable(vt._jt, uid, False)
                        key_expr = '{uid} = va.{uid}.values.get({{ {es} }})'.format(uid=uid, es=','.join(
                            '{}: {}'.format(u, e._ast.to_hql()) for u, e in
                            zip(uids, exprs)))
                        jl = jl.annotateRowsExpr(key_expr)
                        return MatrixTable(jl)

                    return construct_expr(Select(TopLevelReference('va', src._row_indices), uid),
                                          new_schema, indices, aggregations, joins.push(Join(joiner, [uid], uid, exprs)))

            elif indices == src._col_indices:
                if len(exprs) == len(src.col_key) and all([
                        exprs[i] is src[list(src.col_key)[i]] for i in range(len(exprs))]):
                    # no vds_key (faster)
                    joiner = lambda left: MatrixTable(left._jvds.annotateColsTable(
                        right._jt, None, uid, False))
                else:
                    # use vds_key
                    joiner = lambda left: MatrixTable(left._jvds.annotateColsTable(
                        right._jt, [e._ast.to_hql() for e in exprs], uid, False))
                return construct_expr(Select(TopLevelReference('sa', src._col_indices), uid), new_schema,
                                      indices, aggregations, joins.push(Join(joiner, [uid], uid, exprs)))
            else:
                raise NotImplementedError()
        else:
            raise TypeError("Cannot join with expressions derived from '{}'".format(src.__class__))

    def index_globals(self):
        uid = Env.get_uid()

        def joiner(obj):
            from hail.matrixtable import MatrixTable
            if isinstance(obj, MatrixTable):
                return MatrixTable(Env.jutils().joinGlobals(obj._jvds, self._jt, uid))
            else:
                assert isinstance(obj, Table)
                return Table(Env.jutils().joinGlobals(obj._jt, self._jt, uid))

        return construct_expr(Select(TopLevelReference('global', indices=Indices()), uid), self.globals.dtype,
                              joins=LinkedList(Join).push(Join(joiner, [uid], uid, [])))

    def _process_joins(self, *exprs):
        # ordered to support nested joins
        original_key = list(self.key)

        all_uids = []
        left = self
        used_uids = set()

        for e in exprs:
            for j in list(e._joins)[::-1]:
                if j.uid not in used_uids:
                    left = j.join_function(left)
                    all_uids.extend(j.temp_vars)
                    used_uids.add(j.uid)

        if left is not self:
            left = left.key_by(*original_key)

        def cleanup(table):
            remaining_uids = [uid for uid in all_uids if uid in table._fields]
            return table.drop(*remaining_uids)

        return left, cleanup

    def cache(self):
        """Persist this table in memory.

        Examples
        --------
        Persist the table in memory:

        >>> table = table.cache() # doctest: +SKIP

        Notes
        -----

        This method is an alias for :func:`persist("MEMORY_ONLY") <hail.Table.persist>`.

        Returns
        -------
        :class:`.Table`
            Cached table.
        """
        return self.persist('MEMORY_ONLY')

    @typecheck_method(storage_level=storage_level)
    def persist(self, storage_level='MEMORY_AND_DISK'):
        """Persist this table in memory or on disk.

        Examples
        --------
        Persist the table to both memory and disk:

        >>> table = table.persist() # doctest: +SKIP

        Notes
        -----

        The :meth:`.Table.persist` and :meth:`.Table.cache` methods store the
        current table on disk or in memory temporarily to avoid redundant computation
        and improve the performance of Hail pipelines. This method is not a substitution
        for :meth:`.Table.write`, which stores a permanent file.

        Most users should use the "MEMORY_AND_DISK" storage level. See the `Spark
        documentation
        <http://spark.apache.org/docs/latest/programming-guide.html#rdd-persistence>`__
        for a more in-depth discussion of persisting data.

        Parameters
        ----------
        storage_level : str
            Storage level.  One of: NONE, DISK_ONLY,
            DISK_ONLY_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER,
            MEMORY_ONLY_SER_2, MEMORY_AND_DISK, MEMORY_AND_DISK_2,
            MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, OFF_HEAP

        Returns
        -------
        :class:`.Table`
            Persisted table.
        """
        return Table(self._jt.persist(storage_level))

    def unpersist(self):
        """
        Unpersists this table from memory/disk.

        Notes
        -----
        This function will have no effect on a table that was not previously
        persisted.

        Returns
        -------
        :class:`.Table`
            Unpersisted table.
        """
        self._jt.unpersist()

    def collect(self):
        """Collect the rows of the table into a local list.

        Examples
        --------
        Collect a list of all `X` records:

        >>> all_xs = [row['X'] for row in table1.select(table1.X).collect()]

        Notes
        -----
        This method returns a list whose elements are of type :class:`.Struct`. Fields
        of these structs can be accessed similarly to fields on a table, using dot
        methods (``struct.foo``) or string indexing (``struct['foo']``).

        Warning
        -------
        Using this method can cause out of memory errors. Only collect small tables.

        Returns
        -------
        :obj:`list` of :class:`.Struct`
            List of rows.
        """
        return hl.tarray(self.row.dtype)._from_json(self._jt.collectJSON())

    def describe(self):
        """Print information about the fields in the table."""

        def format_type(typ):
            return typ.pretty(indent=4)

        if len(self.globals.dtype) == 0:
            global_fields = '\n    None'
        else:
            global_fields = ''.join("\n    '{name}': {type} ".format(
                name=f, type=format_type(t)) for f, t in self.globals.dtype.items())

        if len(self.row) == 0:
            row_fields = '\n    None'
        else:
            row_fields = ''.join("\n    '{name}': {type} ".format(
                name=f, type=format_type(t)) for f, t in self.row.dtype.items())

        row_key = ''.join("\n    '{name}': {type} ".format(name=f, type=format_type(self[f].dtype))
                          for f in self.key) if self.key else '\n    None'

        s = '----------------------------------------\n' \
            'Global fields:{g}\n' \
            '----------------------------------------\n' \
            'Row fields:{r}\n' \
            '----------------------------------------\n' \
            'Key:{rk}\n' \
            '----------------------------------------'.format(g=global_fields,
                                                              rk=row_key,
                                                              r=row_fields)
        print(s)

    @typecheck_method(name=str)
    def add_index(self, name='idx'):
        """Add the integer index of each row as a new row field.

        Examples
        --------

        .. doctest::

            >>> table_result = table1.add_index()
            >>> table_result.show()
            +-------+-------+-----+-------+-------+-------+-------+-------+-------+
            |    ID |    HT | SEX |     X |     Z |    C1 |    C2 |    C3 |   idx |
            +-------+-------+-----+-------+-------+-------+-------+-------+-------+
            | int32 | int32 | str | int32 | int32 | int32 | int32 | int32 | int64 |
            +-------+-------+-----+-------+-------+-------+-------+-------+-------+
            |     1 |    65 | M   |     5 |     4 |     2 |    50 |     5 |     0 |
            |     2 |    72 | M   |     6 |     3 |     2 |    61 |     1 |     1 |
            |     3 |    70 | F   |     7 |     3 |    10 |    81 |    -5 |     2 |
            |     4 |    60 | F   |     8 |     2 |    11 |    90 |   -10 |     3 |
            +-------+-------+-----+-------+-------+-------+-------+-------+-------+

        Notes
        -----

        This method returns a table with a new field whose name is given by
        the `name` parameter, with type :py:data:`.tint64`. The value of this field
        is the integer index of each row, starting from 0. Methods that respect
        ordering (like :meth:`.Table.take` or :meth:`.Table.export`) will
        return rows in order.

        This method is also helpful for creating a unique integer index for
        rows of a table so that more complex types can be encoded as a simple
        number for performance reasons.

        Parameters
        ----------
        name : str
            Name of index field.

        Returns
        -------
        :class:`.Table`
            Table with a new index field.
        """

        return Table(self._jt.index(name))

    @typecheck_method(tables=table_type)
    def union(self, *tables):
        """Union the rows of multiple tables.

        Examples
        --------

        Take the union of rows from two tables:

        .. testsetup::

            table = hl.import_table('data/kt_example1.tsv', impute=True, key='ID')
            other_table = table

        >>> union_table = table.union(other_table)

        Notes
        -----

        If a row appears in both tables identically, it is duplicated in the
        result. The left and right tables must have the same schema and key.

        Parameters
        ----------
        tables : varargs of :class:`.Table`
            Tables to union.

        Returns
        -------
        :class:`.Table`
            Table with all rows from each component table.
        """

        return Table(self._jt.union([table._jt for table in tables]))

    @typecheck_method(n=int)
    def take(self, n):
        """Collect the first `n` rows of the table into a local list.

        Examples
        --------
        Take the first three rows:

        .. doctest::

            >>> first3 = table1.take(3)
            >>> print(first3)
            [Struct(HT=65, SEX=M, X=5, C3=5, C2=50, C1=2, Z=4, ID=1),
             Struct(HT=72, SEX=M, X=6, C3=1, C2=61, C1=2, Z=3, ID=2),
             Struct(HT=70, SEX=F, X=7, C3=-5, C2=81, C1=10, Z=3, ID=3)]

        Notes
        -----

        This method does not need to look at all the data in the table, and
        allows for fast queries of the start of the table.

        This method is equivalent to :meth:`.Table.head` followed by
        :meth:`.Table.collect`.

        Parameters
        ----------
        n : int
            Number of rows to take.

        Returns
        -------
        :obj:`list` of :class:`.Struct`
            List of row structs.
        """

        return hl.tarray(self.row.dtype)._from_json(self._jt.takeJSON(n))

    @typecheck_method(n=int)
    def head(self, n):
        """Subset table to first `n` rows.

        Examples
        --------
        Subset to the first three rows:

        .. doctest::

            >>> table_result = table1.head(3)
            >>> table_result.count()
            3

        Notes
        -----

        The number of partitions in the new table is equal to the number of
        partitions containing the first `n` rows.

        Parameters
        ----------
        n : int
            Number of rows to include.

        Returns
        -------
        :class:`.Table`
            Table including the first `n` rows.
        """

        return Table(self._jt.head(n))

    @typecheck_method(p=numeric,
                      seed=int)
    def sample(self, p, seed=0):
        """Downsample the table by keeping each row with probability ``p``.

        Examples
        --------

        Downsample the table to approximately 1% of its rows.

        >>> small_table1 = table1.sample(0.01)

        Parameters
        ----------
        p : :obj:`float`
            Probability of keeping each row.
        seed : :obj:`int`
            Random seed.

        Returns
        -------
        :class:`.Table`
            Table with approximately ``p * n_rows`` rows.
        """

        if not (0 <= p <= 1):
            raise ValueError("Requires 'p' in [0,1]. Found p={}".format(p))

        return Table(self._jt.sample(p, seed))

    @typecheck_method(n=int,
                      shuffle=bool)
    def repartition(self, n, shuffle=True):
        """Change the number of distributed partitions.

        Examples
        --------
        Repartition to 10 partitions:

        >>> table_result = table1.repartition(10)

        Warning
        -------
        When `shuffle` is ``False``, `repartition` can only decrease the number
        of partitions and simply combines adjacent partitions to achieve the
        desired number. It does not attempt to rebalance and so can produce a
        heavily unbalanced dataset. An unbalanced dataset can be inefficient to
        operate on because the work is not evenly distributed across partitions.

        Parameters
        ----------
        n : int
            Desired number of partitions.
        shuffle : bool
            If ``True``, shuffle data. Otherwise, naively coalesce.

        Returns
        -------
        :class:`.Table`
            Repartitioned table.
        """

        return Table(self._jt.repartition(n, shuffle))

    @typecheck_method(right=table_type,
                      how=enumeration('inner', 'outer', 'left', 'right'))
    def join(self, right, how='inner'):
        """Join two tables together.

        Examples
        --------
        Join `table1` to `table2` to produce `table_joined`:

        >>> table_joined = table1.key_by('ID').join(table2.key_by('ID'))

        Notes
        -----
        Hail supports four types of joins specified by `how`:

        - **inner** -- Key must be present in both the left and right tables.
        - **outer** -- Key present in either the left or the right. For keys
          only in the left table, the right table's fields will be missing.
          For keys only in the right table, the left table's fields will be
          missing.
        - **left** -- Key present in the left table. For keys not found on
          the right, the right table's fields will be missing.
        - **right** -- Key present in the right table. For keys not found on
          the right, the right table's fields will be missing.

        Both tables must have the same number of keys and the corresponding
        types of each key must be the same (order matters), but the key names
        can be different. For example, if `table1` is keyed by fields ``['a',
        'b']``, both of type ``int32``, and `table2` is keyed by fields ``['c',
        'd']``, both of type ``int32``, then the two tables can be joined (their
        rows will be joined where ``table1.a == table2.c`` and ``table1.b ==
        table2.d``).

        The key fields and order from the left table are preserved,
        while the key fields from the right table are not present in
        the result.

        Parameters
        ----------
        right : :class:`.Table`
            Table with which to join.
        how : :obj:`str`
            Join type. One of "inner", "outer", "left", "right".

        Returns
        -------
        :class:`.Table`
            Joined table.

        """

        return Table(self._jt.join(right._jt, how))

    @typecheck_method(expr=BooleanExpression)
    def all(self, expr):
        """Evaluate whether a boolean expression is true for all rows.

        Examples
        --------
        Test whether `C1` is greater than 5 in all rows of the table:

        >>> if table1.all(table1.C1 == 5):
        ...     print("All rows have C1 equal 5.")

        Parameters
        ----------
        expr : :class:`.BooleanExpression`
            Expression to test.

        Returns
        -------
        :obj:`bool`
        """
        return self.aggregate(hail.agg.all(expr))

    @typecheck_method(expr=BooleanExpression)
    def any(self, expr):
        """Evaluate whether a Boolean expression is true for at least one row.

        Examples
        --------

        Test whether `C1` is equal to 5 any row in any row of the table:

        >>> if table1.any(table1.C1 == 5):
        ...     print("At least one row has C1 equal 5.")

        Parameters
        ----------
        expr : :class:`.BooleanExpression`
            Boolean expression.

        Returns
        -------
        :obj:`bool`
            ``True`` if the predicate evaluated for ``True`` for any row, otherwise ``False``.
        """
        return self.aggregate(hail.agg.any(expr))

    @typecheck_method(mapping=dictof(str, str))
    def rename(self, mapping):
        """Rename fields of the table.

        Examples
        --------
        Rename `C1` to `col1` and `C2` to `col2`:

        >>> table_result = table1.rename({'C1' : 'col1', 'C2' : 'col2'})

        Parameters
        ----------
        mapping : :obj:`dict` of :obj:`str`, :obj:`str`
            Mapping from old field names to new field names.

        Notes
        -----
        Any field that does not appear as a key in `mapping` will not be
        renamed.

        Returns
        -------
        :class:`.Table`
            Table with renamed fields.
        """
        seen = {}

        row_map = {}
        global_map = {}

        for k, v in mapping.items():
            if v in seen:
                raise ValueError(
                    "Cannot rename two fields to the same name: attempted to rename {} and {} both to {}".format(
                        repr(seen[v]), repr(k), repr(v)))
            if v in self._fields and v not in mapping:
                raise ValueError("Cannot rename {} to {}: field already exists.".format(repr(k), repr(v)))
            seen[v] = k
            if self[k]._indices == self._row_indices:
                row_map[k] = v
            elif self[k]._indices == self._global_indices:
                global_map[k] = v

        return Table(self._jt.rename(row_map, global_map))

    def expand_types(self):
        """Expand complex types into structs and arrays.

        Examples
        --------

        >>> table_result = table1.expand_types()

        Notes
        -----
        Expands the following types: :class:`.tlocus`, :class:`.tinterval`,
        :class:`.tset`, :class:`.tdict`.

        The only types that will remain after this method are:
        :py:data:`.tbool`, :py:data:`.tint32`, :py:data:`.tint64`,
        :py:data:`.tfloat64`, :py:data:`.tfloat32`, :class:`.tarray`,
        :class:`.tstruct`.

        Returns
        -------
        :class:`.Table`
            Expanded table.
        """

        return Table(self._jt.expandTypes())

    def flatten(self):
        """Flatten nested structs.

        Examples
        --------
        Flatten table:

        >>> table_result = table1.flatten()

        Notes
        -----
        Consider a table with signature

        .. code-block:: text

            a: struct{
                p: int32,
                q: str
            },
            b: int32,
            c: struct{
                x: str,
                y: array<struct{
                    y: str,
                    z: str
                }>
            }

        and key ``a``.  The result of flatten is

        .. code-block:: text

            a.p: int32
            a.q: str
            b: int32
            c.x: str
            c.y: array<struct{
                y: str,
                z: str
            }>

        with key ``a.p, a.q``.

        Note, structures inside collections like arrays or sets will not be
        flattened.

        Warning
        -------
        Flattening a table will produces fields that cannot be referenced using
        the ``table.<field>`` syntax, e.g. "a.b". Reference these fields using
        square bracket lookups: ``table['a.b']``.

        Returns
        -------
        :class:`.Table`
            Table with a flat schema (no struct fields).
        """

        return Table(self._jt.flatten())

    @typecheck_method(exprs=oneof(str, Expression, Ascending, Descending))
    def order_by(self, *exprs):
        """Sort by the specified fields.

        Examples
        --------
        Four equivalent ways to order the table by field `HT`, ascending:

        >>> sorted_table = table1.order_by(table1.HT)

        >>> sorted_table = table1.order_by('HT')

        >>> sorted_table = table1.order_by(hl.asc(table1.HT))

        >>> sorted_table = table1.order_by(hl.asc('HT'))

        Notes
        -----
        Missing values are sorted after non-missing values. When multiple
        fields are passed, the table will be sorted first by the first
        argument, then the second, etc.

        Parameters
        ----------
        exprs : varargs of :class:`.Ascending` or :class:`.Descending` or :class:`.Expression` or :obj:`str`
            Fields to sort by.

        Returns
        -------
        :class:`.Table`
            Table sorted by the given fields.
        """
        sort_cols = []
        for e in exprs:
            if isinstance(e, str):
                expr = self[e]
                if not expr._indices == self._row_indices:
                    raise ValueError("Sort fields must be row-indexed, found global field '{}'".format(e))
                sort_cols.append(asc(e)._j_obj())
            elif isinstance(e, Expression):
                if not e in self._fields_inverse:
                    raise ValueError("Expect top-level field, found a complex expression")
                if not e._indices == self._row_indices:
                    raise ValueError("Sort fields must be row-indexed, found global field '{}'".format(e))
                sort_cols.append(asc(self._fields_inverse[e])._j_obj())
            else:
                assert isinstance(e, Ascending) or isinstance(e, Descending)
                if isinstance(e.col, str):
                    expr = self[e.col]
                    if not expr._indices == self._row_indices:
                        raise ValueError("Sort fields must be row-indexed, found global field '{}'".format(e))
                    sort_cols.append(e._j_obj())
                else:
                    if not e.col in self._fields_inverse:
                        raise ValueError("Expect top-level field, found a complex expression")
                    if not e.col._indices == self._row_indices:
                        raise ValueError("Sort fields must be row-indexed, found global field '{}'".format(e))
                    e.col = self._fields_inverse[e.col]
                    sort_cols.append(e._j_obj())
        return Table(self._jt.orderBy(jarray(Env.hail().table.SortColumn, sort_cols)))

    @typecheck_method(field=oneof(str, Expression))
    def explode(self, field):
        """Explode rows along a top-level field of the table.

        Each row is copied for each element of `field`.
        The explode operation unpacks the elements in a field of type
        ``Array`` or ``Set`` into its own row. If an empty ``Array`` or ``Set``
        is exploded, the entire row is removed from the table.

        Examples
        --------

        `people_table` is a :class:`.Table` with three fields: `Name`, `Age` and `Children`.

        .. testsetup::

            people_table = hl.import_table('data/explode_example.tsv', delimiter='\\s+',
                                     types={'Age': hl.tint32, 'Children': hl.tarray(hl.tstr)})

        .. doctest::

            >>> people_table.show()
            +----------+-------+--------------------------+
            | Name     |   Age | Children                 |
            +----------+-------+--------------------------+
            | str      | int32 | array<str>               |
            +----------+-------+--------------------------+
            | Alice    |    34 | ["Dave","Ernie","Frank"] |
            | Bob      |    51 | ["Gaby","Helen"]         |
            | Caroline |    10 | []                       |
            +----------+-------+--------------------------+

        :meth:`.Table.explode` can be used to produce a distinct row for each
        element in the `Children` field:

        .. doctest::

            >>> exploded = people_table.explode('Children')
            >>> exploded.show()
            +-------+-------+----------+
            | Name  |   Age | Children |
            +-------+-------+----------+
            | str   | int32 | str      |
            +-------+-------+----------+
            | Alice |    34 | Dave     |
            | Alice |    34 | Ernie    |
            | Alice |    34 | Frank    |
            | Bob   |    51 | Gaby     |
            | Bob   |    51 | Helen    |
            +-------+-------+----------+

        Notes
        -----
        Empty arrays or sets produce no rows in the resulting table. In the
        example above, notice that the name "Caroline" is not found in the
        exploded table.

        Missing arrays or sets are treated as empty.

        Parameters
        ----------
        field : :obj:`str` or :class:`.Expression`
            Top-level field name or expression.

        Returns
        -------
        :class:`.Table`
            Table with exploded field.
        """

        if not isinstance(field, Expression):
            # field is a str
            field = self[field]

        if not field in self._fields_inverse:
            # nested or complex expression
            raise ValueError("method 'explode' expects a top-level field name or expression")
        if not field._indices == self._row_indices:
            # global field
            assert field._indices == self._global_indices
            raise ValueError("method 'explode' expects a field indexed by ['row'], found global field")

        return Table(self._jt.explode(self._fields_inverse[field]))

    @typecheck_method(row_key=expr_any,
                      col_key=expr_any,
                      entry_exprs=expr_any)
    def to_matrix_table(self, row_key, col_key, **entry_exprs):

        from hail.matrixtable import MatrixTable

        all_exprs = []
        all_exprs.append(row_key)
        analyze('to_matrix_table/row_key', row_key, self._row_indices)
        all_exprs.append(col_key)
        analyze('to_matrix_table/col_key', col_key, self._row_indices)

        exprs = []

        for k, e in entry_exprs.items():
            all_exprs.append(e)
            analyze('to_matrix_table/entry_exprs/{}'.format(k), e, self._row_indices)
            exprs.append('`{k}` = {v}'.format(k=k, v=e._ast.to_hql()))

        base, cleanup = self._process_joins(*all_exprs)

        return MatrixTable(base._jt.toMatrixTable(row_key._ast.to_hql(),
                                                  col_key._ast.to_hql(),
                                                  ",\n".join(exprs),
                                                  joption(None)))

    @property
    def globals(self) -> 'StructExpression':
        """Returns a struct expression including all global fields.

        Examples
        --------
        The data type of the globals struct:

        >>> table1.globals.dtype
        dtype('struct{}')

        The number of global fields:

        >>> len(table1.globals)
        0

        Returns
        -------
        :class:`.StructExpression`
            Struct of all global fields.
        """
        return self._globals

    @property
    def row(self) -> 'StructExpression':
        """Returns a struct expression including all row-indexed fields.

        Examples
        --------
        The data type of the row struct:

        >>> table1.row.dtype
        dtype('struct{ID: int32, HT: int32, SEX: str, X: int32, Z: int32, C1: int32, C2: int32, C3: int32}')

        The number of row fields:

        >>> len(table1.row)
        8

        Returns
        -------
        :class:`.StructExpression`
            Struct of all row fields.
        """
        return self._row

    @staticmethod
    @typecheck(df=DataFrame,
               key=oneof(str, listof(str)))
    def from_spark(df, key=[]):
        """Convert PySpark SQL DataFrame to a table.

        Examples
        --------

        >>> t = Table.from_spark(df) # doctest: +SKIP

        Notes
        -----

        Spark SQL data types are converted to Hail types as follows:

        .. code-block:: text

          BooleanType => :py:data:`.tbool`
          IntegerType => :py:data:`.tint32`
          LongType => :py:data:`.tint64`
          FloatType => :py:data:`.tfloat32`
          DoubleType => :py:data:`.tfloat64`
          StringType => :py:data:`.tstr`
          BinaryType => :class:`.TBinary`
          ArrayType => :class:`.tarray`
          StructType => :class:`.tstruct`

        Unlisted Spark SQL data types are currently unsupported.

        Parameters
        ----------
        df : :class:`.pyspark.sql.DataFrame`
            PySpark DataFrame.
        
        key : :obj:`str` or :obj:`list` of :obj:`str`
            Key fields.

        Returns
        -------
        :class:`.Table`
            Table constructed from the Spark SQL DataFrame.
        """
        return Table(Env.hail().table.Table.fromDF(Env.hc()._jhc, df._jdf, wrap_to_list(key)))

    @typecheck_method(expand=bool,
                      flatten=bool)
    def to_spark(self, expand=True, flatten=True):
        """Converts this table to a Spark DataFrame.

        Parameters
        ----------
        expand : :obj:`bool`
            If True, expand_types before converting to Pandas DataFrame.

        flatten : :obj:`bool`
            If True, flatten before converting to Pandas DataFrame.
            If `expand` and `flatten` are True, flatten is run after
            expand so that expanded types are flattened.

        Returns
        -------
        :class:`.pyspark.sql.DataFrame`
            Spark DataFrame constructed from the table.
        """
        jt = self._jt
        if expand:
            jt = jt.expandTypes()
        if flatten:
            jt = jt.flatten()
        return DataFrame(jt.toDF(Env.hc()._jsql_context), Env.hc()._sql_context)

    @typecheck_method(expand=bool,
                      flatten=bool)
    def to_pandas(self, expand=True, flatten=True):
        """Converts this table into a Pandas DataFrame.

        Parameters
        ----------
        expand : :obj:`bool`
            If True, expand_types before converting to Pandas DataFrame.

        flatten : :obj:`bool`
            If True, flatten before converting to Pandas DataFrame.
            If `expand` and `flatten` are True, flatten is run after
            expand so that expanded types are flattened.

        Returns
        -------
        :class:`.pandas.DataFrame`
            Pandas DataFrame constructed from the table.
        """
        return self.to_spark(expand, flatten).toPandas()

    @typecheck_method(other=table_type, tolerance=nullable(numeric))
    def _same(self, other, tolerance=1e-6):
        return self._jt.same(other._jt, tolerance)

    def collect_by_key(self, name: str= 'values') -> 'Table':
        """Collect values for each unique key into an array.

        Examples
        --------
        >>> t1 = hl.Table.parallelize([
        ...     {'t': 'foo', 'x': 4, 'y': 'A'},
        ...     {'t': 'bar', 'x': 2, 'y': 'B'},
        ...     {'t': 'bar', 'x': -3, 'y': 'C'},
        ...     {'t': 'quam', 'x': 0, 'y': 'D'}],
        ...     hl.tstruct(t=hl.tstr, x=hl.tint32, y=hl.tstr),
        ...     key='t')

        >>> t1.show()
        +------+-------+-----+
        | t    |     x | y   |
        +------+-------+-----+
        | str  | int32 | str |
        +------+-------+-----+
        | foo  |     4 | A   |
        | bar  |     2 | B   |
        | bar  |    -3 | C   |
        | quam |     0 | D   |
        +------+-------+-----+

        >>> t1.collect_by_key().show()
        +------+------------------------------------+
        | t    | values                             |
        +------+------------------------------------+
        | str  | array<struct{x: int32, y: str}>    |
        +------+------------------------------------+
        | bar  | [{"x":2,"y":"B"},{"x":-3,"y":"C"}] |
        | foo  | [{"x":4,"y":"A"}]                  |
        | quam | [{"x":0,"y":"D"}]                  |
        +------+------------------------------------+

        Notes
        -----
        The order of the values array is not guaranteed.

        Parameters
        ----------
        name : :obj:`str`
            Field name for all values per key.

        Returns
        -------
        :class:`.Table`
        """
        return Table(self._jt.groupByKey(name))

    def distinct(self) -> 'Table':
        """Keep only one row for each unique key.

        Examples
        --------
        >>> t1 = hl.Table.parallelize([
        ...     {'a': 'foo', 'b': 1},
        ...     {'a': 'bar', 'b': 5},
        ...     {'a': 'bar', 'b': 2}],
        ...     hl.tstruct(a=hl.tstr, b=hl.tint32),
        ...     key='a')

        >>> t1.show()
        +-----+-------+
        | a   |     b |
        +-----+-------+
        | str | int32 |
        +-----+-------+
        | foo |     1 |
        | bar |     5 |
        | bar |     2 |
        +-----+-------+

        >>> t1.distinct().show()
        +-----+-------+
        | a   |     b |
        +-----+-------+
        | str | int32 |
        +-----+-------+
        | bar |     5 |
        | foo |     1 |
        +-----+-------+

        Notes
        -----
        The row chosen per distinct key is not guaranteed.

        Returns
        -------
        :class:`.Table`
        """
        return Table(self._jt.distinctByKey())

table_type.set(Table)
