import hail
import hail as hl
from hail.expr import expressions
from hail.expr.expr_ast import *
from hail.expr.types import *
from hail.genetics import Locus, Call
from hail.utils import Interval, Struct
from hail.utils.java import *
from hail.utils.linkedlist import LinkedList
from hail.utils.misc import plural
from .indices import *


class ExpressionException(Exception):
    def __init__(self, msg=''):
        self.msg = msg
        super(ExpressionException, self).__init__(msg)


class ExpressionWarning(Warning):
    def __init__(self, msg=''):
        self.msg = msg
        super(ExpressionWarning, self).__init__(msg)


def impute_type(x):
    if isinstance(x, Expression):
        return x.dtype
    if isinstance(x, expressions.Aggregable):
        raise ExpressionException("Cannot use the result of 'agg.explode' or 'agg.filter' in expressions\n"
                                  "    These methods produce 'Aggregable' objects that can only be aggregated\n"
                                  "    with aggregator functions or used with further calls to 'agg.explode'\n"
                                  "    and 'agg.filter'. They support no other operations.")
    elif isinstance(x, bool):
        return tbool
    elif isinstance(x, int):
        if hl.tint32.min_value <= x <= hl.tint32.max_value:
            return tint32
        elif hl.tint64.min_value <= x <= hl.tint64.max_value:
            return tint64
        else:
            raise ValueError("Hail has no integer data type large enough to store {}".format(x))
    elif isinstance(x, float):
        return tfloat64
    elif isinstance(x, str):
        return tstr
    elif isinstance(x, Locus):
        return tlocus(x.reference_genome)
    elif isinstance(x, Interval):
        return tinterval(x.point_type)
    elif isinstance(x, Call):
        return tcall
    elif isinstance(x, Struct):
        return tstruct(**{k: impute_type(x[k]) for k in x})
    elif isinstance(x, tuple):
        return ttuple(*(impute_type(element) for element in x))
    elif isinstance(x, list):
        if len(x) == 0:
            raise ExpressionException('Cannot impute type of empty list.')
        ts = {impute_type(element) for element in x}
        unified_type = unify_types_limited(*ts)
        if not unified_type:
            raise ExpressionException("Hail does not support heterogeneous arrays: "
                                      "found list with elements of types {} ".format(list(ts)))
        return tarray(unified_type)
    elif isinstance(x, set):
        if len(x) == 0:
            raise ExpressionException('Cannot impute type of empty set.')
        ts = {impute_type(element) for element in x}
        unified_type = unify_types_limited(*ts)
        if not unified_type:
            raise ExpressionException("Hail does not support heterogeneous sets: "
                                      "found set with elements of types {} ".format(list(ts)))
        return tset(unified_type)
    elif isinstance(x, dict):
        if len(x) == 0:
            raise ExpressionException('Cannot impute type of empty dict.')
        kts = {impute_type(element) for element in x.keys()}
        vts = {impute_type(element) for element in x.values()}
        unified_key_type = unify_types_limited(*kts)
        unified_value_type = unify_types_limited(*vts)
        if not unified_key_type:
            raise ExpressionException("Hail does not support heterogeneous dicts: "
                                      "found dict with keys of types {} ".format(list(kts)))
        if not unified_value_type:
            raise ExpressionException("Hail does not support heterogeneous dicts: "
                                      "found dict with values of types {} ".format(list(vts)))
        return tdict(unified_key_type, unified_value_type)
    elif x is None:
        raise ExpressionException("Hail cannot impute the type of 'None'")
    else:
        raise ExpressionException("Hail cannot automatically impute type of {}: {}".format(type(x), x))


def to_expr(e, dtype=None) -> 'Expression':
    if isinstance(e, Expression):
        if dtype and not dtype == e.dtype:
            raise TypeError("expected expression of type '{}', found expression of type '{}'".format(dtype, e.dtype))
        return e
    if not dtype:
        dtype = impute_type(e)
    x = _to_expr(e, dtype)
    if isinstance(x, Expression):
        return x
    else:
        return hl.literal(x, dtype)


def _to_expr(e, dtype):
    if e is None:
        return hl.null(dtype)
    elif isinstance(e, Expression):
        if e.dtype != dtype:
            assert is_numeric(dtype), 'expected {}, got {}'.format(dtype, e.dtype)
            if dtype == tfloat64:
                return hl.float64(e)
            elif dtype == tfloat32:
                return hl.float32(e)
            else:
                assert dtype == tint64
                return hl.int64(e)
        return e
    elif not is_container(dtype):
        # these are not container types and cannot contain expressions if we got here
        return e
    elif isinstance(dtype, tstruct):
        new_fields = []
        any_expr = False
        for f, t in dtype.items():
            value = _to_expr(e[f], t)
            any_expr = any_expr or isinstance(value, Expression)
            new_fields.append(value)

        if not any_expr:
            return e
        else:
            exprs = [new_fields[i] if isinstance(new_fields[i], Expression)
                     else hl.literal(new_fields[i], dtype[i])
                     for i in range(len(new_fields))]
            indices, aggregations, joins = unify_all(*exprs)
            return expressions.construct_expr(StructDeclaration(list(dtype),
                                                                [expr._ast for expr in exprs]),
                                              dtype, indices, aggregations, joins)

    elif isinstance(dtype, tarray):
        elements = []
        any_expr = False
        for element in e:
            value = _to_expr(element, dtype.element_type)
            any_expr = any_expr or isinstance(value, Expression)
            elements.append(value)
        if not any_expr:
            return e
        else:
            assert (len(elements) > 0)
            exprs = [element if isinstance(element, Expression)
                     else hl.literal(element, dtype.element_type)
                     for element in elements]
            indices, aggregations, joins = unify_all(*exprs)
        return expressions.construct_expr(ArrayDeclaration([expr._ast for expr in exprs]),
                                          dtype, indices, aggregations, joins)
    elif isinstance(dtype, tset):
        elements = []
        any_expr = False
        for element in e:
            value = _to_expr(element, dtype.element_type)
            any_expr = any_expr or isinstance(value, Expression)
            elements.append(value)
        if not any_expr:
            return e
        else:
            assert (len(elements) > 0)
            exprs = [element if isinstance(element, Expression)
                     else hl.literal(element, dtype.element_type)
                     for element in elements]
            indices, aggregations, joins = unify_all(*exprs)
            return hl.set(expressions.construct_expr(ArrayDeclaration([expr._ast for expr in exprs]),
                                                     tarray(dtype.element_type), indices, aggregations, joins))
    elif isinstance(dtype, ttuple):
        elements = []
        any_expr = False
        assert len(e) == len(dtype.types)
        for i in range(len(e)):
            value = _to_expr(e[i], dtype.types[i])
            any_expr = any_expr or isinstance(value, Expression)
            elements.append(value)
        if not any_expr:
            return e
        else:
            exprs = [elements[i] if isinstance(elements[i], Expression)
                     else hl.literal(elements[i], dtype.types[i])
                     for i in range(len(elements))]
            indices, aggregations, joins = unify_all(*exprs)
            return expressions.construct_expr(TupleDeclaration(*[expr._ast for expr in exprs]),
                                              dtype, indices, aggregations, joins)
    elif isinstance(dtype, tdict):
        keys = []
        values = []
        any_expr = False
        for k, v in e.items():
            k_ = _to_expr(k, dtype.key_type)
            v_ = _to_expr(v, dtype.value_type)
            any_expr = any_expr or isinstance(k_, Expression)
            any_expr = any_expr or isinstance(v_, Expression)
            keys.append(k_)
            values.append(v_)
        if not any_expr:
            return e
        else:
            assert len(keys) > 0
            # Here I use `to_expr` to call `lit` the keys and values separately.
            # I anticipate a common mode is statically-known keys and Expression
            # values.
            key_array = to_expr(keys, tarray(dtype.key_type))
            value_array = to_expr(values, tarray(dtype.value_type))
            return hl.dict(hl.zip(key_array, value_array))
    else:
        raise NotImplementedError(dtype)

def unify_all(*exprs) -> Tuple[Indices, LinkedList, LinkedList]:
    assert len(exprs) > 0
    try:
        new_indices = Indices.unify(*[e._indices for e in exprs])
    except ExpressionException:
        # source mismatch
        from collections import defaultdict
        sources = defaultdict(lambda: [])
        for e in exprs:
            from .expression_utils import get_refs
            for name, inds in get_refs(e, *e._aggregations.exprs):
                sources[inds.source].append(str(name))
        raise ExpressionException("Cannot combine expressions from different source objects."
                                  "\n    Found fields from {n} objects:{fields}".format(
            n=len(sources),
            fields=''.join("\n        {}: {}".format(src, fds) for src, fds in sources.items())
        ))
    first, rest = exprs[0], exprs[1:]
    aggregations = first._aggregations
    joins = first._joins
    for e in rest:
        aggregations = aggregations.push(*e._aggregations)
        joins = joins.push(*e._joins)
    return new_indices, aggregations, joins


def unify_types_limited(*ts):
    type_set = set(ts)
    if len(type_set) == 1:
        # only one distinct class
        return next(iter(type_set))
    elif all(is_numeric(t) for t in ts):
        # assert there are at least 2 numeric types
        assert len(type_set) > 1
        if tfloat64 in type_set:
            return tfloat64
        elif tfloat32 in type_set:
            return tfloat32
        else:
            assert type_set == {tint32, tint64}
            return tint64
    else:
        return None


def unify_types(*ts):
    limited_unify = unify_types_limited(*ts)
    if limited_unify is not None:
        return limited_unify
    elif all(isinstance(t, tarray) for t in ts):
        et = unify_types_limited(*(t.element_type for t in ts))
        if et is not None:
            return tarray(et)
        else:
            return None
    else:
        return None


class Expression(object):
    """Base class for Hail expressions."""

    @typecheck_method(ast=AST, type=HailType, indices=Indices, aggregations=LinkedList, joins=LinkedList)
    def __init__(self,
                 ast: AST,
                 type: HailType,
                 indices: Indices = Indices(),
                 aggregations: LinkedList = LinkedList(Aggregation),
                 joins: LinkedList = LinkedList(Join)):
        self._ast = ast
        self._type = type
        self._indices = indices
        self._aggregations = aggregations
        self._joins = joins

    def describe(self):
        """Print information about type, index, and dependencies."""
        if self._aggregations:
            agg_indices = set()
            for a in self._aggregations:
                agg_indices = agg_indices.union(a.indices.axes)
            agg_tag = ' (aggregated)'
            agg_str = f'Includes aggregation with index {list(agg_indices)}\n' \
                      f'    (Aggregation index may be promoted based on context)'
        else:
            agg_tag = ''
            agg_str = ''

        if self._joins:
            n_joins = len(self._joins)
            word = plural('join or literal', n_joins, 'joins or literals')
            join_str = f'\nDepends on {n_joins} {word}'
        else:
            join_str = ''

        bar = '--------------------------------------------------------'
        s = '{bar}\n' \
            'Type:\n' \
            '    {t}\n' \
            '{bar}\n' \
            'Source:\n' \
            '    {src}\n' \
            'Index:\n' \
            '    {inds}{agg_tag}{maybe_bar}{agg}{joins}\n' \
            '{bar}'.format(bar=bar,
                           t=self.dtype.pretty(indent=4),
                           src=self._indices.source.__class__,
                           inds=list(self._indices.axes),
                           maybe_bar='\n' + bar + '\n' if join_str or agg_str else '',
                           agg_tag=agg_tag,
                           agg=agg_str,
                           joins=join_str)
        print(s)

    def __lt__(self, other):
        raise NotImplementedError("'<' comparison with expression of type {}".format(str(self._type)))

    def __le__(self, other):
        raise NotImplementedError("'<=' comparison with expression of type {}".format(str(self._type)))

    def __gt__(self, other):
        raise NotImplementedError("'>' comparison with expression of type {}".format(str(self._type)))

    def __ge__(self, other):
        raise NotImplementedError("'>=' comparison with expression of type {}".format(str(self._type)))

    def __nonzero__(self):
        raise NotImplementedError(
            "The truth value of an expression is undefined\n"
            "    Hint: instead of 'if x', use 'hl.cond(x, ...)'\n"
            "    Hint: instead of 'x and y' or 'x or y', use 'x & y' or 'x | y'\n"
            "    Hint: instead of 'not x', use '~x'")

    def __iter__(self):
        raise TypeError("'Expression' object is not iterable")

    def _promote_scalar(self, typ):
        if typ == tint32:
            return hail.int32(self)
        elif typ == tint64:
            return hail.int64(self)
        elif typ == tfloat32:
            return hail.float32(self)
        else:
            assert typ == tfloat64
            return hail.float64(self)

    def _promote_numeric(self, typ):
        if isinstance(typ, tarray):
            if isinstance(self.dtype, tarray):
                return hail.map(lambda x: x._promote_scalar(typ.element_type), self)
            else:
                return self._promote_scalar(typ.element_type)
        elif isinstance(self, expressions.Aggregable):
            return self._map(lambda x: x._promote_scalar(typ))
        else:
            return self._promote_scalar(typ)

    def _bin_op_numeric_unify_types(self, name, other):
        t = unify_types(self.dtype._scalar_type(), other.dtype._scalar_type())
        if t is None:
            raise NotImplementedError("'{}' {} '{}'".format(
                self.dtype, name, other.dtype))
        if isinstance(self.dtype, tarray) or isinstance(other.dtype, tarray):
            t = tarray(t)
        return t

    def _bin_op_numeric(self, name, other, ret_type_f=None):
        other = to_expr(other)
        unified_type = self._bin_op_numeric_unify_types(name, other)
        me = self._promote_numeric(unified_type)
        other = other._promote_numeric(unified_type)
        if ret_type_f:
            if isinstance(unified_type, tarray):
                ret_type = tarray(ret_type_f(unified_type.element_type))
            else:
                ret_type = ret_type_f(unified_type)
        else:
            ret_type = unified_type
        return me._bin_op(name, other, ret_type)

    def _bin_op_numeric_reverse(self, name, other, ret_type_f=None):
        other = to_expr(other)
        return other._bin_op_numeric(name, self, ret_type_f)

    def _unary_op(self, name):
        return expressions.construct_expr(UnaryOperation(self._ast, name),
                                          self._type, self._indices, self._aggregations, self._joins)

    def _bin_op(self, name, other, ret_type):
        other = to_expr(other)
        indices, aggregations, joins = unify_all(self, other)
        return expressions.construct_expr(BinaryOperation(self._ast, other._ast, name), ret_type, indices, aggregations,
                                          joins)

    def _bin_op_reverse(self, name, other, ret_type):
        other = to_expr(other)
        indices, aggregations, joins = unify_all(self, other)
        return expressions.construct_expr(BinaryOperation(other._ast, self._ast, name), ret_type, indices, aggregations,
                                          joins)

    def _field(self, name, ret_type):
        return expressions.construct_expr(Select(self._ast, name), ret_type, self._indices,
                                          self._aggregations, self._joins)

    def _method(self, name, ret_type, *args):
        args = tuple(to_expr(arg) for arg in args)
        indices, aggregations, joins = unify_all(self, *args)
        return expressions.construct_expr(ClassMethod(name, self._ast, *(a._ast for a in args)),
                                          ret_type, indices, aggregations, joins)

    def _index(self, ret_type, key):
        key = to_expr(key)
        indices, aggregations, joins = unify_all(self, key)
        return expressions.construct_expr(Index(self._ast, key._ast),
                                          ret_type, indices, aggregations, joins)

    def _slice(self, ret_type, start=None, stop=None, step=None):
        if start is not None:
            start = to_expr(start)
            start_ast = start._ast
        else:
            start_ast = None
        if stop is not None:
            stop = to_expr(stop)
            stop_ast = stop._ast
        else:
            stop_ast = None
        if step is not None:
            raise NotImplementedError('Variable slice step size is not currently supported')

        non_null = [x for x in [start, stop] if x is not None]
        indices, aggregations, joins = unify_all(self, *non_null)
        return expressions.construct_expr(Index(self._ast, Slice(start_ast, stop_ast)),
                                          ret_type, indices, aggregations, joins)

    def _bin_lambda_method(self, name, f, input_type, ret_type_f, *args):
        args = (to_expr(arg) for arg in args)
        new_id = Env.get_uid()
        lambda_result = to_expr(
            f(expressions.construct_expr(VariableReference(new_id), input_type, self._indices, self._aggregations,
                                         self._joins)))

        indices, aggregations, joins = unify_all(self, lambda_result)
        ast = LambdaClassMethod(name, new_id, self._ast, lambda_result._ast, *(a._ast for a in args))
        return expressions.construct_expr(ast, ret_type_f(lambda_result._type), indices, aggregations, joins)

    @property
    def dtype(self):
        """The data type of the expression.

        Returns
        -------
        :class:`.HailType`
            Data type.
        """
        return self._type

    def __len__(self):
        raise TypeError("'Expression' objects have no static length: use 'hl.len' for the length of collections")

    def __hash__(self):
        return super(Expression, self).__hash__()

    def __repr__(self):
        return f'<{self.__class__.__name__} of type {self.dtype}>'

    def __eq__(self, other):
        """Returns ``True`` if the two expressions are equal.

        Examples
        --------

        .. doctest::

            >>> x = hl.literal(5)
            >>> y = hl.literal(5)
            >>> z = hl.literal(1)

            >>> hl.eval_expr(x == y)
            True

            >>> hl.eval_expr(x == z)
            False

        Notes
        -----
        This method will fail with an error if the two expressions are not
        of comparable types.

        Parameters
        ----------
        other : :class:`.Expression`
            Expression for equality comparison.

        Returns
        -------
        :class:`.BooleanExpression`
            ``True`` if the two expressions are equal.
        """
        other = to_expr(other)
        t = unify_types(self._type, other._type)
        if t is None:
            raise TypeError("Invalid '==' comparison, cannot compare expressions of type '{}' and '{}'".format(
                self._type, other._type
            ))
        return self._bin_op("==", other, tbool)

    def __ne__(self, other):
        """Returns ``True`` if the two expressions are not equal.

        Examples
        --------

        .. doctest::

            >>> x = hl.literal(5)
            >>> y = hl.literal(5)
            >>> z = hl.literal(1)

            >>> hl.eval_expr(x != y)
            False

            >>> hl.eval_expr(x != z)
            True

        Notes
        -----
        This method will fail with an error if the two expressions are not
        of comparable types.

        Parameters
        ----------
        other : :class:`.Expression`
            Expression for inequality comparison.

        Returns
        -------
        :class:`.BooleanExpression`
            ``True`` if the two expressions are not equal.
        """
        other = to_expr(other)
        t = unify_types(self._type, other._type)
        if t is None:
            raise TypeError("Invalid '!=' comparison, cannot compare expressions of type '{}' and '{}'".format(
                self._type, other._type
            ))
        return self._bin_op("!=", other, tbool)

    def _to_table(self, name):
        source = self._indices.source
        axes = self._indices.axes
        if not self._aggregations.empty():
            raise NotImplementedError('cannot convert aggregated expression to table')
        if source is None:
            # scalar expression
            df = Env.dummy_table()
            df = df.select(**{name: self})
            return df
        elif len(axes) == 0:
            uid = Env.get_uid()
            source = source.select_globals(**{uid: self})
            df = Env.dummy_table()
            df = df.select(**{name: source.index_globals()[uid]})
            return df
        elif len(axes) == 1:
            if isinstance(source, hail.Table):
                df = source
                df = df.select(*filter(lambda f: f != name, df.key), **{name: self})
                return df.select_globals()
            else:
                assert isinstance(source, hail.MatrixTable)
                if self._indices == source._row_indices:
                    field_name = source._fields_inverse.get(self)
                    if field_name is not None:
                        if field_name in source.row_key:
                            m = source.select_rows(*source.row_key)
                        else:
                            m = source.select_rows(*source.row_key, field_name)
                        m = m.rename({field_name: name})
                    else:
                        m = source.select_rows(*source.row_key, **{name: self})
                    return m.rows().select_globals()
                else:
                    field_name = source._fields_inverse.get(self)
                    if field_name is not None:
                        if field_name in source.col_key:
                            m = source.select_cols(*source.col_key)
                        else:
                            m = source.select_cols(*source.col_key, field_name)
                        m = m.rename({field_name: name})
                    else:
                        m = source.select_cols(*source.col_key, **{name: self})
                    return m.cols().select_globals()
        else:
            assert len(axes) == 2
            assert isinstance(source, hail.MatrixTable)
            source = source.select_entries(**{name: self})
            source = source.select_rows(*source.row_key)
            source = source.select_cols(*source.col_key)
            return source.entries().select_globals()

    @typecheck_method(n=int, width=int, truncate=nullable(int), types=bool)
    def show(self, n=10, width=90, truncate=None, types=True):
        """Print the first few rows of the table to the console.

        Examples
        --------
        .. doctest::

            >>> table1.SEX.show()
            +-------+-----+
            |    ID | SEX |
            +-------+-----+
            | int32 | str |
            +-------+-----+
            |     1 | M   |
            |     2 | M   |
            |     3 | F   |
            |     4 | F   |
            +-------+-----+

            >>> hl.literal(123).show()
            +--------+
            | <expr> |
            +--------+
            |  int32 |
            +--------+
            |    123 |
            +--------+

        Warning
        -------
        Extremely experimental.

        Parameters
        ----------
        n : :obj:`int`
            Maximum number of rows to show.
        width : :obj:`int`
            Horizontal width at which to break columns.
        truncate : :obj:`int`, optional
            Truncate each field to the given number of characters. If
            ``None``, truncate fields to the given `width`.
        types : :obj:`bool`
            Print an extra header line with the type of each field.
        """
        name = '<expr>'
        source = self._indices.source
        if source is not None:
            name = source._fields_inverse.get(self, name)
        t = self._to_table(name)
        if name in t.key:
            t = t.select(name)
        t.show(n, width, truncate, types)

    @typecheck_method(n=int)
    def take(self, n):
        """Collect the first `n` records of an expression.

        Examples
        --------
        Take the first three rows:

        .. doctest::

            >>> first3 = table1.X.take(3)
            [5, 6, 7]

        Warning
        -------
        Extremely experimental.

        Parameters
        ----------
        n : int
            Number of records to take.

        Returns
        -------
        :obj:`list`
        """
        uid = Env.get_uid()
        return [r[uid] for r in self._to_table(uid).take(n)]

    def collect(self):
        """Collect all records of an expression into a local list.

        Examples
        --------
        Take the first three rows:

        .. doctest::

            >>> first3 = table1.C1.collect()
            [2, 2, 10, 11]

        Warning
        -------
        Extremely experimental.

        Warning
        -------
        The list of records may be very large.

        Returns
        -------
        :obj:`list`
        """
        uid = Env.get_uid()
        t = self._to_table(uid)
        return [r[uid] for r in t.select(uid).collect()]

    @property
    def value(self):
        """Evaluate this expression.

        Notes
        -----
        This expression must have no indices, but can refer to the
        globals of a :class:`.hail.Table` or
        :class:`.hail.MatrixTable`.

        Returns
        -------
            The value of this expression.

        """
        return hl.eval_expr(self)


class Aggregable(object):
    """Expression that can only be aggregated.

    An :class:`.Aggregable` is produced by the :meth:`.explode` or :meth:`.filter`
    methods. These objects can be aggregated using aggregator functions, but
    cannot otherwise be used in expressions.
    """

    def __init__(self, ast, type, indices, aggregations, joins):
        self._ast = ast
        self._type = type
        self._indices = indices
        self._aggregations = aggregations
        self._joins = joins

    def __nonzero__(self):
        raise NotImplementedError('Truth value of an aggregable collection is undefined')

    def __eq__(self, other):
        raise NotImplementedError('Comparison of aggregable collections is undefined')

    def __ne__(self, other):
        raise NotImplementedError('Comparison of aggregable collections is undefined')

    def _map(self, f):
        uid = Env.get_uid()
        ref = expressions.construct_expr(
            VariableReference(uid), self._type, self._indices, self._aggregations, self._joins)
        mapped = f(ref)
        indices, aggregations, joins = unify_all(ref, mapped)
        return expressions.Aggregable(
            LambdaClassMethod('map', uid, self._ast, mapped._ast), mapped.dtype,
            indices, aggregations, joins)

    @property
    def dtype(self) -> HailType:
        return self._type


FieldRef = Union[str, Expression]
FieldRefArgs = Tuple[FieldRef]
NamedExprs = Dict[str, Expression]
