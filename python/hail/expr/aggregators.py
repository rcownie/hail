import hail as hl
from hail.typecheck import *
from hail.expr.expressions import *
from hail.expr.expr_ast import *
from hail.expr.types import *


def _to_agg(x):
    if isinstance(x, Aggregable):
        return x
    else:
        x = to_expr(x)
        uid = Env.get_uid()
        ast = LambdaClassMethod('map', uid, AggregableReference(), x._ast)
        return Aggregable(ast, x._type, x._indices, x._aggregations, x._joins)


def _agg_func(name, aggregable, ret_type, *args):
    args = [to_expr(a) for a in args]
    indices, aggregations, joins = unify_all(aggregable, *args)
    if aggregations:
        raise ExpressionException('Cannot aggregate an already-aggregated expression')

    for a in args:
        _check_agg_bindings(a)
    _check_agg_bindings(aggregable)

    ast = ClassMethod(name, aggregable._ast, *[a._ast for a in args])
    return construct_expr(ast, ret_type, Indices(source=indices.source),
                          aggregations.push(Aggregation(aggregable, *args)), joins)

def _check_agg_bindings(expr):
    bound_references = {ast.name for ast in expr._ast.search(lambda ast: isinstance(ast, VariableReference))}
    bind_vars = {ast.uid for ast in expr._ast.search(lambda ast: isinstance(ast, Bind))}
    lambda_vars = {ast.lambda_var for ast in expr._ast.search(lambda ast: isinstance(ast, LambdaClassMethod))}
    declared_temp_vars = bind_vars.union(lambda_vars)

    free_variables = bound_references - declared_temp_vars
    if free_variables:
        raise ExpressionException("dynamic variables created by 'hl.bind' or lambda methods like 'hl.map' may not be aggregated")

@typecheck(expr=oneof(Aggregable, expr_any))
def collect(expr):
    """Collect records into an array.

    Examples
    --------
    Collect the `ID` field where `HT` is greater than 68:

    .. doctest::

        >>> table1.aggregate(agg.collect(agg.filter(table1.HT > 68, table1.ID)))
        [2, 3]

    Notes
    -----
    The element order of the resulting array is not guaranteed, and in some
    cases is non-deterministic.

    Use :meth:`collect_as_set` to collect unique items.

    Warning
    -------
    Collecting a large number of items can cause out-of-memory exceptions.

    Parameters
    ----------
    expr : :class:`.Expression`
        Expression to collect.

    Returns
    -------
    :class:`.ArrayExpression`
        Array of all `expr` records.
    """
    agg = _to_agg(expr)
    return _agg_func('collect', agg, tarray(agg._type))

@typecheck(expr=oneof(Aggregable, expr_any))
def collect_as_set(expr):
    """Collect records into a set.

    Examples
    --------
    Collect the unique `ID` field where `HT` is greater than 68:

    .. doctest::

        >>> table1.aggregate(agg.collect_as_set(agg.filter(table1.HT > 68, table1.ID)))
        set([2, 3]

    Warning
    -------
    Collecting a large number of items can cause out-of-memory exceptions.

    Parameters
    ----------
    expr : :class:`.Expression`
        Expression to collect.

    Returns
    -------
    :class:`.SetExpression`
        Set of unique `expr` records.
    """

    agg = _to_agg(expr)
    return _agg_func('collectAsSet', agg, tarray(agg._type))

@typecheck(expr=nullable(oneof(Aggregable, expr_any)))
def count(expr=None):
    """Count the number of records.

    Examples
    --------
    Group by the `SEX` field and count the number of rows in each category:

    .. doctest::

        >>> (table1.group_by(table1.SEX)
        ...        .aggregate(n=agg.count())
        ...        .show())
        +-----+-------+
        | SEX |     n |
        +-----+-------+
        | str | int64 |
        +-----+-------+
        | M   |     2 |
        | F   |     2 |
        +-----+-------+

    Notes
    -----
    If `expr` is not provided, then this method will count the number of
    records aggregated. If `expr` is provided, then the result should
    make use of :meth:`filter` or :meth:`explode` so that the number of
    records aggregated changes.

    Parameters
    ----------
    expr : :class:`.Expression`, or :obj:`None`
        Expression to count.

    Returns
    -------
    :class:`.Expression` of type :py:data:`.tint64`
        Total number of records.
    """
    if expr is not None:
        return _agg_func('count', _to_agg(expr), tint64)
    else:
        return _agg_func('count', _to_agg(0), tint64)

@typecheck(condition=oneof(Aggregable, expr_bool))
def count_where(condition):
    """Count the number of records where a predicate is ``True``.

    Examples
    --------

    Count the number of individuals with `HT` greater than 68:

    .. doctest::

        >>> table1.aggregate(agg.count_where(table1.HT > 68))
        2

    Parameters
    ----------
    condition : :class:`.BooleanExpression`
        Criteria for inclusion.

    Returns
    -------
    :class:`.Expression` of type :py:data:`.tint64`
        Total number of records where `condition` is ``True``.
    """

    return _agg_func('count', filter(condition, 0), tint64)

@typecheck(condition=oneof(Aggregable, expr_bool))
def any(condition):
    """Returns ``True`` if `condition` is ``True`` for any record.

    Examples
    --------
    .. doctest::

        >>> (table1.group_by(table1.SEX)
        ... .aggregate(any_over_70 = agg.any(table1.HT > 70))
        ... .show())
        +-----+-------------+
        | SEX | any_over_70 |
        +-----+-------------+
        | str | bool        |
        +-----+-------------+
        | M   | true        |
        | F   | false       |
        +-----+-------------+

    Notes
    -----
    If there are no records to aggregate, the result is ``False``.

    Missing records are not considered. If every record is missing,
    the result is also ``False``.

    Parameters
    ----------
    condition : :class:`.BooleanExpression`
        Condition to test.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    return count(filter(lambda x: x, condition)) > 0

@typecheck(condition=oneof(Aggregable, expr_bool))
def all(condition):
    """Returns ``True`` if `condition` is ``True`` for every record.

    Examples
    --------
    .. doctest::

        >>> (table1.group_by(table1.SEX)
        ... .aggregate(all_under_70 = agg.all(table1.HT < 70))
        ... .show())
        +-----+--------------+
        | SEX | all_under_70 |
        +-----+--------------+
        | str | bool         |
        +-----+--------------+
        | M   | false        |
        | F   | false        |
        +-----+--------------+

    Notes
    -----
    If there are no records to aggregate, the result is ``True``.

    Missing records are not considered. If every record is missing,
    the result is also ``True``.

    Parameters
    ----------
    condition : :class:`.BooleanExpression`
        Condition to test.

    Returns
    -------
    :class:`.BooleanExpression`
    """
    n_defined = count(filter(lambda x: hl.is_defined(x), condition))
    n_true = count(filter(lambda x: hl.is_defined(x) & x, condition))
    return n_defined == n_true

@typecheck(expr=oneof(Aggregable, expr_any))
def counter(expr):
    """Count the occurrences of each unique record and return a dictionary.

    Examples
    --------
    Count the number of individuals for each unique `SEX` value:

    .. doctest::

        >>> table1.aggregate(agg.counter(table1.SEX))
        {'M': 2L, 'F': 2L}

    Notes
    -----
    This aggregator method returns a dict expression whose key type is the
    same type as `expr` and whose value type is :class:`.Expression` of type :py:data:`.tint64`.
    This dict contains a key for each unique value of `expr`, and the value
    is the number of times that key was observed.

    Ensure that the result can be stored in memory on a single machine.

    Warning
    -------
    Using :meth:`counter` with a large number of unique items can cause
    out-of-memory exceptions.

    Parameters
    ----------
    expr : :class:`.Expression`
        Expression to count by key.

    Returns
    -------
    :class:`.DictExpression`
        Dictionary with the number of occurrences of each unique record.
    """
    agg = _to_agg(expr)
    return _agg_func('counter', agg, tdict(agg._type, tint64))

@typecheck(expr=oneof(Aggregable, expr_any), n=int, ordering=nullable(oneof(expr_any, func_spec(1, expr_any))))
def take(expr, n, ordering=None):
    """Take `n` records of `expr`, optionally ordered by `ordering`.

    Examples
    --------
    Take 3 elements of field `X`:

    .. doctest::

        >>> table1.aggregate(agg.take(table1.X, 3))
        [5, 6, 7]

    Take the `ID` and `HT` fields, ordered by `HT` (descending):

    .. doctest::

        >>> table1.aggregate(agg.take(hl.struct(ID=table1.ID, HT=table1.HT),
        ...                           3,
        ...                           ordering=-table1.HT))
        [Struct(ID=2, HT=72), Struct(ID=3, HT=70), Struct(ID=1, HT=65)]

    Notes
    -----
    The resulting array can include fewer than `n` elements if there are fewer
    than `n` total records.

    The `ordering` argument may be an expression, a function, or ``None``.

    If `ordering` is an expression, this expression's type should be one with
    a natural ordering (e.g. numeric).

    If `ordering` is a function, it will be evaluated on each record of `expr`
    to compute the value used for ordering. In the above example,
    ``ordering=-table1.HT`` and ``ordering=lambda x: -x.HT`` would be
    equivalent.

    If `ordering` is ``None``, then there is no guaranteed ordering on the
    elements taken, and and the results may be non-deterministic.

    Missing values are always sorted **last**.

    Parameters
    ----------
    expr : :class:`.Expression`
        Expression to store.
    n : :class:`.Expression` of type :py:data:`.tint32`
        Number of records to take.
    ordering : :class:`.Expression` or function ((arg) -> :class:`.Expression`) or None
        Optional ordering on records.

    Returns
    -------
    :class:`.ArrayExpression`
        Array of up to `n` records of `expr`.

    """
    agg = _to_agg(expr)
    n = to_expr(n)
    if ordering is None:
        return _agg_func('take', agg, tarray(agg.dtype), n)
    else:
        uid = Env.get_uid()
        if callable(ordering):
            lambda_result = to_expr(
                ordering(construct_expr(VariableReference(uid), agg._type, agg._indices,
                                        agg._aggregations, agg._joins)))

        else:
            lambda_result = ordering
        indices, aggregations, joins = unify_all(agg, lambda_result)

        if not (is_numeric(ordering.dtype) or ordering.dtype == tstr):
            raise TypeError("'take' expects 'ordering' to be or return an ordered expression\n"
                            "    Ordered expressions are 'int32', 'int64', 'float32', 'float64', 'str'\n"
                            "    Found '{}'".format(ordering._type))
        ast = LambdaClassMethod('takeBy', uid, agg._ast, lambda_result._ast, n._ast)

        if aggregations:
            raise ExpressionException('Cannot aggregate an already-aggregated expression')

        _check_agg_bindings(agg)
        _check_agg_bindings(lambda_result)

        return construct_expr(ast, tarray(agg._type), Indices(source=indices.source),
                              aggregations.push(Aggregation(agg, lambda_result)), joins)

@typecheck(expr=oneof(Aggregable, expr_numeric))
def min(expr):
    """Compute the minimum `expr`.

    Examples
    --------
    Compute the minimum value of `HT`:

    .. doctest::

        >>> table1.aggregate(agg.min(table1.HT))
        min_ht=60

    Notes
    -----
    This method returns the minimum non-missing value. If there are no
    non-missing values, then the result is missing.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Numeric expression.

    Returns
    -------
    :class:`.NumericExpression`
        Minimum value of all `expr` records, same type as `expr`.
    """
    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'min' expects a numeric argument, found '{}'".format(agg._type))
    return _agg_func('min', agg, agg._type)

@typecheck(expr=oneof(Aggregable, expr_numeric))
def max(expr):
    """Compute the maximum `expr`.

    Examples
    --------
    Compute the maximum value of `HT`:

    .. doctest::

        >>> table1.aggregate(agg.max(table1.HT))
        max_ht=72

    Notes
    -----
    This method returns the maximum non-missing value. If there are no
    non-missing values, then the result is missing.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Numeric expression.

    Returns
    -------
    :class:`.NumericExpression`
        Maximum value of all `expr` records, same type as `expr`.
    """
    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'max' expects a numeric argument, found '{}'".format(agg._type))
    return _agg_func('max', agg, agg._type)

@typecheck(expr=oneof(Aggregable, expr_numeric))
def sum(expr):
    """Compute the sum of all records of `expr`.

    Examples
    --------
    Compute the sum of field `C1`:

    .. doctest::

        >>> table1.aggregate(agg.sum(table1.C1))
        25

    Notes
    -----
    Missing values are ignored (treated as zero).

    If `expr` is an expression of type :py:data:`.tint32` or :py:data:`.tint64`, then
    the result is an expression of type :py:data:`.tint64`. If `expr` is an
    expression of type :py:data:`.tfloat32` or :py:data:`.tfloat64`, then the result
    is an expression of type :py:data:`.tfloat64`.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Numeric expression.

    Returns
    -------
    :class:`.Expression` of type :py:data:`.tint64` or :py:data:`.tfloat64`
        Sum of records of `expr`.
    """
    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'sum' expects a numeric argument, found '{}'".format(agg._type))
    return _agg_func('sum', agg, agg._type)

@typecheck(expr=oneof(Aggregable, expr_any))
def array_sum(expr):
    """Compute the coordinate-wise sum of all records of `expr`.

    Examples
    --------
    Compute the sum of `C1` and `C2`:

    .. doctest::

        >>> table1.aggregate(agg.array_sum([table1.C1, table1.C2]))
        [25, 46]

    Notes
    ------
    All records must have the same length. Each coordinate is summed
    independently as described in :func:`sum`.

    Parameters
    ----------
    expr : :class:`.ArrayNumericExpression`

    Returns
    -------
    :class:`.ArrayNumericExpression`
    """
    agg = _to_agg(expr)
    if not (isinstance(agg._type, tarray) and is_numeric(agg._type.element_type)):
        raise TypeError("'array_sum' expects a numeric array argument, found '{}'".format(agg._type))
    return _agg_func('sum', agg, agg._type)

@typecheck(expr=oneof(Aggregable, expr_numeric))
def mean(expr):
    """Compute the mean value of records of `expr`.

    Examples
    --------
    Compute the mean of field `HT`:

    .. doctest::

        >>> table1.aggregate(agg.mean(table1.HT))
        66.75

    Notes
    -----
    Missing values are ignored.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Numeric expression.

    Returns
    -------
    :class:`.Expression` of type :py:data:`.tfloat64`
        Mean value of records of `expr`.
    """
    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'mean' expects a numeric argument, found '{}'".format(agg._type))
    return stats(agg).mean

@typecheck(expr=oneof(Aggregable, expr_numeric))
def stats(expr):
    """Compute a number of useful statistics about `expr`.

    Examples
    --------
    Compute statistics about field `HT`:

    .. doctest::

        >>> table1.aggregate(agg.stats(table1.HT))
        Struct(min=60.0, max=72.0, sum=267.0, stdev=4.65698400255, n=4, mean=66.75)

    Notes
    -----
    Computes a struct with the following fields:

    - `min` (:py:data:`.tfloat64`) - Minimum value.
    - `max` (:py:data:`.tfloat64`) - Maximum value.
    - `mean` (:py:data:`.tfloat64`) - Mean value,
    - `stdev` (:py:data:`.tfloat64`) - Standard deviation.
    - `n` (:py:data:`.tfloat64`) - Number of non-missing records.
    - `sum` (:py:data:`.tfloat64`) - Sum.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Numeric expression.

    Returns
    -------
    :class:`.StructExpression`
        Struct expression with fields `mean`, `stdev`, `min`, `max`,
        `n`, and `sum`.
    """
    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'stats' expects a numeric argument, found '{}'".format(agg._type))
    agg = Expression._promote_numeric(agg, tfloat64)
    return _agg_func('stats', agg, tstruct(mean=tfloat64,
                                           stdev=tfloat64,
                                           min=tfloat64,
                                           max=tfloat64,
                                           n=tint64,
                                           sum=tfloat64))

@typecheck(expr=oneof(Aggregable, expr_numeric))
def product(expr):
    """Compute the product of all records of `expr`.

    Examples
    --------
    Compute the product of field `C1`:

    .. doctest::

        >>> table1.aggregate(agg.product(table1.C1))
        440

    Notes
    -----
    Missing values are ignored (treated as one).

    If `expr` is an expression of type :py:data:`.tint32` or :py:data:`.tint64`, then
    the result is an expression of type :py:data:`.tint64`. If `expr` is an
    expression of type :py:data:`.tfloat32` or :py:data:`.tfloat64`, then the result
    is an expression of type :py:data:`.tfloat64`.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Numeric expression.

    Returns
    -------
    :class:`.Expression` of type :py:data:`.tint64` or :py:data:`.tfloat64`
        Product of records of `expr`.
    """

    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'product' expects a numeric argument, found '{}'".format(agg._type))
    agg = Expression._promote_numeric(agg, tfloat64)
    return _agg_func('product', agg, agg._type)

@typecheck(predicate=oneof(Aggregable, expr_bool))
def fraction(predicate):
    """Compute the fraction of records where `predicate` is ``True``.

    Examples
    --------
    Compute the fraction of rows where `SEX` is "F" and `HT` > 65:

    .. doctest::

        >>> table1.aggregate(agg.fraction((table1.SEX == 'F') & (table1.HT > 65)))
        0.25

    Notes
    -----
    Missing values for `predicate` are treated as ``False``.

    Parameters
    ----------
    predicate : :class:`.BooleanExpression`
        Boolean predicate.

    Returns
    -------
    :class:`.Expression` of type :py:data:`.tfloat64`
        Fraction of records where `predicate` is ``True``.
    """
    agg = _to_agg(predicate)
    if not agg.dtype == tbool:
        raise TypeError(
            "'fraction' aggregator expects an expression of type 'bool', found '{}'".format(agg.dtype))

    if agg._aggregations:
        raise ExpressionException('Cannot aggregate an already-aggregated expression')

    uid = Env.get_uid()
    ast = LambdaClassMethod('fraction', uid, agg._ast, VariableReference(uid))
    return construct_expr(ast, tfloat64, Indices(source=agg._indices.source),
                          agg._aggregations.push(Aggregation(agg)), agg._joins)

@typecheck(expr=oneof(Aggregable, expr_any))
def hardy_weinberg(expr):
    """Compute Hardy-Weinberg Equilbrium (HWE) p-value and heterozygosity ratio.

    Examples
    --------
    Compute HWE statistics per row of a dataset:

    .. doctest::

        >>> dataset_result = dataset.annotate_rows(hwe = agg.hardy_weinberg(dataset.GT))

    Compute HWE statistics for a single population:

    .. doctest::

        >>> dataset_result = dataset.annotate_rows(
        ...     hwe_eas = agg.hardy_weinberg(agg.filter(dataset.pop == 'EAS', dataset.GT)))

    Notes
    -----
    This method returns a struct expression with the following fields:

    - `r_expected_het_freq` (:py:data:`.tfloat64`) - Ratio of observed to
      expected heterozygote frequency.
    - `p_hwe` (:py:data:`.tfloat64`) - Hardy-Weinberg p-value.

    Hail computes the exact p-value with mid-p-value correction, i.e. the
    probability of a less-likely outcome plus one-half the probability of an
    equally-likely outcome. See this `document <LeveneHaldane.pdf>`__ for
    details on the Levene-Haldane distribution and references.

    Warning
    -------
    Non-diploid calls (``ploidy != 2``) are not included in statistics. It is
    assumed the row is biallelic. Use :func:`~hail.methods.split_multi` to split multiallelic
    variants before computing statistics.

    Parameters
    ----------
    expr : :class:`.CallExpression`
        Call for which to compute Hardy-Weinberg statistics.

    Returns
    -------
    :class:`.StructExpression`
        Struct expression with fields `r_expected_het_freq` and `p_hwe`.
    """
    t = tstruct(r_expected_het_freq=tfloat64,
                p_hwe=tfloat64)
    agg = _to_agg(expr)
    if not agg.dtype == tcall:
        raise TypeError("aggregator 'hardy_weinberg' requires an expression of type 'Call', found '{}'".format(
            agg._type.__class__))
    return _agg_func('hardyWeinberg', agg, t)


@typecheck(expr=oneof(Aggregable, expr_array(), expr_set()))
def explode(expr):
    """Explode an array or set expression to aggregate the elements of all records.

    Examples
    --------
    Compute the mean of all elements in fields `C1`, `C2`, and `C3`:

    .. doctest::

        >>> table1.aggregate(agg.mean(agg.explode([table1.C1, table1.C2, table1.C3])))
        24.8333333333

    Compute the set of all observed elements in the `filters` field (``Set[String]``):

    .. doctest::

        >>> dataset.aggregate_rows(agg.collect_as_set(agg.explode(dataset.filters)))
        set([u'VQSRTrancheSNP99.80to99.90',
             u'VQSRTrancheINDEL99.95to100.00',
             u'VQSRTrancheINDEL99.00to99.50',
             u'VQSRTrancheINDEL97.00to99.00',
             u'VQSRTrancheSNP99.95to100.00',
             u'VQSRTrancheSNP99.60to99.80',
             u'VQSRTrancheINDEL99.50to99.90',
             u'VQSRTrancheSNP99.90to99.95',
             u'VQSRTrancheINDEL96.00to97.00']))

    Notes
    -----
    This method can be used with aggregator functions to aggregate the elements
    of collection types (:class:`.tarray` and :class:`.tset`).

    The result of the :meth:`explode` and :meth:`filter` methods is an
    :class:`.Aggregable` expression which can be used only in aggregator
    methods.

    Parameters
    ----------
    expr : :class:`.CollectionExpression`
        Expression of type :class:`.tarray` or :class:`.tset`.

    Returns
    -------
    :class:`.Aggregable`
        Aggregable expression.
    """
    agg = _to_agg(expr)
    if not (isinstance(agg._type, tset) or isinstance(agg._type, tarray)):
        raise  TypeError("'explode' expects a 'Set' or 'Array' argument, found '{}'".format(agg._type))
    uid = Env.get_uid()
    return Aggregable(LambdaClassMethod('flatMap', uid, agg._ast, VariableReference(uid)),
                      agg._type.element_type, agg._indices, agg._aggregations, agg._joins)

@typecheck(condition=oneof(func_spec(1, expr_bool), expr_bool), expr=oneof(Aggregable, expr_any))
def filter(condition, expr):
    """Filter records according to a predicate.

    Examples
    --------
    Collect the `ID` field where `HT` >= 70:

    .. doctest::

        >>> table1.aggregate(agg.collect(agg.filter(table1.HT >= 70, table1.ID)))
        [2, 3]

    Notes
    -----
    This method can be used with aggregator functions to remove records from
    aggregation.

    The result of the :meth:`explode` and :meth:`filter` methods is an
    :class:`.Aggregable` expression which can be used only in aggregator
    methods.

    Parameters
    ----------
    condition : :class:`.BooleanExpression` or function ( (arg) -> :class:`.BooleanExpression`)
        Filter expression, or a function to evaluate for each record.
    expr : :class:`.Expression`
        Expression to filter.

    Returns
    -------
    :class:`.Aggregable`
        Aggregable expression.
    """

    agg = _to_agg(expr)
    uid = Env.get_uid()

    if callable(condition):
        lambda_result = to_expr(
            condition(
                construct_expr(VariableReference(uid), agg._type, agg._indices,
                               agg._aggregations, agg._joins)))
    else:
        lambda_result = to_expr(condition)

    if lambda_result.dtype != tbool:
        raise TypeError(
            "'filter' expects the 'condition' argument to be or return an expression of type 'bool', found '{}'".format(
                lambda_result.dtype))
    indices, aggregations, joins = unify_all(agg, lambda_result)
    ast = LambdaClassMethod('filter', uid, agg._ast, lambda_result._ast)
    return Aggregable(ast, agg.dtype, indices, aggregations, joins)


@typecheck(expr=oneof(Aggregable, expr_call), prior=expr_numeric)
def inbreeding(expr, prior):
    """Compute inbreeding statistics on calls.

    Examples
    --------
    Compute inbreeding statistics per column:

    .. doctest::

        >>> dataset_result = dataset.annotate_cols(IB = agg.inbreeding(dataset.GT, dataset.variant_qc.AF))
        >>> dataset_result.cols().show()
        +----------------+--------------+-------------+------------------+------------------+
        | s              |    IB.f_stat | IB.n_called | IB.expected_homs | IB.observed_homs |
        +----------------+--------------+-------------+------------------+------------------+
        | str            |      float64 |       int64 |          float64 |            int64 |
        +----------------+--------------+-------------+------------------+------------------+
        | C1046::HG02024 | -1.23867e-01 |         338 |      2.96180e+02 |              291 |
        | C1046::HG02025 |  2.02944e-02 |         339 |      2.97151e+02 |              298 |
        | C1046::HG02026 |  5.47269e-02 |         336 |      2.94742e+02 |              297 |
        | C1047::HG00731 | -1.89046e-02 |         337 |      2.95779e+02 |              295 |
        | C1047::HG00732 |  1.38718e-01 |         337 |      2.95202e+02 |              301 |
        | C1047::HG00733 |  3.50684e-01 |         338 |      2.96418e+02 |              311 |
        | C1048::HG02024 | -1.95603e-01 |         338 |      2.96180e+02 |              288 |
        | C1048::HG02025 |  2.02944e-02 |         339 |      2.97151e+02 |              298 |
        | C1048::HG02026 |  6.74296e-02 |         338 |      2.96180e+02 |              299 |
        | C1049::HG00731 | -1.00467e-02 |         337 |      2.95418e+02 |              295 |
        +----------------+--------------+-------------+------------------+------------------+

    Notes
    -----

    ``E`` is total number of expected homozygous calls, given by the sum of
    ``1 - 2.0 * prior * (1 - prior)`` across records.

    ``O`` is the observed number of homozygous calls across records.

    ``N`` is the number of non-missing calls.

    ``F`` is the inbreeding coefficient, and is computed by: ``(O - E) / (N - E)``.

    This method returns a struct expression with four fields:

     - `f_stat` (:py:data:`.tfloat64`): ``F``, the inbreeding coefficient.
     - `n_called` (:py:data:`.tint64`): ``N``, the number of non-missing calls.
     - `expected_homs` (:py:data:`.tfloat64`): ``E``, the expected number of homozygotes.
     - `observed_homs` (:py:data:`.tint64`): ``O``, the number of observed homozygotes.

    Parameters
    ----------
    expr : :class:`.CallExpression`
        Call expression.
    prior : :class:`.Expression` of type :py:data:`.tfloat64`
        Alternate allele frequency prior.

    Returns
    -------
    :class:`.StructExpression`
        Struct expression with fields `f_stat`, `n_called`, `expected_homs`, `observed_homs`.
    """
    agg = _to_agg(expr)

    if not agg.dtype == tcall:
        raise TypeError("aggregator 'inbreeding' requires an expression of type 'call', found '{}'".format(
            agg.dtype))

    if prior.dtype == tfloat32:
        prior = hl.float64(prior)
    if not prior.dtype == tfloat64:
        raise TypeError("'inbreeding' expects 'prior' to be type 'float32' or 'float64', found '{}'".format(prior._type))

    uid = Env.get_uid()
    ast = LambdaClassMethod('inbreeding', uid, agg._ast, prior._ast)

    indices, aggregations, joins = unify_all(agg, prior)
    if aggregations:
        raise ExpressionException('Cannot aggregate an already-aggregated expression')

    _check_agg_bindings(agg)
    _check_agg_bindings(prior)
    t = tstruct(f_stat=tfloat64,
                n_called=tint64,
                expected_homs=tfloat64,
                observed_homs=tint64)
    return construct_expr(ast, t, Indices(source=indices.source),
                          aggregations.push(Aggregation(agg, prior)), joins)


@typecheck(expr=oneof(Aggregable, expr_call), alleles=expr_array(expr_str))
def call_stats(expr, alleles):
    """Compute useful call statistics.

    Examples
    --------
    Compute call statistics per row:

    .. doctest::

        >>> dataset_result = dataset.annotate_rows(gt_stats = agg.call_stats(dataset.GT, dataset.alleles))
        >>> dataset_result.rows().select('locus', 'gt_stats').show()
        +---------------+--------------+----------------+-------------+--------------+
        | locus         | gt_stats.AC  | gt_stats.AF    | gt_stats.AN | gt_stats.GC  |
        +---------------+--------------+----------------+-------------+--------------+
        | locus<GRCh37> | array<int32> | array<float64> |       int32 | array<int32> |
        +---------------+--------------+----------------+-------------+--------------+
        | 20:10019093   | [111,89]     | [0.555,0.445]  |         200 | [36,39,25]   |
        | 20:10026348   | [198,2]      | [0.99,0.01]    |         200 | [98,2,0]     |
        | 20:10026357   | [166,34]     | [0.83,0.17]    |         200 | [68,30,2]    |
        | 20:10030188   | [166,34]     | [0.83,0.17]    |         200 | [68,30,2]    |
        | 20:10030452   | [170,30]     | [0.85,0.15]    |         200 | [71,28,1]    |
        | 20:10030508   | [199,1]      | [0.995,0.005]  |         200 | [99,1,0]     |
        | 20:10030573   | [198,2]      | [0.99,0.01]    |         200 | [98,2,0]     |
        | 20:10032413   | [166,34]     | [0.83,0.17]    |         200 | [68,30,2]    |
        | 20:10036107   | [187,13]     | [0.935,0.065]  |         200 | [87,13,0]    |
        | 20:10036141   | [192,8]      | [0.96,0.04]    |         200 | [92,8,0]     |
        +---------------+--------------+----------------+-------------+--------------+

    Notes
    -----
    This method is meaningful for computing call metrics per variant, but not
    especially meaningful for computing metrics per sample.

    This method returns a struct expression with three fields:

     - `AC` (:class:`.tarray` of :py:data:`.tint32`) - Allele counts. One element
       for each allele, including the reference.
     - `AF` (:class:`.tarray` of :py:data:`.tfloat64`) - Allele frequencies. One
       element for each allele, including the reference.
     - `AN` (:py:data:`.tint32`) - Allele number. The total number of called
       alleles, or the number of non-missing calls * 2.

    Parameters
    ----------
    expr : :class:`.CallExpression`
        Call.
    alleles : :class:`.ArrayStringExpression`
        Variant alleles.

    Returns
    -------
    :class:`.StructExpression`
        Struct expression with fields `AC`, `AF`, and `AN`
    """
    agg = _to_agg(expr)
    alleles = to_expr(alleles)

    uid = Env.get_uid()

    if agg.dtype != tcall:
        raise TypeError("aggregator 'call_stats' requires an expression of type 'TCall', found '{}'".format(
            agg.dtype))

    ast = LambdaClassMethod('callStats', uid, agg._ast, alleles._ast)
    indices, aggregations, joins = unify_all(agg, alleles)

    if aggregations:
        raise ExpressionException('Cannot aggregate an already-aggregated expression')

    _check_agg_bindings(agg)
    _check_agg_bindings(alleles)
    t = tstruct(AC=tarray(tint32),
                AF=tarray(tfloat64),
                AN=tint32)

    return construct_expr(ast, t, Indices(source=indices.source),
                          aggregations.push(Aggregation(agg, alleles)), joins)

@typecheck(expr=oneof(Aggregable, expr_numeric), start=expr_float64, end=expr_float64, bins=expr_int32)
def hist(expr, start, end, bins):
    """Compute binned counts of a numeric expression.

    Examples
    --------
    Compute a histogram of field `GQ`:

    .. doctest::

        >>> dataset.aggregate_entries(agg.hist(dataset.GQ, 0, 100, 10))
        Struct(bin_edges=[0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
               bin_freq=[2194L, 637L, 2450L, 1081L, 518L, 402L, 11168L, 1918L, 1379L, 11973L]),
               nLess=0,
               nGreater=0)

    Notes
    -----
    This method returns a struct expression with four fields:

     - `bin_edges` (:class:`.tarray` of :py:data:`.tfloat64`): Bin edges. Bin `i`
       contains values in the left-inclusive, right-exclusive range
       ``[ bin_edges[i], bin_edges[i+1] )``.
     - `bin_freq` (:class:`.tarray` of :py:data:`.tint64`): Bin
       frequencies. The number of records found in each bin.
     - `n_smaller` (:py:data:`.tint64`): The number of records smaller than the start
       of the first bin.
     - `n_larger` (:py:data:`.tint64`): The number of records larger than the end
       of the last bin.

    Parameters
    ----------
    expr : :class:`.NumericExpression`
        Target numeric expression.
    start : :obj:`int` or :obj:`float`
        Start of histogram range.
    end : :obj:`int` or :obj:`float`
        End of histogram range.
    bins : :obj:`int` or :obj:`float`
        Number of bins.

    Returns
    -------
    :class:`.StructExpression`
        Struct expression with fields `bin_edges`, `bin_freq`, `n_smaller`, and `n_larger`.
    """
    agg = _to_agg(expr)
    if not is_numeric(agg._type):
        raise TypeError("'hist' expects argument 'expr' to be a numeric type, found '{}'".format(agg._type))
    agg = Expression._promote_numeric(agg, tfloat64)
    t = tstruct(bin_edges=tarray(tfloat64),
                bin_freq=tarray(tint64),
                n_less=tint64,
                n_larger=tint64)
    return _agg_func('hist', agg, t, start, end, bins)
