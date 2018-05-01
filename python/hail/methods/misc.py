import hail as hl
from hail.expr.expr_ast import VariableReference
from hail.expr.expressions import *
from hail.expr.types import *
from hail.matrixtable import MatrixTable
from hail.table import Table
from hail.typecheck import *
from hail.utils import Interval, Struct
from hail.utils.java import Env, joption


@typecheck(i=Expression,
           j=Expression,
           keep=bool,
           tie_breaker=nullable(func_spec(2, expr_numeric)))
def maximal_independent_set(i, j, keep=True, tie_breaker=None) -> Table:
    """Return a table containing the vertices in a near
    `maximal independent set <https://en.wikipedia.org/wiki/Maximal_independent_set>`_
    of an undirected graph whose edges are given by a two-column table.

    Examples
    --------

    Prune individuals from a dataset until no close relationships remain with
    respect to a PC-Relate measure of kinship.

    >>> pc_rel = hl.pc_relate(dataset.GT, 0.001, k=2, statistics='kin')
    >>> pairs = pc_rel.filter(pc_rel['kin'] > 0.125)
    >>> pairs = pairs.select(i=pairs.i.s, j=pairs.j.s)
    >>> related_samples_to_remove = hl.maximal_independent_set(pairs.i, pairs.j, False)
    >>> result = dataset.filter_cols(hl.is_defined(related_samples_to_remove[dataset.s]), keep=False)

    Prune individuals from a dataset, preferring to keep cases over controls.

    >>> pc_rel = hl.pc_relate(dataset.GT, 0.001, k=2, statistics='kin')
    >>> pairs = pc_rel.filter(pc_rel['kin'] > 0.125)
    >>> pairs = pairs.select(i=pairs.i.s, j=pairs.j.s)
    >>> samples = dataset.cols()
    >>> pairs_with_case = pairs.select(
    ...     i=hl.struct(id=pairs.i, is_case=samples[pairs.i].is_case),
    ...     j=hl.struct(id=pairs.j, is_case=samples[pairs.j].is_case))
    >>> def tie_breaker(l, r):
    ...     return hl.cond(l.is_case & ~r.is_case, -1,
    ...                    hl.cond(~l.is_case & r.is_case, 1, 0))
    >>> related_samples_to_remove = hl.maximal_independent_set(
    ...    pairs_with_case.i, pairs_with_case.j, False, tie_breaker)
    >>> result = dataset.filter_cols(hl.is_defined(
    ...     related_samples_to_remove.select(
    ...        s = related_samples_to_remove.node.id).key_by('s')[dataset.s]), keep=False)

    Notes
    -----

    The vertex set of the graph is implicitly all the values realized by `i`
    and `j` on the rows of this table. Each row of the table corresponds to an
    undirected edge between the vertices given by evaluating `i` and `j` on
    that row. An undirected edge may appear multiple times in the table and
    will not affect the output. Vertices with self-edges are removed as they
    are not independent of themselves.

    The expressions for `i` and `j` must have the same type.

    The value of `keep` determines whether the vertices returned are those
    in the maximal independent set, or those in the complement of this set.
    This is useful if you need to filter a table without removing vertices that
    don't appear in the graph at all.

    This method implements a greedy algorithm which iteratively removes a
    vertex of highest degree until the graph contains no edges. The greedy
    algorithm always returns an independent set, but the set may not always
    be perfectly maximal.

    `tie_breaker` is a Python function taking two arguments---say `l` and
    `r`---each of which is an :class:`Expression` of the same type as `i` and
    `j`. `tie_breaker` returns a :class:`NumericExpression`, which defines an
    ordering on nodes. A pair of nodes can be ordered in one of three ways, and
    `tie_breaker` must encode the relationship as follows:

     - if ``l < r`` then ``tie_breaker`` evaluates to some negative integer
     - if ``l == r`` then ``tie_breaker`` evaluates to 0
     - if ``l > r`` then ``tie_breaker`` evaluates to some positive integer

    For example, the usual ordering on the integers is defined by: ``l - r``.

    When multiple nodes have the same degree, this algorithm will order the
    nodes according to ``tie_breaker`` and remove the *largest* node.

    Parameters
    ----------
    i : :class:`.Expression`
        Expression to compute one endpoint of an edge.
    j : :class:`.Expression`
        Expression to compute another endpoint of an edge.
    keep : :obj:`bool`
        If ``True``, return vertices in set. If ``False``, return vertices removed.
    tie_breaker : function
        Function used to order nodes with equal degree.

    Returns
    -------
    :class:`.Table`
    """
    if i.dtype != j.dtype:
        raise ValueError("'maximal_independent_set' expects arguments `i` and `j` to have same type. "
                         "Found {} and {}.".format(i.dtype, j.dtype))
    source = i._indices.source
    if not isinstance(source, Table):
        raise ValueError("'maximal_independent_set' expects an expression of 'Table'. Found {}".format(
            "expression of '{}'".format(
                source.__class__) if source is not None else 'scalar expression'))
    if i._indices.source != j._indices.source:
        raise ValueError(
            "'maximal_independent_set' expects arguments `i` and `j` to be expressions of the same Table. "
            "Found\n{}\n{}".format(i, j))

    node_t = i.dtype
    l = construct_expr(VariableReference('l'), node_t)
    r = construct_expr(VariableReference('r'), node_t)
    if tie_breaker:
        tie_breaker_expr = hl.int64(tie_breaker(l, r))
        edges, _ = source._process_joins(i, j, tie_breaker_expr)
        tie_breaker_hql = tie_breaker_expr._ast.to_hql()
    else:
        edges, _ = source._process_joins(i, j)
        tie_breaker_hql = None
    return Table(edges._jt.maximalIndependentSet(
        i._ast.to_hql(), j._ast.to_hql(), keep, joption(tie_breaker_hql)))


def require_col_key_str(dataset: MatrixTable, method: str):
    if not len(dataset.col_key) == 1 or dataset[next(iter(dataset.col_key))].dtype != hl.tstr:
        raise ValueError(f"Method '{method}' requires column key to be one field of type 'str', found "
                         f"{list(str(x.dtype) for x in dataset.col_key.values())}")


def require_row_key_variant(dataset, method):
    if (list(dataset.row_key) != ['locus', 'alleles'] or
            not isinstance(dataset['locus'].dtype, tlocus) or
            not dataset['alleles'].dtype == tarray(tstr)):
        raise ValueError("Method '{}' requires row key to be two fields 'locus' (type 'locus<any>') and "
                         "'alleles' (type 'array<str>')\n"
                         "  Found:{}".format(method, ''.join(
            "\n    '{}': {}".format(k, str(dataset[k].dtype)) for k in dataset.row_key)))

def require_row_key_variant_w_struct_locus(dataset, method):
    if (list(dataset.row_key) != ['locus', 'alleles'] or
            not dataset['alleles'].dtype == tarray(tstr) or
            (not isinstance(dataset['locus'].dtype, tlocus) and
                     dataset['locus'].dtype != hl.dtype('struct{contig: str, position: int32}'))):
        raise ValueError("Method '{}' requires row key to be two fields 'locus'"
                         " (type 'locus<any>' or 'struct{contig: str, position: int32}') and "
                         "'alleles' (type 'array<str>')\n"
                         "  Found:{}".format(method, ''.join(
            "\n    '{}': {}".format(k, str(dataset[k].dtype)) for k in dataset.row_key)))


def require_partition_key_locus(dataset, method):
    if (len(dataset.partition_key) != 1 or
            not isinstance(dataset.partition_key[0].dtype, tlocus)):
        raise ValueError("Method '{}' requires partition key to be one field of type 'locus<any>'.\n"
                         "  Found:{}".format(method, ''.join(
            "\n    '{}': {}".format(k, str(dataset[k].dtype)) for k in dataset.partition_key)))


@typecheck(dataset=MatrixTable, method=str)
def require_biallelic(dataset, method) -> MatrixTable:
    require_row_key_variant(dataset, method)
    dataset = MatrixTable(Env.hail().methods.VerifyBiallelic.apply(dataset._jvds, method))
    return dataset


@typecheck(dataset=MatrixTable, name=str)
def rename_duplicates(dataset, name='unique_id') -> MatrixTable:
    """Rename duplicate column keys.

    .. include:: ../_templates/req_tstring.rst

    Examples
    --------

    >>> renamed = hl.rename_duplicates(dataset).cols()
    >>> duplicate_samples = (renamed.filter(renamed.s != renamed.unique_id)
    ...                             .select('s')
    ...                             .collect())

    Notes
    -----

    This method produces a new column field from the string column key by
    appending a unique suffix ``_N`` as necessary. For example, if the column
    key "NA12878" appears three times in the dataset, the first will produce
    "NA12878", the second will produce "NA12878_1", and the third will produce
    "NA12878_2". The name of this new field is parameterized by `name`.

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset.
    name : :obj:`str`
        Name of new field.

    Returns
    -------
    :class:`.MatrixTable`
    """

    return MatrixTable(dataset._jvds.renameDuplicates(name))


@typecheck(ds=MatrixTable,
           intervals=expr_array(expr_interval(expr_any)),
           keep=bool)
def filter_intervals(ds, intervals, keep=True) -> MatrixTable:
    """Filter rows with a list of intervals.

    Examples
    --------

    Filter to loci falling within one interval:

    >>> ds_result = hl.filter_intervals(dataset, [hl.parse_locus_interval('17:38449840-38530994')])

    Remove all loci within list of intervals:

    >>> intervals = [hl.parse_locus_interval(x) for x in ['1:50M-75M', '2:START-400000', '3-22']]
    >>> ds_result = hl.filter_intervals(dataset, intervals)

    Notes
    -----
    Based on the ``keep`` argument, this method will either restrict to points
    in the supplied interval ranges, or remove all rows in those ranges.

    When ``keep=True``, partitions that don't overlap any supplied interval
    will not be loaded at all.  This enables :func:`.filter_intervals` to be
    used for reasonably low-latency queries of small ranges of the dataset, even
    on large datasets.

    Parameters
    ----------
    ds : :class:`.MatrixTable`
        Dataset.
    intervals : :class:`.ArrayExpression` of type :py:data:`.tinterval`
        Intervals to filter on. If there is only one row partition key, the
        point type of the interval can be the type of the first partition key.
        Otherwise, the interval point type must be a :class:`.Struct` matching
        the row partition key schema.
    keep : :obj:`bool`
        If ``True``, keep only rows that fall within any interval in `intervals`.
        If ``False``, keep only rows that fall outside all intervals in
        `intervals`.

    Returns
    -------
    :class:`.MatrixTable`
    """

    n_pk = len(ds.partition_key)
    pk_type = ds.partition_key.dtype
    point_type = intervals.dtype.element_type.point_type

    if point_type == pk_type:
        needs_wrapper = False
    elif n_pk == 1 and point_type == ds.partition_key[0].dtype:
        needs_wrapper = True
    else:
        raise TypeError("The point type does not match the row partition key type of the dataset ('{}', '{}')".format(repr(point_type), repr(pk_type)))

    def wrap_input(interval):
        if interval is None:
            raise TypeError("'filter_intervals' does not allow missing values in 'intervals'.")
        elif needs_wrapper:
            return Interval(Struct(foo=interval.start),
                            Struct(foo=interval.end),
                            interval.includes_start,
                            interval.includes_end)
        else:
            return interval

    intervals = [wrap_input(x)._jrep for x in intervals.value]
    jmt = Env.hail().methods.FilterIntervals.apply(ds._jvds, intervals, keep)
    return MatrixTable(jmt)
