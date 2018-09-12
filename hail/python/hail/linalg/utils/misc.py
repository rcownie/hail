import numpy as np

import hail as hl
from hail.typecheck import *
from hail.expr.expressions import expr_locus, expr_float64, check_row_indexed


@typecheck(a=np.ndarray,
           radius=oneof(int, float))
def array_windows(a, radius):
    """Returns start and stop indices for window around each array value.

    Examples
    --------

    >>> hl.linalg.utils.array_windows(np.array([1, 2, 4, 4, 6, 8]), 2)
    (array([0, 0, 1, 1, 2, 4]), array([2, 4, 5, 5, 6, 6]))

    >>> hl.linalg.utils.array_windows(np.array([-10.0, -2.5, 0.0, 0.0, 1.2, 2.3, 3.0]), 2.5)
    (array([0, 1, 1, 1, 2, 2, 4]), array([1, 4, 6, 6, 7, 7, 7]))

    Notes
    -----
    For an array ``a`` in ascending order, the resulting ``starts`` and ``stops``
    arrays have the same length as ``a`` and the property that, for all indices
    ``i``, ``[starts[i], stops[i])`` is the maximal range of indices ``j`` such
    that ``a[i] - radius <= a[j] <= a[i] + radius``.

    Index ranges are start-inclusive and stop-exclusive. This function is
    especially useful in conjunction with
    :meth:`.BlockMatrix.sparsify_row_intervals`.

    Parameters
    ----------
    a: :obj:`ndarray` of signed integer or float values
        1-dimensional array of values, non-decreasing with respect to index.
    radius: :obj:`float`
        Non-negative radius of window for values.

    Returns
    -------
    (:class:`ndarray` of :obj:`int64`, :class:`ndarray` of :obj:`int64`)
        Tuple of start indices array and stop indices array.
    """
    if radius < 0:
        raise ValueError(f'array_windows: radius must be non-negative, found {radius}')
    if a.ndim != 1:
        raise ValueError("array_windows: 'a' must be 1-dimensional")
    if not (np.issubdtype(a.dtype, np.signedinteger) or np.issubdtype(a.dtype, np.floating)):
        raise ValueError(f"array_windows: 'a' must be an ndarray of signed integer or float values, "
                         f"found dtype {str(a.dtype)}")

    size = a.size
    if size == 0:
        return np.zeros(shape=0, dtype=np.int64), np.zeros(shape=0, dtype=np.int64)

    if (not np.all(a[:-1] <= a[1:])) or np.isnan(a[0]):
        raise ValueError("array_windows: 'a' must be in ascending order with no nan elements")
    if a[0] - radius > a[0]:
        raise ValueError('array_windows: underflow for a[0] - radius')
    if a[-1] + radius < a[-1]:
        raise ValueError('array_windows: overflow for a[-1] + radius')

    starts, stops = np.zeros(size, dtype=np.int64),  np.zeros(size, dtype=np.int64)
    j, k = 0, 0
    for i in range(size):
        min_val = a[i] - radius
        while j < size and a[j] < min_val:
            j += 1
        starts[i] = j

        max_val = a[i] + radius
        while k < size and a[k] <= max_val:
            k += 1
        stops[i] = k

    return starts, stops


@typecheck(locus_expr=expr_locus(),
           radius=oneof(int, float),
           coord_expr=nullable(expr_float64))
def locus_windows(locus_expr, radius, coord_expr=None):
    """Returns start and stop indices for window around each locus.

    Examples
    --------

    Windows with 2bp radius for one contig with positions 1, 2, 3, 4, 5:

    >>> starts, stops = hl.linalg.utils.locus_windows(
    ...     hl.balding_nichols_model(1, 5, 5).locus,
    ...     radius=2)
    >>> starts, stops
    (array([0, 0, 0, 1, 2]), array([3, 4, 5, 5, 5]))

    The following examples involve three contigs.

    >>> loci = [{'locus': hl.Locus('1', 1), 'cm': 1.0},
    ...         {'locus': hl.Locus('1', 2), 'cm': 3.0},
    ...         {'locus': hl.Locus('1', 4), 'cm': 4.0},
    ...         {'locus': hl.Locus('2', 1), 'cm': 2.0},
    ...         {'locus': hl.Locus('2', 1), 'cm': 2.0},
    ...         {'locus': hl.Locus('3', 3), 'cm': 5.0}]

    >>> ht = hl.Table.parallelize(
    ...         loci,
    ...         hl.tstruct(locus=hl.tlocus('GRCh37'), cm=hl.tfloat64),
    ...         key=['locus'])

    Windows with 1bp radius:

    >>> hl.linalg.utils.locus_windows(ht.locus, 1)
    (array([0, 0, 2, 3, 3, 5]), array([2, 2, 3, 5, 5, 6]))

    Windows with 1cm radius:

    >>> hl.linalg.utils.locus_windows(ht.locus, 1.0, coord_expr=ht.cm)
    (array([0, 1, 1, 3, 3, 5]), array([1, 3, 3, 5, 5, 6]))

    Notes
    -----
    This function returns two 1-dimensional ndarrays of integers,
    ``starts`` and ``stops``, each of size equal to the number of rows.

    By default, for all indices ``i``, ``[starts[i], stops[i])`` is the maximal
    range of row indices ``j`` such that ``contig[i] == contig[j]`` and
    ``position[i] - radius <= position[j] <= position[i] + radius``.

    If the :meth:`.global_position` on `locus_expr` is not in ascending order,
    this method will fail. Ascending order should hold for a matrix table keyed
    by locus or variant (and the associated row table), or for a table that has
    been ordered by `locus_expr`.

    Set `coord_expr` to use a value other than position to define the windows.
    This row-indexed numeric expression must be non-missing, non-``nan``, on the
    same source as `locus_expr`, and ascending with respect to locus
    position for each contig; otherwise the function will fail.

    The last example above uses centimorgan coordinates, so
    ``[starts[i], stops[i])`` is the maximal range of row indices ``j`` such
    that ``contig[i] == contig[j]`` and
    ``cm[i] - radius <= cm[j] <= cm[i] + radius``.

    Index ranges are start-inclusive and stop-exclusive. This function is
    especially useful in conjunction with
    :meth:`.BlockMatrix.sparsify_row_intervals`.

    Parameters
    ----------
    locus_expr : :class:`.LocusExpression`
        Row-indexed locus expression on a table or matrix table.
    radius: :obj:`int`
        Radius of window for row values.
    coord_expr: :class:`.Float64Expression`, optional
        Row-indexed numeric expression for the row value.
        Must be on the same table or matrix table as `locus_expr`.
        By default, the row value is given by the locus position.

    Returns
    -------
    (:class:`ndarray` of :obj:`int64`, :class:`ndarray` of :obj:`int64`)
        Tuple of start indices array and stop indices array.
    """
    if radius < 0:
        raise ValueError(f"locus_windows: 'radius' must be non-negative, found {radius}")
    check_row_indexed('locus_windows', locus_expr)
    if coord_expr is None:
        global_pos_list = locus_expr.global_position().collect()
        n_loci = len(global_pos_list)
        global_pos = np.zeros(n_loci, dtype=np.int64)
        for i, p in enumerate(global_pos_list):
            if p is None:
                raise ValueError(f"locus_windows: missing value for 'locus_expr' global position at row {i}")
            global_pos[i] = p
        coord = global_pos
        del global_pos_list
    else:
        check_row_indexed('locus_windows', coord_expr)
        global_pos_and_coord =\
            hl.tuple([locus_expr.global_position(), coord_expr]).collect()  # raises exception if sources differ
        n_loci = len(global_pos_and_coord)

        global_pos = np.zeros(n_loci, dtype=np.int64)
        coord = np.zeros(n_loci, dtype=np.float64)
        for i, x in enumerate(global_pos_and_coord):
            if x[0] is None:
                raise ValueError(f"locus_windows: missing value for 'locus_expr' global position at row {i}")
            global_pos[i] = x[0]
            if x[1] is None:
                raise ValueError(f"locus_windows: missing value for 'coord_expr' at row {i}")
            coord[i] = x[1]
        del global_pos_and_coord

    if n_loci == 0:
        return np.zeros(shape=0, dtype=np.int64), np.zeros(shape=0, dtype=np.int64)

    contig_name = locus_expr.dtype.reference_genome.contigs
    contig_len = locus_expr.dtype.reference_genome.lengths
    contig_cum_len = np.cumsum([contig_len[name] for name in contig_name])

    assert(global_pos[-1] < contig_cum_len[-1])

    contig_start_idx = _compute_contig_start_idx(global_pos, contig_cum_len)
    n_contigs = len(contig_start_idx)
    contig_start_idx.append(n_loci)
    contig_bounds = [array_windows(coord[contig_start_idx[c]:contig_start_idx[c + 1]], radius)
                     for c in range(n_contigs)]
    starts = np.concatenate([contig_start_idx[c] + contig_bounds[c][0] for c in range(n_contigs)])
    stops = np.concatenate([contig_start_idx[c] + contig_bounds[c][1] for c in range(n_contigs)])

    return starts, stops


def _compute_contig_start_idx(global_pos, contig_cum_len):
    last = global_pos[0]
    contig_start_idx = [0]
    cum_len_iter = iter(contig_cum_len)
    cum_len = next(cum_len_iter)
    for i in range(len(global_pos)):
        curr = global_pos[i]
        if curr < last:
            raise ValueError("locus_windows: 'locus_expr' global position must be in ascending order")
        while curr >= cum_len:
            contig_start_idx.append(i)
            cum_len = next(cum_len_iter)
        last = curr
    return contig_start_idx


def _check_dims(a, name, ndim, min_size=1):
    if len(a.shape) != ndim:
        raise ValueError(f'{name} must be {ndim}-dimensional, '
                         f'found {a.ndim}')
    for i in range(ndim):
        if a.shape[i] < min_size:
            raise ValueError(f'{name}.shape[{i}] must be at least '
                             f'{min_size}, found {a.shape[i]}')
