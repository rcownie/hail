import itertools

import hail as hl
import hail.expr.aggregators as agg
from hail.expr.expressions import *
from hail.expr.types import *
from hail.genetics import KinshipMatrix
from hail.genetics.reference_genome import reference_genome_type
from hail.linalg import BlockMatrix
from hail.matrixtable import MatrixTable
from hail.methods.misc import require_biallelic, require_col_key_str
from hail.stats import UniformDist, BetaDist, TruncatedBetaDist
from hail.table import Table
from hail.typecheck import *
from hail.utils import wrap_to_list
from hail.utils.java import *
from hail.utils.misc import check_collisions


@typecheck(dataset=MatrixTable,
           maf=nullable(expr_float64),
           bounded=bool,
           min=nullable(numeric),
           max=nullable(numeric))
def identity_by_descent(dataset, maf=None, bounded=True, min=None, max=None):
    """Compute matrix of identity-by-descent estimates.

    .. include:: ../_templates/req_tvariant.rst

    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------

    To calculate a full IBD matrix, using minor allele frequencies computed
    from the dataset itself:

    >>> hl.identity_by_descent(dataset)

    To calculate an IBD matrix containing only pairs of samples with
    ``PI_HAT`` in :math:`[0.2, 0.9]`, using minor allele frequencies stored in
    the row field `panel_maf`:

    >>> hl.identity_by_descent(dataset, maf=dataset['panel_maf'], min=0.2, max=0.9)

    Notes
    -----

    The implementation is based on the IBD algorithm described in the `PLINK
    paper <http://www.ncbi.nlm.nih.gov/pmc/articles/PMC1950838>`__.

    :func:`.identity_by_descent` requires the dataset to be biallelic and does
    not perform LD pruning. Linkage disequilibrium may bias the result so
    consider filtering variants first.

    The resulting :class:`.Table` entries have the type: *{ i: String,
    j: String, ibd: { Z0: Double, Z1: Double, Z2: Double, PI_HAT: Double },
    ibs0: Long, ibs1: Long, ibs2: Long }*. The key list is: `*i: String, j:
    String*`.

    Conceptually, the output is a symmetric, sample-by-sample matrix. The
    output table has the following form

    .. code-block:: text

        i		j	ibd.Z0	ibd.Z1	ibd.Z2	ibd.PI_HAT ibs0	ibs1	ibs2
        sample1	sample2	1.0000	0.0000	0.0000	0.0000 ...
        sample1	sample3	1.0000	0.0000	0.0000	0.0000 ...
        sample1	sample4	0.6807	0.0000	0.3193	0.3193 ...
        sample1	sample5	0.1966	0.0000	0.8034	0.8034 ...

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Variant-keyed :class:`.MatrixTable` containing genotype information.
    maf : :class:`.Float64Expression`, optional
        Row-indexed expression for the minor allele frequency.
    bounded : :obj:`bool`
        Forces the estimations for `Z0``, ``Z1``, ``Z2``, and ``PI_HAT`` to take
        on biologically meaningful values (in the range [0,1]).
    min : :obj:`float` or :obj:`None`
        Sample pairs with a ``PI_HAT`` below this value will
        not be included in the output. Must be in :math:`[0,1]`.
    max : :obj:`float` or :obj:`None`
        Sample pairs with a ``PI_HAT`` above this value will
        not be included in the output. Must be in :math:`[0,1]`.

    Returns
    -------
    :class:`.Table`
    """

    if maf is not None:
        analyze('identity_by_descent/maf', maf, dataset._row_indices)
        dataset, _ = dataset._process_joins(maf)
        maf = maf._ast.to_hql()

    return Table(Env.hail().methods.IBD.apply(require_biallelic(dataset, 'ibd')._jvds,
                                              joption(maf),
                                              bounded,
                                              joption(min),
                                              joption(max)))


@typecheck(call=expr_call,
           aaf_threshold=numeric,
           include_par=bool,
           female_threshold=numeric,
           male_threshold=numeric,
           aaf=nullable(str))
def impute_sex(call, aaf_threshold=0.0, include_par=False, female_threshold=0.2, male_threshold=0.8, aaf=None):
    """Impute sex of samples by calculating inbreeding coefficient on the X
    chromosome.

    .. include:: ../_templates/req_tvariant.rst

    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------

    Remove samples where imputed sex does not equal reported sex:

    >>> imputed_sex = hl.impute_sex(ds.GT)
    >>> ds.filter_cols(imputed_sex[ds.s].is_female != ds.pheno.is_female)

    Notes
    -----

    We have used the same implementation as `PLINK v1.7
    <http://pngu.mgh.harvard.edu/~purcell/plink/summary.shtml#sexcheck>`__.

    Let `gr` be the the reference genome of the type of the `locus` key (as
    given by :meth:`.TLocus.reference_genome`)

    1. Filter the dataset to loci on the X contig defined by `gr`.

    2. Calculate alternate allele frequency (AAF) for each row from the dataset.

    3. Filter to variants with AAF above `aaf_threshold`.

    4. Remove loci in the pseudoautosomal region, as defined by `gr`, if and
       only if `include_par` is ``True`` (it defaults to ``False``)

    5. For each row and column with a non-missing genotype call, :math:`E`, the
       expected number of homozygotes (from population AAF), is computed as
       :math:`1.0 - (2.0*maf*(1.0-maf))`.

    6. For each row and column with a non-missing genotype call, :math:`O`, the
       observed number of homozygotes, is computed interpreting ``0`` as
       heterozygote and ``1`` as homozygote`

    7. For each row and column with a non-missing genotype call, :math:`N` is
       incremented by 1

    8. For each column, :math:`E`, :math:`O`, and :math:`N` are combined across
       variants

    9. For each column, :math:`F` is calculated by :math:`(O - E) / (N - E)`

    10. A sex is assigned to each sample with the following criteria:
        - Female when ``F < 0.2``
        - Male when ``F > 0.8``
        Use `female_threshold` and `male_threshold` to change this behavior.

    **Annotations**

    The returned column-key indexed :class:`.Table` has the following fields in
    addition to the matrix table's column keys:

    - **is_female** (:py:data:`.tbool`) -- True if the imputed sex is female,
      false if male, missing if undetermined.
    - **f_stat** (:py:data:`.tfloat64`) -- Inbreeding coefficient.
    - **n_called**  (:py:data:`.tint64`) -- Number of variants with a genotype call.
    - **expected_homs** (:py:data:`.tfloat64`) -- Expected number of homozygotes.
    - **observed_homs** (:py:data:`.tint64`) -- Observed number of homozygotes.

    call : :class:`.CallExpression`
        A genotype call for each row and column. The source dataset's row keys
        must be [[locus], alleles] with types :class:`.tlocus` and
        :class:`.ArrayStringExpression`. Moreover, the alleles array must have
        exactly two elements (i.e. the variant must be biallelic).
    aaf_threshold : :obj:`float`
        Minimum alternate allele frequency threshold.
    include_par : :obj:`bool`
        Include pseudoautosomal regions.
    female_threshold : :obj:`float`
        Samples are called females if F < female_threshold.
    male_threshold : :obj:`float`
        Samples are called males if F > male_threshold.
    aaf : :obj:`str` or :obj:`None`
        A field defining the alternate allele frequency for each row. If
        ``None``, AAF will be computed from `call`.

    Return
    ------
    :class:`.Table`
        Sex imputation statistics per sample.
    """
    if aaf_threshold < 0.0 or aaf_threshold > 1.0:
        raise FatalError("Invalid argument for `aaf_threshold`. Must be in range [0, 1].")

    ds = call._indices.source
    ds, _ = ds._process_joins(call)
    ds = ds.annotate_entries(call=call)
    ds = require_biallelic(ds, 'impute_sex')
    if (aaf is None):
        ds = ds.annotate_rows(aaf=agg.call_stats(ds.call, ds.alleles).AF[1])
        aaf = 'aaf'

    # FIXME: filter_intervals
    if include_par:
        ds = ds.filter_rows(ds.locus.in_x_par() | ds.locus.in_x_nonpar())
    else:
        ds = ds.filter_rows(ds.locus.in_x_nonpar())

    ds = ds.filter_rows(ds[aaf] > aaf_threshold)
    ds = ds.annotate_cols(ib=agg.inbreeding(ds.call, ds[aaf]))
    kt = ds.select_cols(
        *ds.col_key,
        is_female=hl.cond(ds.ib.f_stat < female_threshold,
                          True,
                          hl.cond(ds.ib.f_stat > male_threshold,
                                  False,
                                  hl.null(tbool))),
        **ds.ib).cols()

    return kt


@typecheck(dataset=MatrixTable,
           ys=oneof(expr_float64, listof(expr_float64)),
           x=expr_float64,
           covariates=listof(expr_float64),
           root=str,
           block_size=int)
def linear_regression(dataset, ys, x, covariates=[], root='linreg', block_size=16):
    """For each row, test a derived input variable for association with
    response variables using linear regression.

    Examples
    --------

    >>> dataset_result = hl.linear_regression(dataset, [dataset.pheno.height], dataset.GT.n_alt_alleles(),
    ...                                       covariates=[dataset.pheno.age, dataset.pheno.is_female])

    Warning
    -------
    :func:`.linear_regression` considers the same set of columns (i.e., samples, points)
    for every response variable and row, namely those columns for which **all**
    response variables and covariates are defined. For each row, missing values
    of `x` are mean-imputed over these columns.

    Notes
    -----
    With the default root, the following row-indexed fields are added. The
    indexing of the array fields corresponds to that of `ys`.

    - **linreg.n_complete_samples** (:py:data:`.tint32`) -- Number of columns used.
    - **linreg.sum_x** (:py:data:`.tfloat64`) -- Sum of input values `x`.
    - **linreg.y_transpose_x** (:class:`.tarray` of :py:data:`.tfloat64`) -- Array of
      dot products of each response vector `y` with the input vector `x`.
    - **linreg.beta** (:class:`.tarray` of :py:data:`.tfloat64`) -- Array of
      fit effect coefficients of `x`, :math:`\hat\\beta_1` below.
    - **linreg.standard_error** (:class:`.tarray` of :py:data:`.tfloat64`) -- Array of
      estimated standard errors, :math:`\widehat{\mathrm{se}}_1`.
    - **linreg.t_stat** (:class:`.tarray` of :py:data:`.tfloat64`) -- Array
      of :math:`t`-statistics, equal to
      :math:`\hat\\beta_1 / \widehat{\mathrm{se}}_1`.
    - **linreg.p_value** (:class:`.tarray` of :py:data:`.tfloat64`) -- array
      of :math:`p`-values.

    In the statistical genetics example above, the input variable `x` encodes
    genotype as the number of alternate alleles (0, 1, or 2). For each variant
    (row), genotype is tested for association with height controlling for age
    and sex, by fitting the linear regression model:

    .. math::

        \mathrm{height} = \\beta_0 + \\beta_1 \, \mathrm{genotype}
                          + \\beta_2 \, \mathrm{age}
                          + \\beta_3 \, \mathrm{is_female}
                          + \\varepsilon, \quad \\varepsilon
                        \sim \mathrm{N}(0, \sigma^2)

    Boolean covariates like :math:`\mathrm{is_female}` are encoded as 1 for
    ``True`` and 0 for ``False``. The null model sets :math:`\\beta_1 = 0`.

    The standard least-squares linear regression model is derived in Section
    3.2 of `The Elements of Statistical Learning, 2nd Edition
    <http://statweb.stanford.edu/~tibs/ElemStatLearn/printings/ESLII_print10.pdf>`__.
    See equation 3.12 for the t-statistic which follows the t-distribution with
    :math:`n - k - 2` degrees of freedom, under the null hypothesis of no
    effect, with :math:`n` samples and :math:`k` covariates in addition to
    ``x`` and the intercept.

    Parameters
    ----------
    y : :class:`.Float64Expression` or :obj:`list` of :class:`.Float64Expression`
        One or more column-indexed response expressions.
    x : :class:`.Float64Expression`
        Row- and column-indexed expression for input variable.
    covariates : :obj:`list` of :class:`.Float64Expression`
        List of column-indexed covariate expressions.
    root : :obj:`str`
        Name of resulting row-indexed field.
    block_size : :obj:`int`
        Number of row regressions to perform simultaneously per core. Larger blocks
        require more memory but may improve performance.

    Returns
    -------
    :class:`.MatrixTable`
        Dataset with regression results in a new row-indexed field.
    """

    all_exprs = [x]

    ys = wrap_to_list(ys)

    # x is entry-indexed
    analyze('linear_regression/x', x, dataset._entry_indices)

    # ys and covariates are col-indexed
    ys = wrap_to_list(ys)
    for e in ys:
        all_exprs.append(e)
        analyze('linear_regression/ys', e, dataset._col_indices)
    for e in covariates:
        all_exprs.append(e)
        analyze('linear_regression/covariates', e, dataset._col_indices)

    base, cleanup = dataset._process_joins(*all_exprs)

    jm = base._jvds.linreg(
        jarray(Env.jvm().java.lang.String, [y._ast.to_hql() for y in ys]),
        x._ast.to_hql(),
        jarray(Env.jvm().java.lang.String, [cov._ast.to_hql() for cov in covariates]),
        root,
        block_size
    )

    return cleanup(MatrixTable(jm))


@typecheck(dataset=MatrixTable,
           test=enumeration('wald', 'lrt', 'score', 'firth'),
           y=expr_float64,
           x=expr_float64,
           covariates=listof(expr_float64),
           root=str)
def logistic_regression(dataset, test, y, x, covariates=[], root='logreg'):
    r"""For each row, test a derived input variable for association with a
    Boolean response variable using logistic regression.

    Examples
    --------

    Run the logistic regression Wald test per variant using a Boolean
    phenotype and two covariates stored in column-indexed fields:

    >>> ds_result = hl.logistic_regression(
    ...     dataset,
    ...     test='wald',
    ...     y=dataset.pheno.is_case,
    ...     x=dataset.GT.n_alt_alleles(),
    ...     covariates=[dataset.pheno.age, dataset.pheno.is_female])

    Notes
    -----

    This method performs, for each row, a significance test of the input
    variable in predicting a binary (case-control) response variable based on
    the logistic regression model. The response variable type must either be
    numeric (with all present values 0 or 1) or Boolean, in which case true and
    false are coded as 1 and 0, respectively.

    Hail supports the Wald test ('wald'), likelihood ratio test ('lrt'), Rao
    score test ('score'), and Firth test ('firth'). Hail only includes columns
    for which the response variable and all covariates are defined. For each
    row, Hail imputes missing input values as the mean of the non-missing
    values.

    The example above considers a model of the form

    .. math::

        \mathrm{Prob}(\mathrm{is_case}) =
            \mathrm{sigmoid}(\beta_0 + \beta_1 \, \mathrm{gt}
                            + \beta_2 \, \mathrm{age}
                            + \beta_3 \, \mathrm{is_female} + \varepsilon),
        \quad
        \varepsilon \sim \mathrm{N}(0, \sigma^2)

    where :math:`\mathrm{sigmoid}` is the `sigmoid function`_, the genotype
    :math:`\mathrm{gt}` is coded as 0 for HomRef, 1 for Het, and 2 for
    HomVar, and the Boolean covariate :math:`\mathrm{is_female}` is coded as
    for ``True`` (female) and 0 for ``False`` (male). The null model sets
    :math:`\beta_1 = 0`.

    .. _sigmoid function: https://en.wikipedia.org/wiki/Sigmoid_function

    The resulting variant annotations depend on the test statistic as shown
    in the tables below.

    ========== ======================= ======= ============================================
    Test       Field                   Type    Value
    ========== ======================= ======= ============================================
    Wald       `logreg.beta`           float64 fit effect coefficient,
                                               :math:`\hat\beta_1`
    Wald       `logreg.standard_error` float64 estimated standard error,
                                               :math:`\widehat{\mathrm{se}}`
    Wald       `logreg.z_stat`         float64 Wald :math:`z`-statistic, equal to
                                               :math:`\hat\beta_1 / \widehat{\mathrm{se}}`
    Wald       `logreg.p_value`        float64 Wald p-value testing :math:`\beta_1 = 0`
    LRT, Firth `logreg.beta`           float64 fit effect coefficient,
                                               :math:`\hat\beta_1`
    LRT, Firth `logreg.chi_sq_stat`    float64 deviance statistic
    LRT, Firth `logreg.p_value`        float64 LRT / Firth p-value testing
                                               :math:`\beta_1 = 0`
    Score      `logreg.chi_sq_stat`    float64 score statistic
    Score      `logreg.p_value`        float64 score p-value testing :math:`\beta_1 = 0`
    ========== ======================= ======= ============================================

    For the Wald and likelihood ratio tests, Hail fits the logistic model for
    each row using Newton iteration and only emits the above annotations
    when the maximum likelihood estimate of the coefficients converges. The
    Firth test uses a modified form of Newton iteration. To help diagnose
    convergence issues, Hail also emits three variant annotations which
    summarize the iterative fitting process:

    ================ ========================= ======= ===============================
    Test             Field                     Type    Value
    ================ ========================= ======= ===============================
    Wald, LRT, Firth `logreg.fit.n_iterations` int32   number of iterations until
                                                       convergence, explosion, or
                                                       reaching the max (25 for
                                                       Wald, LRT; 100 for Firth)
    Wald, LRT, Firth `logreg.fit.converged`    bool    ``True`` if iteration converged
    Wald, LRT, Firth `logreg.fit.exploded`     bool    ``True`` if iteration exploded
    ================ ========================= ======= ===============================

    We consider iteration to have converged when every coordinate of
    :math:`\beta` changes by less than :math:`10^{-6}`. For Wald and LRT,
    up to 25 iterations are attempted; in testing we find 4 or 5 iterations
    nearly always suffice. Convergence may also fail due to explosion,
    which refers to low-level numerical linear algebra exceptions caused by
    manipulating ill-conditioned matrices. Explosion may result from (nearly)
    linearly dependent covariates or complete separation_.

    .. _separation: https://en.wikipedia.org/wiki/Separation_(statistics)

    A more common situation in genetics is quasi-complete seperation, e.g.
    variants that are observed only in cases (or controls). Such variants
    inevitably arise when testing millions of variants with very low minor
    allele count. The maximum likelihood estimate of :math:`\beta` under
    logistic regression is then undefined but convergence may still occur after
    a large number of iterations due to a very flat likelihood surface. In
    testing, we find that such variants produce a secondary bump from 10 to 15
    iterations in the histogram of number of iterations per variant. We also
    find that this faux convergence produces large standard errors and large
    (insignificant) p-values. To not miss such variants, consider using Firth
    logistic regression, linear regression, or group-based tests.

    Here's a concrete illustration of quasi-complete seperation in R. Suppose
    we have 2010 samples distributed as follows for a particular variant:

    ======= ====== === ======
    Status  HomRef Het HomVar
    ======= ====== === ======
    Case    1000   10  0
    Control 1000   0   0
    ======= ====== === ======

    The following R code fits the (standard) logistic, Firth logistic,
    and linear regression models to this data, where ``x`` is genotype,
    ``y`` is phenotype, and ``logistf`` is from the logistf package:

    .. code-block:: R

        x <- c(rep(0,1000), rep(1,1000), rep(1,10)
        y <- c(rep(0,1000), rep(0,1000), rep(1,10))
        logfit <- glm(y ~ x, family=binomial())
        firthfit <- logistf(y ~ x)
        linfit <- lm(y ~ x)

    The resulting p-values for the genotype coefficient are 0.991, 0.00085,
    and 0.0016, respectively. The erroneous value 0.991 is due to
    quasi-complete separation. Moving one of the 10 hets from case to control
    eliminates this quasi-complete separation; the p-values from R are then
    0.0373, 0.0111, and 0.0116, respectively, as expected for a less
    significant association.

    The Firth test reduces bias from small counts and resolves the issue of
    separation by penalizing maximum likelihood estimation by the
    `Jeffrey's invariant prior <https://en.wikipedia.org/wiki/Jeffreys_prior>`__.
    This test is slower, as both the null and full model must be fit per
    variant, and convergence of the modified Newton method is linear rather than
    quadratic. For Firth, 100 iterations are attempted for the null model and,
    if that is successful, for the full model as well. In testing we find 20
    iterations nearly always suffices. If the null model fails to converge, then
    the `logreg.fit` fields reflect the null model; otherwise, they reflect the
    full model.

    See
    `Recommended joint and meta-analysis strategies for case-control association testing of single low-count variants <http://www.ncbi.nlm.nih.gov/pmc/articles/PMC4049324/>`__
    for an empirical comparison of the logistic Wald, LRT, score, and Firth
    tests. The theoretical foundations of the Wald, likelihood ratio, and score
    tests may be found in Chapter 3 of Gesine Reinert's notes
    `Statistical Theory <http://www.stats.ox.ac.uk/~reinert/stattheory/theoryshort09.pdf>`__.
    Firth introduced his approach in
    `Bias reduction of maximum likelihood estimates, 1993 <http://www2.stat.duke.edu/~scs/Courses/Stat376/Papers/GibbsFieldEst/BiasReductionMLE.pdf>`__.
    Heinze and Schemper further analyze Firth's approach in
    `A solution to the problem of separation in logistic regression, 2002 <https://cemsiis.meduniwien.ac.at/fileadmin/msi_akim/CeMSIIS/KB/volltexte/Heinze_Schemper_2002_Statistics_in_Medicine.pdf>`__.

    Those variants that don't vary across the included samples (e.g., all
    genotypes are HomRef) will have missing annotations.

    For Boolean covariate types, ``True`` is coded as 1 and ``False`` as 0. In
    particular, for the sample annotation `fam.is_case` added by importing a FAM
    file with case-control phenotype, case is 1 and control is 0.

    Hail's logistic regression tests correspond to the ``b.wald``, ``b.lrt``,
    and ``b.score`` tests in `EPACTS`_. For each variant, Hail imputes missing
    input values as the mean of non-missing input values, whereas EPACTS
    subsets to those samples with called genotypes. Hence, Hail and EPACTS
    results will currently only agree for variants with no missing genotypes.

    .. _EPACTS: http://genome.sph.umich.edu/wiki/EPACTS#Single_Variant_Tests

    Parameters
    ----------
    test : {'wald', 'lrt', 'score', 'firth'}
        Statistical test.
    y : :class:`.Float64Expression`
        Column-indexed response expression.
        All non-missing values must evaluate to 0 or 1.
        Note that a :class:`.BooleanExpression` will be implicitly converted to
        a :class:`.Float64Expression` with this property.
    x : :class:`.Float64Expression`
        Row- and column-indexed expression for input variable.
    covariates : :obj:`list` of :class:`.Float64Expression`
        List of column-indexed covariate expressions.
    root : :obj:`str`, optional
        Name of resulting row-indexed field.

    Returns
    -------
    :class:`.MatrixTable`
        Dataset with regression results in a new row-indexed field.
    """

    # x is entry-indexed
    analyze('logistic_regression/x', x, dataset._entry_indices)

    # y and covariates are col-indexed
    analyze('logistic_regression/y', y, dataset._col_indices)

    all_exprs = [x, y]
    for e in covariates:
        all_exprs.append(e)
        analyze('logistic_regression/covariates', e, dataset._col_indices)

    base, cleanup = dataset._process_joins(*all_exprs)

    jds = base._jvds.logreg(
        test,
        y._ast.to_hql(),
        x._ast.to_hql(),
        jarray(Env.jvm().java.lang.String,
               [cov._ast.to_hql() for cov in covariates]),
        root)

    return cleanup(MatrixTable(jds))


@typecheck(ds=MatrixTable,
           kinship_matrix=KinshipMatrix,
           y=expr_float64,
           x=expr_float64,
           covariates=listof(expr_float64),
           global_root=str,
           row_root=str,
           run_assoc=bool,
           use_ml=bool,
           delta=nullable(numeric),
           sparsity_threshold=numeric,
           n_eigenvectors=nullable(int),
           dropped_variance_fraction=(nullable(float)))
def linear_mixed_regression(ds, kinship_matrix, y, x, covariates=[], global_root="lmmreg_global",
                            row_root="lmmreg", run_assoc=True, use_ml=False, delta=None,
                            sparsity_threshold=1.0, n_eigenvectors=None, dropped_variance_fraction=None):
    r"""Use a kinship-based linear mixed model to estimate the genetic component
    of phenotypic variance (narrow-sense heritability) and optionally test each
    variant for association.

    Examples
    --------
    Compute a :class:`.KinshipMatrix`, and use it to test variants for
    association using a linear mixed model:

    .. testsetup::

        lmmreg_ds = hl.variant_qc(hl.split_multi_hts(hl.import_vcf('data/sample.vcf.bgz')))
        lmmreg_tsv = hl.import_table('data/example_lmmreg.tsv', 'Sample', impute=True)
        lmmreg_ds = lmmreg_ds.annotate_cols(**lmmreg_tsv[lmmreg_ds['s']])
        lmmreg_ds = lmmreg_ds.annotate_rows(use_in_kinship = lmmreg_ds.variant_qc.AF > 0.05)
        lmmreg_ds.write('data/example_lmmreg.vds', overwrite=True)


    >>> lmm_ds = hl.read_matrix_table("data/example_lmmreg.vds")
    >>> kinship_matrix = hl.realized_relationship_matrix(lmm_ds.filter_rows(lmm_ds.use_in_kinship)['GT'])
    >>> lmm_ds = hl.linear_mixed_regression(lmm_ds,
    ...                                     kinship_matrix,
    ...                                     lmm_ds.pheno,
    ...                                     lmm_ds.GT.n_alt_alleles(),
    ...                                     [lmm_ds.cov1, lmm_ds.cov2])

    Notes
    -----
    Suppose the variant dataset saved at :file:`data/example_lmmreg.vds` has a
    Boolean variant-indexed field `use_in_kinship` and numeric or Boolean
    sample-indexed fields `pheno`, `cov1`, and `cov2`. Then the
    :func:`.linear_mixed_regression` function in the above example will execute
    the following four steps in order:

    1) filter to samples in given kinship matrix to those for which
       `ds.pheno`, `ds.cov`, and `ds.cov2` are all defined
    2) compute the eigendecomposition :math:`K = USU^T` of the kinship matrix
    3) fit covariate coefficients and variance parameters in the
       sample-covariates-only (global) model using restricted maximum
       likelihood (`REML`_), storing results in a global field under
       `lmmreg_global`
    4) test each variant for association, storing results in a row-indexed
       field under `lmmreg`

    .. _REML: https://en.wikipedia.org/wiki/Restricted_maximum_likelihood

    This plan can be modified as follows:

    - Set `run_assoc` to :obj:`False` to not test any variants for association,
      i.e. skip Step 5.
    - Set `use_ml` to :obj:`True` to use maximum likelihood instead of REML in
      Steps 4 and 5.
    - Set the `delta` argument to manually set the value of :math:`\delta`
      rather that fitting :math:`\delta` in Step 4.
    - Set the `global_root` argument to change the global annotation root in
      Step 4.
    - Set the `row_root` argument to change the variant annotation root in
      Step 5.

    :func:`.linear_mixed_regression` adds 9 or 13 global annotations in Step 4,
    depending on whether :math:`\delta` is set or fit. These global annotations
    are stored under the prefix `global_root`, which is by default
    ``lmmreg_global``. The prefix is not displayed in the table below.

    .. list-table::
       :header-rows: 1

       * - Field
         - Type
         - Value
       * - `use_ml`
         - bool
         - true if fit by ML, false if fit by REML
       * - `beta`
         - dict<str, float64>
         - map from *intercept* and the given `covariates` expressions to the
           corresponding fit :math:`\beta` coefficients
       * - `sigma_g_squared`
         - float64
         - fit coefficient of genetic variance, :math:`\hat{\sigma}_g^2`
       * - `sigma_e_squared`
         - float64
         - fit coefficient of environmental variance :math:`\hat{\sigma}_e^2`
       * - `delta`
         - float64
         - fit ratio of variance component coefficients, :math:`\hat{\delta}`
       * - `h_squared`
         - float64
         - fit narrow-sense heritability, :math:`\hat{h}^2`
       * - `n_eigenvectors`
         - int32
         - number of eigenvectors of kinship matrix used to fit model
       * - `dropped_variance_fraction`
         - float64
         - specified value of `dropped_variance_fraction`
       * - `eigenvalues`
         - array<float64>
         - all eigenvalues of the kinship matrix in descending order
       * - `fit.standard_error_h_squared`
         - float64
         - standard error of :math:`\hat{h}^2` under asymptotic normal
           approximation
       * - `fit.normalized_likelihood_h_squared`
         - array<float64>
         - likelihood function of :math:`h^2` normalized on the discrete grid
           ``0.01, 0.02, ..., 0.99``. Index ``i`` is the likelihood for
           percentage ``i``.
       * - `fit.max_log_likelihood`
         - float64
         - (restricted) maximum log likelihood corresponding to
           :math:`\hat{\delta}`
       * - `fit.log_delta_grid`
         - array<float64>
         - values of :math:`\mathrm{ln}(\delta)` used in the grid search
       * - `fit.log_likelihood_values`
         - array<float64>
         - (restricted) log likelihood of :math:`y` given :math:`X` and
           :math:`\mathrm{ln}(\delta)` at the (RE)ML fit of :math:`\beta` and
           :math:`\sigma_g^2`

    These global annotations are also added to ``hail.log``, with the ranked
    evals and :math:`\delta` grid with values in .tsv tabular form.  Use
    ``grep 'linear mixed regression' hail.log`` to find the lines just above
    each table.

    If Step 5 is performed, :func:`.linear_mixed_regression` also adds four
    linear regression row fields. These annotations are stored as `row_root`,
    which defaults to ``lmmreg``. Once again, the prefix is not displayed in the
    table.

    +-------------------+---------+------------------------------------------------+
    | Field             | Type    | Value                                          |
    +===================+=========+================================================+
    | `beta`            | float64 | fit genotype coefficient, :math:`\hat\beta_0`  |
    +-------------------+---------+------------------------------------------------+
    | `sigma_g_squared` | float64 | fit coefficient of genetic variance component, |
    |                   |         | :math:`\hat{\sigma}_g^2`                       |
    +-------------------+---------+------------------------------------------------+
    | `chi_sq_stat`     | float64 | :math:`\chi^2` statistic of the likelihood     |
    |                   |         | ratio test                                     |
    +-------------------+---------+------------------------------------------------+
    | `p_value`         | float64 | :math:`p`-value                                |
    +-------------------+---------+------------------------------------------------+

    Those variants that don't vary across the included samples (e.g., all
    genotypes are HomRef) will have missing annotations.

    **Performance**

    Hail's initial version of :func:`.linear_mixed_regression` scales beyond
    15k samples and to an essentially unbounded number of variants, making it
    particularly well-suited to modern sequencing studies and complementary to
    tools designed for SNP arrays. Analysts have used
    :func:`.linear_mixed_regression` in research to compute kinship from 100k
    common variants and test 32 million non-rare variants on 8k whole genomes in
    about 10 minutes on `Google cloud`_.

    .. _Google cloud:
        http://discuss.hail.is/t/using-hail-on-the-google-cloud-platform/80

    While :func:`.linear_mixed_regression` computes the kinship matrix
    :math:`K` using distributed matrix multiplication (Step 2), the full
    `eigendecomposition`_ (Step 3) is currently run on a single core of master
    using the `LAPACK routine DSYEVD`_, which we empirically find to be the most
    performant of the four available routines; laptop performance plots showing
    cubic complexity in :math:`n` are available `here
    <https://github.com/hail-is/hail/pull/906>`__. On Google cloud,
    eigendecomposition takes about 2 seconds for 2535 sampes and 1 minute for
    8185 samples. If you see worse performance, check that LAPACK natives are
    being properly loaded (see "BLAS and LAPACK" in Getting Started).

    .. _LAPACK routine DSYEVD:
        http://www.netlib.org/lapack/explore-html/d2/d8a/group__double_s_yeigen_ga694ddc6e5527b6223748e3462013d867.html

    .. _eigendecomposition:
        https://en.wikipedia.org/wiki/Eigendecomposition_of_a_matrix

    Given the eigendecomposition, fitting the global model (Step 4) takes on
    the order of a few seconds on master. Association testing (Step 5) is fully
    distributed by variant with per-variant time complexity that is completely
    independent of the number of sample covariates and dominated by
    multiplication of the genotype vector :math:`v` by the matrix of
    eigenvectors :math:`U^T` as described below, which we accelerate with a
    sparse representation of :math:`v`.  The matrix :math:`U^T` has size about
    :math:`8n^2` bytes and is currently broadcast to each Spark executor. For
    example, with 15k samples, storing :math:`U^T` consumes about 3.6GB of
    memory on a 16-core worker node with two 8-core executors. So for large
    :math:`n`, we recommend using a high-memory configuration such as
    *highmem* workers.

    **Linear mixed model**

    :func:`.linear_mixed_regression` estimates the genetic proportion of
    residual phenotypic variance (narrow-sense heritability) under a
    kinship-based linear mixed model, and then optionally tests each variant for
    association using the likelihood ratio test. Inference is exact.

    We first describe the sample-covariates-only model used to estimate
    heritability, which we simply refer to as the *global model*. With
    :math:`n` samples and :math:`c` sample covariates, we define:

    - :math:`y = n \times 1` vector of phenotypes
    - :math:`X = n \times c` matrix of sample covariates and intercept column
      of ones
    - :math:`K = n \times n` kinship matrix
    - :math:`I = n \times n` identity matrix
    - :math:`\beta = c \times 1` vector of covariate coefficients
    - :math:`\sigma_g^2 =` coefficient of genetic variance component :math:`K`
    - :math:`\sigma_e^2 =` coefficient of environmental variance component
      :math:`I`
    - :math:`\delta = \frac{\sigma_e^2}{\sigma_g^2} =` ratio of environmental
      and genetic variance component coefficients
    - :math:`h^2 = \frac{\sigma_g^2}{\sigma_g^2 + \sigma_e^2} = \frac{1}{1 + \delta} =`
      genetic proportion of residual phenotypic variance

    Under a linear mixed model, :math:`y` is sampled from the
    :math:`n`-dimensional `multivariate normal distribution`_ with mean
    :math:`X \beta` and variance components that are scalar multiples of
    :math:`K` and :math:`I`:

    .. math::

      y \sim \mathrm{N}\left(X\beta, \sigma_g^2 K + \sigma_e^2 I\right)

    .. _multivariate normal distribution:
       https://en.wikipedia.org/wiki/Multivariate_normal_distribution

    Thus the model posits that the residuals :math:`y_i - X_{i,:}\beta` and
    :math:`y_j - X_{j,:}\beta` have covariance :math:`\sigma_g^2 K_{ij}` and
    approximate correlation :math:`h^2 K_{ij}`. Informally: phenotype residuals
    are correlated as the product of overall heritability and pairwise kinship.
    By contrast, standard (unmixed) linear regression is equivalent to fixing
    :math:`\sigma_2` (equivalently, :math:`h^2`) at 0 above, so that all
    phenotype residuals are independent.

    **Caution:** while it is tempting to interpret :math:`h^2` as the
    `narrow-sense heritability`_ of the phenotype alone, note that its value
    depends not only the phenotype and genetic data, but also on the choice of
    sample covariates.

    .. _narrow-sense heritability: https://en.wikipedia.org/wiki/Heritability#Definition

    **Fitting the global model**

    The core algorithm is essentially a distributed implementation of the
    spectral approach taken in `FastLMM`_. Let :math:`K = USU^T` be the
    `eigendecomposition`_ of the real symmetric matrix :math:`K`. That is:

    - :math:`U = n \times n` orthonormal matrix whose columns are the
      eigenvectors of :math:`K`
    - :math:`S = n \times n` diagonal matrix of eigenvalues of :math:`K` in
      descending order. :math:`S_{ii}` is the eigenvalue of eigenvector
      :math:`U_{:,i}`
    - :math:`U^T = n \times n` orthonormal matrix, the transpose (and inverse)
      of :math:`U`

    .. _FastLMM: https://www.microsoft.com/en-us/research/project/fastlmm/

    A bit of matrix algebra on the multivariate normal density shows that the
    linear mixed model above is mathematically equivalent to the model

    .. math::

      U^Ty \sim \mathrm{N}\left(U^TX\beta, \sigma_g^2 (S + \delta I)\right)

    for which the covariance is diagonal (e.g., unmixed). That is, rotating the
    phenotype vector (:math:`y`) and covariate vectors (columns of :math:`X`)
    in :math:`\mathbb{R}^n` by :math:`U^T` transforms the model to one with
    independent residuals. For any particular value of :math:`\delta`, the
    restricted maximum likelihood (REML) solution for the latter model can be
    solved exactly in time complexity that is linear rather than cubic in
    :math:`n`.  In particular, having rotated, we can run a very efficient
    1-dimensional optimization procedure over :math:`\delta` to find the REML
    estimate :math:`(\hat{\delta}, \hat{\beta}, \hat{\sigma}_g^2)` of the
    triple :math:`(\delta, \beta, \sigma_g^2)`, which in turn determines
    :math:`\hat{\sigma}_e^2` and :math:`\hat{h}^2`.

    We first compute the maximum log likelihood on a :math:`\delta`-grid that
    is uniform on the log scale, with :math:`\mathrm{ln}(\delta)` running from
    -8 to 8 by 0.01, corresponding to :math:`h^2` decreasing from 0.9995 to
    0.0005. If :math:`h^2` is maximized at the lower boundary then standard
    linear regression would be more appropriate and Hail will exit; more
    generally, consider using standard linear regression when :math:`\hat{h}^2`
    is very small. A maximum at the upper boundary is highly suspicious and
    will also cause Hail to exit. In any case, the log file records the table
    of grid values for further inspection, beginning under the info line
    containing "linear mixed regression: table of delta".

    If the optimal grid point falls in the interior of the grid as expected,
    we then use `Brent's method`_ to find the precise location of the maximum
    over the same range, with initial guess given by the optimal grid point and
    a tolerance on :math:`\mathrm{ln}(\delta)` of 1e-6. If this location
    differs from the optimal grid point by more than 0.01, a warning will be
    displayed and logged, and one would be wise to investigate by plotting the
    values over the grid.

    .. _Brent's method: https://en.wikipedia.org/wiki/Brent%27s_method

    Note that :math:`h^2` is related to :math:`\mathrm{ln}(\delta)` through the
    `sigmoid function`_. More precisely,

    .. math::

      h^2 = 1 - \mathrm{sigmoid}(\mathrm{ln}(\delta))
          = \mathrm{sigmoid}(-\mathrm{ln}(\delta))

    .. _sigmoid function: https://en.wikipedia.org/wiki/Sigmoid_function

    Hence one can change variables to extract a high-resolution discretization
    of the likelihood function of :math:`h^2` over :math:`[0, 1]` at the
    corresponding REML estimators for :math:`\beta` and :math:`\sigma_g^2`, as
    well as integrate over the normalized likelihood function using
    `change of variables`_ and the `sigmoid differential equation`_.

    .. _change of variables: https://en.wikipedia.org/wiki/Integration_by_substitution
    .. _sigmoid differential equation: https://en.wikipedia.org/wiki/Sigmoid_function#Properties

    For convenience, `lmmreg.fit.normalized_likelihood_h_squared` records the
    the likelihood function of :math:`h^2` normalized over the discrete grid
    :math:`0.01, 0.02, \ldots, 0.98, 0.99`. The length of the array is 101 so
    that index ``i`` contains the likelihood at percentage ``i``. The values at
    indices 0 and 100 are left undefined.

    By the theory of maximum likelihood estimation, this normalized likelihood
    function is approximately normally distributed near the maximum likelihood
    estimate. So we estimate the standard error of the estimator of :math:`h^2`
    as follows. Let :math:`x_2` be the maximum likelihood estimate of
    :math:`h^2` and let :math:`x_ 1` and :math:`x_3` be just to the left and
    right of :math:`x_2`. Let :math:`y_1`, :math:`y_2`, and :math:`y_3` be the
    corresponding values of the (unnormalized) log likelihood function. Setting
    equal the leading coefficient of the unique parabola through these points
    (as given by Lagrange interpolation) and the leading coefficient of the log
    of the normal distribution, we have:

    .. math::

      \frac{x_3 (y_2 - y_1) + x_2 (y_1 - y_3) + x_1 (y_3 - y_2))}
           {(x_2 - x_1)(x_1 - x_3)(x_3 - x_2)} = -\frac{1}{2 \sigma^2}

    The standard error :math:`\hat{\sigma}` is then estimated by solving for
    :math:`\sigma`.

    Note that the mean and standard deviation of the (discretized or
    continuous) distribution held in
    `lmmreg.fit.normalized_likelihood_h_squared` will not coincide with
    :math:`\hat{h}^2` and :math:`\hat{\sigma}`, since this distribution only
    becomes normal in the infinite sample limit. One can visually assess
    normality by plotting this distribution against a normal distribution with
    the same mean and standard deviation, or use this distribution to
    approximate credible intervals under a flat prior on :math:`h^2`.

    **Testing each variant for association**

    Fixing a single variant, we define:

    - :math:`v = n \times 1` input vector, with missing values imputed as the
      mean of the non-missing values
    - :math:`X_v = \left[v | X \right] = n \times (1 + c)` matrix concatenating
      :math:`v` and :math:`X`
    - :math:`\beta_v = (\beta^0_v, \beta^1_v, \ldots, \beta^c_v) = (1 + c) \times 1`
      vector of covariate coefficients

    Fixing :math:`\delta` at the global REML estimate :math:`\hat{\delta}`, we
    find the REML estimate :math:`(\hat{\beta}_v, \hat{\sigma}_{g, v}^2)` via
    rotation of the model

    .. math::

      y \sim \mathrm{N}\left(X_v\beta_v, \sigma_{g,v}^2 (K + \hat{\delta} I)\right)

    Note that the only new rotation to compute here is :math:`U^T v`.

    To test the null hypothesis that the genotype coefficient :math:`\beta^0_v`
    is zero, we consider the restricted model with parameters
    :math:`((0, \beta^1_v, \ldots, \beta^c_v), \sigma_{g,v}^2)` within the full
    model with parameters
    :math:`(\beta^0_v, \beta^1_v, \ldots, \beta^c_v), \sigma_{g_v}^2)`, with
    :math:`\delta` fixed at :math:`\hat\delta` in both. The latter fit is
    simply that of the global model,
    :math:`((0, \hat{\beta}^1, \ldots, \hat{\beta}^c), \hat{\sigma}_g^2)`. The
    likelihood ratio test statistic is given by

    .. math::

      \chi^2 = n \, \mathrm{ln}\left(\frac{\hat{\sigma}^2_g}{\hat{\sigma}_{g,v}^2}\right)

    and follows a chi-squared distribution with one degree of freedom. Here the
    ratio :math:`\hat{\sigma}^2_g / \hat{\sigma}_{g,v}^2` captures the degree
    to which adding the variant :math:`v` to the global model reduces the
    residual phenotypic variance.

    **Kinship Matrix**

    FastLMM uses the Realized Relationship Matrix (RRM) for kinship. This can
    be computed with :func:`.rrm`. However, any instance of
    :class:`.KinshipMatrix` may be used, so long as
    :meth:`~.KinshipMatrix.sample_list` contains the complete samples of the
    caller variant dataset in the same order.

    **Low-rank approximation of kinship for improved performance**

    :func:`.linear_mixed_regression` can implicitly use a low-rank
    approximation of the kinship matrix to more rapidly fit delta and the
    statistics for each variant. The computational complexity per variant is
    proportional to the number of eigenvectors used. This number can be
    specified in two ways. Specify the parameter `n_eigenvectors` to use only the
    top `n_eigenvectors` eigenvectors. Alternatively, specify
    `dropped_variance_fraction` to use as many eigenvectors as necessary to
    capture all but at most this fraction of the sample variance (also known as
    the trace, or the sum of the eigenvalues). For example, setting
    `dropped_variance_fraction` to 0.01 will use the minimal number of
    eigenvectors to account for 99% of the sample variance. Specifying both
    parameters will apply the more stringent (fewest eigenvectors) of the two.

    **Further background**

    For the history and mathematics of linear mixed models in genetics,
    including `FastLMM`_, see `Christoph Lippert's PhD thesis
    <https://publikationen.uni-tuebingen.de/xmlui/bitstream/handle/10900/50003/pdf/thesis_komplett.pdf>`__.
    For an investigation of various approaches to defining kinship, see
    `Comparison of Methods to Account for Relatedness in Genome-Wide Association
    Studies with Family-Based Data
    <http://journals.plos.org/plosgenetics/article?id=10.1371/journal.pgen.1004445>`__.


    Parameters
    ----------
    kinship_matrix : :class:`.KinshipMatrix`
        Kinship matrix to be used.
    y : :class:`.Float64Expression`
        Column-indexed response expression.
    x : :class:`.Float64Expression`
        Row- and column-indexed expression for input variable.
    covariates : :obj:`list` of :class:`.Float64Expression`
        List of column-indexed covariate expressions.
    global_root : :obj:`str`
        Global field root.
    row_root : :obj:`str`
        Row-indexed field root.
    run_assoc : :obj:`bool`
        If true, run association testing in addition to fitting the global model.
    use_ml : :obj:`bool`
        Use ML instead of REML throughout.
    delta : :obj:`float` or :obj:`None`
        Fixed delta value to use in the global model, overrides fitting delta.
    sparsity_threshold : :obj:`float`
        Genotype vector sparsity at or below which to use sparse genotype
        vector in rotation (advanced).
    n_eigenvectors : :obj:`int`
        Number of eigenvectors of the kinship matrix used to fit the model.
    dropped_variance_fraction : :obj:`float`
        Upper bound on fraction of sample variance lost by dropping
        eigenvectors with small eigenvalues.

    Returns
    -------
    :class:`.MatrixTable`
        Variant dataset with linear mixed regression annotations.
    """

    # x is entry-indexed
    analyze('linear_mixed_regression/x', x, ds._entry_indices)

    # y and covariates are col-indexed
    analyze('linear_mixed_regression/y', y, ds._col_indices)

    all_exprs = [x, y]
    for e in covariates:
        all_exprs.append(e)
        analyze('linear_mixed_regression/covariates', e, ds._col_indices)

    base, cleanup = ds._process_joins(*all_exprs)

    jds = base._jvds.lmmreg(kinship_matrix._jkm,
                            y._ast.to_hql(),
                            x._ast.to_hql(),
                            jarray(Env.jvm().java.lang.String,
                                   [cov._ast.to_hql() for cov in covariates]),
                            use_ml,
                            global_root,
                            row_root,
                            run_assoc,
                            joption(delta),
                            sparsity_threshold,
                            joption(n_eigenvectors),
                            joption(dropped_variance_fraction))

    return cleanup(MatrixTable(jds))


@typecheck(dataset=MatrixTable,
           key_expr=expr_any,
           weight_expr=expr_float64,
           y=expr_float64,
           x=expr_float64,
           covariates=listof(expr_float64),
           logistic=bool,
           max_size=int,
           accuracy=numeric,
           iterations=int)
def skat(dataset, key_expr, weight_expr, y, x, covariates=[], logistic=False,
         max_size=46340, accuracy=1e-6, iterations=10000):
    r"""Test each keyed group of rows for association by linear or logistic
    SKAT test.

    Examples
    --------

    Test each gene for association using the linear sequence kernel association
    test:

    .. testsetup::

        burden_ds = hl.import_vcf('data/example_burden.vcf')
        burden_kt = hl.import_table('data/example_burden.tsv', key='Sample', impute=True)
        burden_ds = burden_ds.annotate_cols(burden = burden_kt[burden_ds.s])
        burden_ds = burden_ds.annotate_rows(weight = hl.float64(burden_ds.locus.position))
        burden_ds = hl.variant_qc(burden_ds)
        genekt = hl.import_locus_intervals('data/gene.interval_list')
        burden_ds = burden_ds.annotate_rows(gene = genekt[burden_ds.locus])
        burden_ds.write('data/example_burden.vds', overwrite=True)

    >>> burden_ds = hl.read_matrix_table('data/example_burden.vds')
    >>> skat_table = hl.skat(burden_ds,
    ...                      key_expr=burden_ds.gene,
    ...                      weight_expr=burden_ds.weight,
    ...                      y=burden_ds.burden.pheno,
    ...                      x=burden_ds.GT.n_alt_alleles(),
    ...                      covariates=[burden_ds.burden.cov1, burden_ds.burden.cov2])

    .. caution::

       By default, the Davies algorithm iterates up to 10k times until an
       accuracy of 1e-6 is achieved. Hence a reported p-value of zero with no
       issues may truly be as large as 1e-6. The accuracy and maximum number of
       iterations may be controlled by the corresponding function parameters.
       In general, higher accuracy requires more iterations.

    .. caution::

       To process a group with :math:`m` rows, several copies of an
       :math:`m \times m` matrix of doubles must fit in worker memory. Groups
       with tens of thousands of rows may exhaust worker memory causing the
       entire job to fail. In this case, use the `max_size` parameter to skip
       groups larger than `max_size`.

    Notes
    -----

    This method provides a scalable implementation of the score-based
    variance-component test originally described in
    `Rare-Variant Association Testing for Sequencing Data with the Sequence Kernel Association Test
    <https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3135811/>`__.

    The test is run on columns with `y` and all `covariates` non-missing. For
    each row, missing input (`x`) values are imputed as the mean of all
    non-missing input values.

    Row weights must be non-negative. Rows with missing weights are ignored. In
    the R package ``skat``---which assumes rows are variants---default weights
    are given by evaluating the Beta(1, 25) density at the minor allele
    frequency. To replicate these weights in Hail using alternate allele
    frequencies stored in a row-indexd field `AF`, one can use the expression:

    .. testsetup::

        ds2 = hl.variant_qc(dataset)
        ds2 = ds2.select_rows(ds2.locus, ds2.alleles, AF = ds2.variant_qc.AF)

    .. doctest::

        >>> hl.dbeta(hl.min(ds2.AF, 1 - ds2.AF),
        ...          1.0, 25.0) ** 2

    In the logistic case, the response `y` must either be numeric (with all
    present values 0 or 1) or Boolean, in which case true and false are coded
    as 1 and 0, respectively.

    The resulting :class:`.Table` provides the group's key, the size (number of
    rows) in the group, the variance component score `q_stat`, the SKAT
    p-value, and a fault flag. For the toy example above, the table has the
    form:

    +-------+------+--------+---------+-------+
    |  key  | size | q_stat | p_value | fault |
    +=======+======+========+=========+=======+
    | geneA |   2  | 4.136  | 0.205   |   0   |
    +-------+------+--------+---------+-------+
    | geneB |   1  | 5.659  | 0.195   |   0   |
    +-------+------+--------+---------+-------+
    | geneC |   3  | 4.122  | 0.192   |   0   |
    +-------+------+--------+---------+-------+

    Groups larger than `max_size` appear with missing `q_stat`, `p_value`, and
    `fault`. The hard limit on the number of rows in a group is 46340.

    Note that the variance component score `q_stat` agrees with ``Q`` in the R
    package ``skat``, but both differ from :math:`Q` in the paper by the factor
    :math:`\frac{1}{2\sigma^2}` in the linear case and :math:`\frac{1}{2}` in
    the logistic case, where :math:`\sigma^2` is the unbiased estimator of
    residual variance for the linear null model. The R package also applies a
    "small-sample adjustment" to the null distribution in the logistic case
    when the sample size is less than 2000. Hail does not apply this
    adjustment.

    The fault flag is an integer indicating whether any issues occurred when
    running the Davies algorithm to compute the p-value as the right tail of a
    weighted sum of :math:`\chi^2(1)` distributions.

    +-------------+-----------------------------------------+
    | fault value | Description                             |
    +=============+=========================================+
    |      0      | no issues                               |
    +------+------+-----------------------------------------+
    |      1      | accuracy NOT achieved                   |
    +------+------+-----------------------------------------+
    |      2      | round-off error possibly significant    |
    +------+------+-----------------------------------------+
    |      3      | invalid parameters                      |
    +------+------+-----------------------------------------+
    |      4      | unable to locate integration parameters |
    +------+------+-----------------------------------------+
    |      5      | out of memory                           |
    +------+------+-----------------------------------------+

    Parameters
    ----------
    key_expr : :class:`.Expression`
        Row-indexed expression for key associated to each row.
    weight_expr : :class:`.Float64Expression`
        Row-indexed expression for row weights.
    y : :class:`.Float64Expression`
        Column-indexed response expression.
    x : :class:`.Float64Expression`
        Row- and column-indexed expression for input variable.
    covariates : :obj:`list` of :class:`.Float64Expression`
        List of column-indexed covariate expressions.
    logistic : :obj:`bool`
        If true, use the logistic test rather than the linear test.
    max_size : :obj:`int`
        Maximum size of group on which to run the test.
    accuracy : :obj:`float`
        Accuracy achieved by the Davies algorithm if fault value is zero.
    iterations : :obj:`int`
        Maximum number of iterations attempted by the Davies algorithm.

    Returns
    -------
    :class:`.Table`
        Table of SKAT results.
    """
    all_exprs = [key_expr, weight_expr, x, y]

    # key_expr and weight_expr are row_indexed
    analyze('skat/key_expr', key_expr, dataset._row_indices)
    analyze('skat/weight_expr', weight_expr, dataset._row_indices)

    # x is entry-indexed
    analyze('skat/x', x, dataset._entry_indices)

    # y and covariates are col-indexed
    analyze('skat/y', y, dataset._col_indices)
    for e in covariates:
        all_exprs.append(e)
        analyze('skat/covariates', e, dataset._col_indices)

    base, _ = dataset._process_joins(*all_exprs)

    jt = base._jvds.skat(
        key_expr._ast.to_hql(),
        weight_expr._ast.to_hql(),
        y._ast.to_hql(),
        x._ast.to_hql(),
        jarray(Env.jvm().java.lang.String, [cov._ast.to_hql() for cov in covariates]),
        logistic,
        max_size,
        accuracy,
        iterations
    )

    return Table(jt)


@typecheck(dataset=MatrixTable,
           k=int,
           compute_loadings=bool,
           as_array=bool)
def hwe_normalized_pca(dataset, k=10, compute_loadings=False, as_array=False):
    r"""Run principal component analysis (PCA) on the Hardy-Weinberg-normalized
    genotype call matrix.

    Examples
    --------

    >>> eigenvalues, scores, loadings = hl.hwe_normalized_pca(dataset, k=5)

    Notes
    -----

    This is a specialization of :func:`.pca` for the common use case
    of PCA in statistical genetics, that of projecting samples to a small
    number of ancestry coordinates. Variants that are all homozygous reference
    or all homozygous alternate are unnormalizable and removed before
    evaluation.

    Users of PLINK/GCTA should be aware that Hail computes the GRM slightly
    differently with regard to missing data. In Hail, the
    :math:`ij` entry of the GRM :math:`MM^T` is simply the dot product of rows
    :math:`i` and :math:`j` of :math:`M`; in terms of :math:`C` it is

    .. math::
    
      \frac{1}{m}\sum_{l\in\mathcal{C}_i\cap\mathcal{C}_j}\frac{(C_{il}-2p_l)(C_{jl} - 2p_l)}{2p_l(1-p_l)}

    where :math:`\mathcal{C}_i = \{l \mid C_{il} \text{ is non-missing}\}`. In
    PLINK/GCTA the denominator :math:`m` is replaced with the number of terms in
    the sum :math:`\lvert\mathcal{C}_i\cap\mathcal{C}_j\rvert`, i.e. the
    number of variants where both samples have non-missing genotypes. While this
    is arguably a better estimator of the true GRM (trading shrinkage for
    noise), it has the drawback that one loses the clean interpretation of the
    loadings and scores as features and projections

    Separately, for the PCs PLINK/GCTA output the eigenvectors of the GRM, i.e.
    the left singular vectors :math:`U_k` instead of the component scores
    :math:`U_k S_k`. The scores have the advantage of representing true
    projections of the data onto features with the variance of a score
    reflecting the variance explained by the corresponding feature. In PC
    bi-plots this amounts to a change in aspect ratio; for use of PCs as
    covariates in regression it is immaterial.

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset.
    k : :obj:`int`
        Number of principal components.
    compute_loadings : :obj:`bool`
        If ``True``, compute row loadings.
    as_array : :obj:`bool`
        If ``True``, return scores and loadings as an array field. If ``False``, return
        one field per element (`PC1`, `PC2`, ... `PCk`).

    Returns
    -------
    (:obj:`list` of :obj:`float`, :class:`.Table`, :class:`.Table`)
        List of eigenvalues, table with column scores, table with row loadings.
    """
    dataset = require_biallelic(dataset, 'hwe_normalized_pca')
    dataset = dataset.annotate_rows(AC=agg.sum(dataset.GT.n_alt_alleles()),
                                    n_called=agg.count_where(hl.is_defined(dataset.GT)))
    dataset = dataset.filter_rows((dataset.AC > 0) & (dataset.AC < 2 * dataset.n_called))

    # once count_rows() adds partition_counts we can avoid annotating and filtering twice
    n_variants = dataset.count_rows()
    if n_variants == 0:
        raise FatalError(
            "Cannot run PCA: found 0 variants after filtering out monomorphic sites.")
    info("Running PCA using {} variants.".format(n_variants))

    entry_expr = hl.bind(
        dataset.AC / dataset.n_called,
        lambda mean_gt: hl.cond(hl.is_defined(dataset.GT),
                                (dataset.GT.n_alt_alleles() - mean_gt) /
                                hl.sqrt(mean_gt * (2 - mean_gt) * n_variants / 2),
                                0.0))
    result = pca(entry_expr,
                 k,
                 compute_loadings,
                 as_array)

    return result


@typecheck(entry_expr=expr_float64,
           k=int,
           compute_loadings=bool,
           as_array=bool)
def pca(entry_expr, k=10, compute_loadings=False, as_array=False):
    r"""Run principal component analysis (PCA) on numeric columns derived from a
    matrix table.

    Examples
    --------

    For a matrix table with variant rows, sample columns, and genotype entries,
    compute the top 2 PC sample scores and eigenvalues of the matrix of 0s and
    1s encoding missingness of genotype calls.

    >>> eigenvalues, scores, _ = hl.pca(hl.int(hl.is_defined(dataset.GT)),
    ...                                      k=2)

    Warning
    -------
      This method does **not** automatically mean-center or normalize each column.
      If desired, such transformations should be incorporated in `entry_expr`.

      Hail will return an error if `entry_expr` evaluates to missing, NaN, or
      infinity on any entry.

    Notes
    -----

    PCA is run on the columns of the numeric matrix obtained by evaluating
    `entry_expr` on each entry of the matrix table, or equivalently on the rows
    of the **transposed** numeric matrix :math:`M` referenced below.

    PCA computes the SVD

    .. math::

      M = USV^T

    where columns of :math:`U` are left singular vectors (orthonormal in
    :math:`\mathbb{R}^n`), columns of :math:`V` are right singular vectors
    (orthonormal in :math:`\mathbb{R}^m`), and :math:`S=\mathrm{diag}(s_1, s_2,
    \ldots)` with ordered singular values :math:`s_1 \ge s_2 \ge \cdots \ge 0`.
    Typically one computes only the first :math:`k` singular vectors and values,
    yielding the best rank :math:`k` approximation :math:`U_k S_k V_k^T` of
    :math:`M`; the truncations :math:`U_k`, :math:`S_k` and :math:`V_k` are
    :math:`n \times k`, :math:`k \times k` and :math:`m \times k`
    respectively.

    From the perspective of the rows of :math:`M` as samples (data points),
    :math:`V_k` contains the loadings for the first :math:`k` PCs while
    :math:`MV_k = U_k S_k` contains the first :math:`k` PC scores of each
    sample. The loadings represent a new basis of features while the scores
    represent the projected data on those features. The eigenvalues of the Gramian
    :math:`MM^T` are the squares of the singular values :math:`s_1^2, s_2^2,
    \ldots`, which represent the variances carried by the respective PCs. By
    default, Hail only computes the loadings if the ``loadings`` parameter is
    specified.

    Scores are stored in a :class:`.Table` with the column keys of the matrix
    followed by the principal component scores. If `as_array` is ``True``, there
    is one row field `scores` of type ``array<float64>`` containing the principal
    component scores. If `as_array` is ``False`` (default), then each principal
    component score is a new row field of type ``float64`` with the names `PC1`,
    `PC2`, etc.

    Loadings are stored in a :class:`.Table` with a structure similar to the
    scores table except the row keys of the matrix are followed by the loadings.
    If `as_array` is ``False``, the loadings are stored in one row field
    `loadings` of type ``array<float64>``. Otherwise, each principal component
    loading is a new row field of type ``float64`` with the names `PC1`, `PC2`,
    etc.

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset.
    entry_expr : :class:`.Expression`
        Numeric expression for matrix entries.
    k : :obj:`int`
        Number of principal components.
    compute_loadings : :obj:`bool`
        If ``True``, compute row loadings.
    as_array : :obj:`bool`
        If ``True``, return scores and loadings as an array field. If ``False``,
        add one row field per element (`PC1`, `PC2`, ... `PCk`).

    Returns
    -------
    (:obj:`list` of :obj:`float`, :class:`.Table`, :class:`.Table`)
        List of eigenvalues, table with column scores, table with row loadings.
    """
    source = entry_expr._indices.source
    if not isinstance(source, MatrixTable):
        raise ValueError("Expect an expression of 'MatrixTable', found {}".format(
            "expression of '{}'".format(source.__class__) if source is not None else 'scalar expression'))
    dataset = source
    base, _ = dataset._process_joins(entry_expr)
    analyze('pca', entry_expr, dataset._entry_indices)

    r = Env.hail().methods.PCA.apply(dataset._jvds, to_expr(entry_expr)._ast.to_hql(), k, compute_loadings, as_array)
    scores = Table(Env.hail().methods.PCA.scoresTable(dataset._jvds, as_array, r._2()))
    loadings = from_option(r._3())
    if loadings:
        loadings = Table(loadings)
    return (jiterable_to_list(r._1()), scores, loadings)


@typecheck(dataset=MatrixTable,
           k=int,
           maf=numeric,
           block_size=int,
           min_kinship=numeric,
           statistics=enumeration("phi", "phik2", "phik2k0", "all"))
def pc_relate(dataset, k, maf, block_size=512, min_kinship=-float("inf"), statistics="all"):
    """Compute relatedness estimates between individuals using a variant
    of the PC-Relate method.

    .. include:: ../_templates/experimental.rst

    .. include:: ../_templates/req_tvariant.rst

    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------

    Estimate kinship, identity-by-descent two, identity-by-descent one, and
    identity-by-descent zero for every pair of samples, using 10 prinicpal
    components to correct for ancestral populations, and a minimum minor
    allele frequency filter of 0.01:

    >>> rel = hl.pc_relate(dataset, 10, 0.01)

    Calculate values as above, but when performing distributed matrix
    multiplications use a matrix-block-size of 1024 by 1024.

    >>> rel = hl.pc_relate(dataset, 10, 0.01, 1024)

    Calculate values as above, excluding sample-pairs with kinship less
    than 0.1. This is more efficient than producing the full table and
    filtering using :meth:`.Table.filter`.

    >>> rel = hl.pc_relate(dataset, 5, 0.01, min_kinship=0.1)

    The traditional estimator for kinship between a pair of individuals
    :math:`i` and :math:`j`, sharing the set :math:`S_{ij}` of
    single-nucleotide variants, from a population with allele frequencies
    :math:`p_s`, is given by:

    .. math::

      \\widehat{\\phi_{ij}} :=
        \\frac{1}{|S_{ij}|}
        \\sum_{s \\in S_{ij}}
          \\frac{(g_{is} - 2 p_s) (g_{js} - 2 p_s)}
                {4 * \\sum_{s \\in S_{ij} p_s (1 - p_s)}}

    This estimator is true under the model that the sharing of common
    (relative to the population) alleles is not very informative to
    relatedness (because they're common) and the sharing of rare alleles
    suggests a recent common ancestor from which the allele was inherited by
    descent.

    When multiple ancestry groups are mixed in a sample, this model breaks
    down. Alleles that are rare in all but one ancestry group are treated as
    very informative to relatedness. However, these alleles are simply
    markers of the ancestry group. The PC-Relate method corrects for this
    situation and the related situation of admixed individuals.

    PC-Relate slightly modifies the usual estimator for relatedness:
    occurences of population allele frequency are replaced with an
    "individual-specific allele frequency". This modification allows the
    method to correctly weight an allele according to an individual's unique
    ancestry profile.

    The "individual-specific allele frequency" at a given genetic locus is
    modeled by PC-Relate as a linear function of their first ``k`` principal
    component coordinates. As such, the efficacy of this method rests on two
    assumptions:

     - an individual's first ``k`` principal component coordinates fully
       describe their allele-frequency-relevant ancestry, and

     - the relationship between ancestry (as described by principal
       component coordinates) and population allele frequency is linear

    The estimators for kinship, and identity-by-descent zero, one, and two
    follow. Let:

     - :math:`S_{ij}` be the set of genetic loci at which both individuals
       :math:`i` and :math:`j` have a defined genotype

     - :math:`g_{is} \\in {0, 1, 2}` be the number of alternate alleles that
       individual :math:`i` has at gentic locus :math:`s`

     - :math:`\\widehat{\\mu_{is}} \\in [0, 1]` be the individual-specific allele
       frequency for individual :math:`i` at genetic locus :math:`s`

     - :math:`{\\widehat{\\sigma^2_{is}}} := \\widehat{\\mu_{is}} (1 - \\widehat{\\mu_{is}})`,
       the binomial variance of :math:`\\widehat{\\mu_{is}}`

     - :math:`\\widehat{\\sigma_{is}} := \\sqrt{\\widehat{\\sigma^2_{is}}}`,
       the binomial standard deviation of :math:`\\widehat{\\mu_{is}}`

     - :math:`\\text{IBS}^{(0)}_{ij} := \\sum_{s \\in S_{ij}} \\mathbb{1}_{||g_{is} - g_{js} = 2||}`,
       the number of genetic loci at which individuals :math:`i` and :math:`j`
       share no alleles

     - :math:`\\widehat{f_i} := 2 \\widehat{\\phi_{ii}} - 1`, the inbreeding
       coefficient for individual :math:`i`

     - :math:`g^D_{is}` be a dominance encoding of the genotype matrix, and
       :math:`X_{is}` be a normalized dominance-coded genotype matrix

    .. math::

        g^D_{is} :=
          \\begin{cases}
            \\widehat{\\mu_{is}}     & g_{is} = 0 \\\\
            0                        & g_{is} = 1 \\\\
            1 - \\widehat{\\mu_{is}} & g_{is} = 2
          \\end{cases}

        X_{is} := g^D_{is} - \\widehat{\\sigma^2_{is}} (1 - \\widehat{f_i})

    The estimator for kinship is given by:

    .. math::

      \\widehat{\phi_{ij}} :=
        \\frac{\sum_{s \\in S_{ij}}(g - 2 \\mu)_{is} (g - 2 \\mu)_{js}}
              {4 * \\sum_{s \\in S_{ij}}
                            \\widehat{\\sigma_{is}} \\widehat{\\sigma_{js}}}

    The estimator for identity-by-descent two is given by:

    .. math::

      \\widehat{k^{(2)}_{ij}} :=
        \\frac{\\sum_{s \\in S_{ij}}X_{is} X_{js}}{\sum_{s \\in S_{ij}}
          \\widehat{\\sigma^2_{is}} \\widehat{\\sigma^2_{js}}}

    The estimator for identity-by-descent zero is given by:

    .. math::

      \\widehat{k^{(0)}_{ij}} :=
        \\begin{cases}
          \\frac{\\text{IBS}^{(0)}_{ij}}
                {\\sum_{s \\in S_{ij}}
                       \\widehat{\\mu_{is}}^2(1 - \\widehat{\\mu_{js}})^2
                       + (1 - \\widehat{\\mu_{is}})^2\\widehat{\\mu_{js}}^2}
            & \\widehat{\\phi_{ij}} > 2^{-5/2} \\\\
          1 - 4 \\widehat{\\phi_{ij}} + k^{(2)}_{ij}
            & \\widehat{\\phi_{ij}} \\le 2^{-5/2}
        \\end{cases}

    The estimator for identity-by-descent one is given by:

    .. math::

      \\widehat{k^{(1)}_{ij}} :=
        1 - \\widehat{k^{(2)}_{ij}} - \\widehat{k^{(0)}_{ij}}

    Notes
    -----
    The PC-Relate method is described in "Model-free Estimation of Recent
    Genetic Relatedness". Conomos MP, Reiner AP, Weir BS, Thornton TA. in
    American Journal of Human Genetics. 2016 Jan 7. The reference
    implementation is available in the `GENESIS Bioconductor package
    <https://bioconductor.org/packages/release/bioc/html/GENESIS.html>`_ .

    :func:`.pc_relate` differs from the reference
    implementation in a couple key ways:

     - the principal components analysis does not use an unrelated set of
       individuals

     - the estimators do not perform small sample correction

     - the algorithm does not provide an option to use population-wide
       allele frequency estimates

     - the algorithm does not provide an option to not use "overall
       standardization" (see R ``pcrelate`` documentation)

    Note
    ----
    The `block_size` controls memory usage and parallelism. If it is large
    enough to hold an entire sample-by-sample matrix of 64-bit doubles in
    memory, then only one Spark worker node can be used to compute matrix
    operations. If it is too small, communication overhead will begin to
    dominate the computation's time. The author has found that on Google
    Dataproc (where each core has about 3.75GB of memory), setting `block_size`
    larger than 512 tends to cause memory exhaustion errors.

    Note
    ----
    The minimum allele frequency filter is applied per-pair: if either of
    the two individual's individual-specific minor allele frequency is below
    the threshold, then the variant's contribution to relatedness estimates
    is zero.

    Note
    ----
    Under the PC-Relate model, kinship, :math:`\\phi_{ij}`, ranges from 0 to
    0.5, and is precisely half of the
    fraction-of-genetic-material-shared. Listed below are the statistics for
    a few pairings:

     - Monozygotic twins share all their genetic material so their kinship
       statistic is 0.5 in expection.

     - Parent-child and sibling pairs both have kinship 0.25 in expectation
       and are separated by the identity-by-descent-zero, :math:`k^{(2)}_{ij}`,
       statistic which is zero for parent-child pairs and 0.25 for sibling
       pairs.

     - Avuncular pairs and grand-parent/-child pairs both have kinship 0.125
       in expectation and both have identity-by-descent-zero 0.5 in expectation

     - "Third degree relatives" are those pairs sharing
       :math:`2^{-3} = 12.5 %` of their genetic material, the results of
       PCRelate are often too noisy to reliably distinguish these pairs from
       higher-degree-relative-pairs or unrelated pairs.

    Parameters
    ----------
    ds : :class:`.MatrixTable`
        A variant-keyed :class:`.MatrixTable` containing
        genotype information.
    k : :obj:`int`
        The number of principal components to use to distinguish ancestries.
    maf : :obj:`float`
        The minimum individual-specific allele frequency for an allele used to
        measure relatedness.
    block_size : :obj:`int`
        the side length of the blocks of the block-distributed matrices; this
        should be set such that at least three of these matrices fit in memory
        (in addition to all other objects necessary for Spark and Hail).
    min_kinship : :obj:`float`
        Pairs of samples with kinship lower than ``min_kinship`` are excluded
        from the results.
    statistics : :obj:`str`
        the set of statistics to compute, `phi` will only compute the
        kinship statistic, `phik2` will compute the kinship and
        identity-by-descent two statistics, `phik2k0` will compute the
        kinship statistics and both identity-by-descent two and zero, `all`
        computes the kinship statistic and all three identity-by-descent
        statistics.

    Returns
    -------
    :class:`.Table`
        A :class:`.Table` mapping pairs of samples to estimations of their
        kinship and identity-by-descent zero, one, and two.

        The fields of the resulting :class:`.Table` entries are of types: `i`:
        :py:data:`.tstr`, `j`: :py:data:`.tstr`, `kin`: :py:data:`.tfloat64`, `k2`:
        :py:data:`.tfloat64`, `k1`: :py:data:`.tfloat64`, `k0`:
        :py:data:`.tfloat64`. The table is keyed by `i` and `j`.

    """
    require_col_key_str(dataset, 'pc_relate')
    dataset = require_biallelic(dataset, 'pc_relate')
    intstatistics = {"phi": 0, "phik2": 1, "phik2k0": 2, "all": 3}[statistics]
    _, scores, _ = hwe_normalized_pca(dataset, k, False, True)
    return Table(
        scala_object(Env.hail().methods, 'PCRelate')
            .apply(dataset._jvds,
                   k,
                   scores._jt,
                   maf,
                   block_size,
                   min_kinship,
                   intstatistics))


class SplitMulti(object):
    """Split multiallelic variants.

    Example
    -------

    :func:`.split_multi_hts`, which splits
    multiallelic variants for the HTS genotype schema and updates
    the genotype annotations by downcoding the genotype, is
    implemented as:

    >>> sm = hl.SplitMulti(ds)
    >>> pl = hl.or_missing(
    ...      hl.is_defined(ds.PL),
    ...      (hl.range(0, 3).map(lambda i: hl.min(hl.range(0, hl.len(ds.PL))
    ...                     .filter(lambda j: hl.downcode(hl.unphased_diploid_gt_index_call(j), sm.a_index()) == hl.unphased_diploid_gt_index_call(i))
    ...                     .map(lambda j: ds.PL[j])))))
    >>> sm.update_rows(a_index=sm.a_index(), was_split=sm.was_split())
    >>> sm.update_entries(
    ...     GT=hl.downcode(ds.GT, sm.a_index()),
    ...     AD=hl.or_missing(hl.is_defined(ds.AD),
    ...                     [hl.sum(ds.AD) - ds.AD[sm.a_index()], ds.AD[sm.a_index()]]),
    ...     DP=ds.DP,
    ...     PL=pl,
    ...     GQ=hl.gq_from_pl(pl))
    >>> split_ds = sm.result()
    """

    @typecheck_method(ds=MatrixTable,
                      keep_star=bool,
                      left_aligned=bool)
    def __init__(self, ds, keep_star=False, left_aligned=False):
        """
        Parameters
        ----------
        ds : :class:`.MatrixTable`
            An unsplit dataset.
        keep_star : :obj:`bool`
            Do not filter out * alleles.
        left_aligned : :obj:`bool`
            If ``True``, variants are assumed to be left aligned and have unique
            loci. This avoids a shuffle. If the assumption is violated, an error
            is generated.

        Returns
        -------
        :class:`.SplitMulti`
        """
        self._ds = ds
        self._keep_star = keep_star
        self._left_aligned = left_aligned
        self._entry_fields = None
        self._row_fields = None

    def new_locus(self):
        """The new, split variant locus.

        Returns
        -------
        :class:`.LocusExpression`
        """
        return construct_reference(
            "newLocus", type=self._ds.locus.dtype, indices=self._ds._row_indices)

    def new_alleles(self):
        """The new, split variant alleles.

        Returns
        -------
        :class:`.ArrayStringExpression`
        """
        return construct_reference(
            "newAlleles", type=tarray(tstr), indices=self._ds._row_indices)

    def a_index(self):
        """The index of the input allele to the output variant.

        Returns
        -------
        :class:`.Expression` of type :py:data:`.tint32`
        """
        return construct_reference(
            "aIndex", type=tint32, indices=self._ds._row_indices)

    def was_split(self):
        """``True`` if the original variant was multiallelic.

        Returns
        -------
        :class:`.BooleanExpression`
        """
        return construct_reference(
            "wasSplit", type=tbool, indices=self._ds._row_indices)

    def update_rows(self, **kwargs):
        """Set the row field updates for this SplitMulti object.

        Note
        ----
        May only be called once.
        """
        if self._row_fields is None:
            self._row_fields = kwargs
        else:
            raise FatalError("You may only call update_rows once")

    def update_entries(self, **kwargs):
        """Set the entry field updates for this SplitMulti object.

        Note
        ----
        May only be called once.
        """
        if self._entry_fields is None:
            self._entry_fields = kwargs
        else:
            raise FatalError("You may only call update_entries once")

    def result(self):
        """Split the dataset.

        Returns
        -------
        :class:`.MatrixTable`
            A split dataset.
        """

        if not self._row_fields:
            self._row_fields = {}
        if not self._entry_fields:
            self._entry_fields = {}

        base, _ = self._ds._process_joins(*itertools.chain(
            self._row_fields.values(), self._entry_fields.values()))

        annotate_rows = ','.join(['va.`{}` = {}'.format(k, v._ast.to_hql())
                                  for k, v in self._row_fields.items()])
        annotate_entries = ','.join(['g.`{}` = {}'.format(k, v._ast.to_hql())
                                     for k, v in self._entry_fields.items()])

        jvds = scala_object(Env.hail().methods, 'SplitMulti').apply(
            self._ds._jvds,
            annotate_rows,
            annotate_entries,
            self._keep_star,
            self._left_aligned)
        return MatrixTable(jvds)


@typecheck(ds=MatrixTable,
           keep_star=bool,
           left_aligned=bool)
def split_multi_hts(ds, keep_star=False, left_aligned=False):
    """Split multiallelic variants for datasets with a standard high-throughput
    sequencing entry schema.

    .. code-block:: text

      struct {
        GT: call,
        AD: array<int32>,
        DP: int32,
        GQ: int32,
        PL: array<int32>
      }

    For generic genotype schema, use :func:`.split_multi_hts`.

    Examples
    --------

    >>> hl.split_multi_hts(dataset).write('output/split.vds')

    Notes
    -----

    We will explain by example. Consider a hypothetical 3-allelic
    variant:

    .. code-block:: text

      A   C,T 0/2:7,2,6:15:45:99,50,99,0,45,99

    :func:`.split_multi_hts` will create two biallelic variants (one for each
    alternate allele) at the same position

    .. code-block:: text

      A   C   0/0:13,2:15:45:0,45,99
      A   T   0/1:9,6:15:50:50,0,99

    Each multiallelic `GT` field is downcoded once for each alternate allele. A
    call for an alternate allele maps to 1 in the biallelic variant
    corresponding to itself and 0 otherwise. For example, in the example above,
    0/2 maps to 0/0 and 0/1. The genotype 1/2 maps to 0/1 and 0/1.

    The biallelic alt `AD` entry is just the multiallelic `AD` entry
    corresponding to the alternate allele. The ref AD entry is the sum of the
    other multiallelic entries.

    The biallelic `DP` is the same as the multiallelic `DP`.

    The biallelic `PL` entry for a genotype g is the minimum over `PL` entries
    for multiallelic genotypes that downcode to g. For example, the `PL` for (A,
    T) at 0/1 is the minimum of the PLs for 0/1 (50) and 1/2 (45), and thus 45.

    Fixing an alternate allele and biallelic variant, downcoding gives a map
    from multiallelic to biallelic alleles and genotypes. The biallelic `AD` entry
    for an allele is just the sum of the multiallelic `AD` entries for alleles
    that map to that allele. Similarly, the biallelic `PL` entry for a genotype is
    the minimum over multiallelic `PL` entries for genotypes that map to that
    genotype.

    `GQ` is recomputed from `PL`.

    Here is a second example for a het non-ref

    .. code-block:: text

      A   C,T 1/2:2,8,6:16:45:99,50,99,45,0,99

    splits as

    .. code-block:: text

      A   C   0/1:8,8:16:45:45,0,99
      A   T   0/1:10,6:16:50:50,0,99

    **VCF Info Fields**

    Hail does not split fields in the info field. This means that if a
    multiallelic site with `info.AC` value ``[10, 2]`` is split, each split
    site will contain the same array ``[10, 2]``. The provided allele index
    field `a_index` can be used to select the value corresponding to the split
    allele's position:

    >>> split_ds = hl.split_multi_hts(dataset)
    >>> split_ds = split_ds.filter_rows(split_ds.info.AC[split_ds.a_index - 1] < 10,
    ...                                 keep = False)

    VCFs split by Hail and exported to new VCFs may be
    incompatible with other tools, if action is not taken
    first. Since the "Number" of the arrays in split multiallelic
    sites no longer matches the structure on import ("A" for 1 per
    allele, for example), Hail will export these fields with
    number ".".

    If the desired output is one value per site, then it is
    possible to use annotate_variants_expr to remap these
    values. Here is an example:

    >>> split_ds = hl.split_multi_hts(dataset)
    >>> split_ds = split_ds.annotate_rows(info = Struct(AC=split_ds.info.AC[split_ds.a_index - 1],
    ...                                   **split_ds.info)) # doctest: +SKIP
    >>> hl.export_vcf(split_ds, 'output/export.vcf') # doctest: +SKIP

    The info field AC in *data/export.vcf* will have ``Number=1``.

    **New Fields**

    :func:`.split_multi_hts` adds the following fields:

     - `was_split` (*Boolean*) -- ``True`` if this variant was originally
       multiallelic, otherwise ``False``.

     - `a_index` (*Int*) -- The original index of this alternate allele in the
       multiallelic representation (NB: 1 is the first alternate allele or the
       only alternate allele in a biallelic variant). For example, 1:100:A:T,C
       splits into two variants: 1:100:A:T with ``a_index = 1`` and 1:100:A:C
       with ``a_index = 2``.

    Parameters
    ----------
    keep_star : :obj:`bool`
        Do not filter out * alleles.
    left_aligned : :obj:`bool`
        If ``True``, variants are assumed to be left
        aligned and have unique loci. This avoids a shuffle. If the assumption
        is violated, an error is generated.

    Returns
    -------
    :class:`.MatrixTable`
        A biallelic variant dataset.

    """

    if ds.entry.dtype != hl.hts_entry_schema:
        raise FatalError("'split_multi_hts': entry schema must be the HTS entry schema:\n"
                         "  found: {}\n"
                         "  expected: {}\n"
                         "  Use 'split_multi' to split entries with non-HTS entry fields.".format(
            ds.entry.dtype, hl.hts_entry_schema
        ))

    sm = SplitMulti(ds, keep_star=keep_star, left_aligned=left_aligned)
    pl = hl.or_missing(
        hl.is_defined(ds.PL),
        (hl.range(0, 3).map(lambda i: hl.min((hl.range(0, hl.triangle(ds.alleles.length()))
            .filter(
            lambda j: hl.downcode(hl.unphased_diploid_gt_index_call(j),
                                  sm.a_index()) == hl.unphased_diploid_gt_index_call(
                i))
            .map(lambda j: ds.PL[j]))))))
    sm.update_rows(a_index=sm.a_index(), was_split=sm.was_split())
    sm.update_entries(
        GT=hl.downcode(ds.GT, sm.a_index()),
        AD=hl.or_missing(hl.is_defined(ds.AD),
                         [hl.sum(ds.AD) - ds.AD[sm.a_index()], ds.AD[sm.a_index()]]),
        DP=ds.DP,
        GQ=hl.gq_from_pl(pl),
        PL=pl
    )
    return sm.result()


@typecheck(dataset=MatrixTable)
def genetic_relatedness_matrix(dataset):
    """Compute the Genetic Relatedness Matrix (GRM).

    .. include:: ../_templates/req_tvariant.rst
    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------

    >>> km = hl.genetic_relatedness_matrix(dataset)

    Notes
    -----

    The genetic relationship matrix (GRM) :math:`G` encodes genetic correlation
    between each pair of samples. It is defined by :math:`G = MM^T` where
    :math:`M` is a standardized version of the genotype matrix, computed as
    follows. Let :math:`C` be the :math:`n \\times m` matrix of raw genotypes
    in the variant dataset, with rows indexed by :math:`n` samples and columns
    indexed by :math:`m` bialellic autosomal variants; :math:`C_{ij}` is the
    number of alternate alleles of variant :math:`j` carried by sample
    :math:`i`, which can be 0, 1, 2, or missing. For each variant :math:`j`,
    the sample alternate allele frequency :math:`p_j` is computed as half the
    mean of the non-missing entries of column :math:`j`. Entries of :math:`M`
    are then mean-centered and variance-normalized as

    .. math::

        M_{ij} = \\frac{C_{ij}-2p_j}{\sqrt{2p_j(1-p_j)m}},

    with :math:`M_{ij} = 0` for :math:`C_{ij}` missing (i.e. mean genotype
    imputation). This scaling normalizes genotype variances to a common value
    :math:`1/m` for variants in Hardy-Weinberg equilibrium and is further
    motivated in the paper `Patterson, Price and Reich, 2006
    <http://journals.plos.org/plosgenetics/article?id=10.1371/journal.pgen.0020190>`__.
    (The resulting amplification of signal from the low end of the allele
    frequency spectrum will also introduce noise for rare variants; common
    practice is to filter out variants with minor allele frequency below some
    cutoff.) The factor :math:`1/m` gives each sample row approximately unit
    total variance (assuming linkage equilibrium) so that the diagonal entries
    of the GRM are approximately 1. Equivalently,

    .. math::

        G_{ik} = \\frac{1}{m} \\sum_{j=1}^m \\frac{(C_{ij}-2p_j)(C_{kj}-2p_j)}{2 p_j (1-p_j)}

    Warning
    -------
    Since Hardy-Weinberg normalization cannot be applied to variants that
    contain only reference alleles or only alternate alleles, all such variants
    are removed prior to calcularing the GRM.

    Parameters
    ----------
    dataset : :class:`.MatrixTable`
        Dataset to sample from.

    Returns
    -------
    :class:`.genetics.KinshipMatrix`
        Genetic Relatedness Matrix for all samples.
    """
    require_col_key_str(dataset, 'genetic_relatedness_matrix')
    dataset = require_biallelic(dataset, "genetic_relatedness_matrix")
    dataset = dataset.annotate_rows(AC=agg.sum(dataset.GT.n_alt_alleles()),
                                    n_called=agg.count_where(hl.is_defined(dataset.GT)))
    dataset = dataset.filter_rows((dataset.AC > 0) & (dataset.AC < 2 * dataset.n_called)).persist()

    n_variants = dataset.count_rows()
    if n_variants == 0:
        raise FatalError("Cannot run GRM: found 0 variants after filtering out monomorphic sites.")
    info("Computing GRM using {} variants.".format(n_variants))

    normalized_genotype_expr = hl.bind(
        dataset.AC / dataset.n_called,
        lambda mean_gt: hl.cond(hl.is_defined(dataset.GT),
                                (dataset.GT.n_alt_alleles() - mean_gt) /
                                hl.sqrt(mean_gt * (2 - mean_gt) * n_variants / 2),
                                0))

    bm = BlockMatrix.from_entry_expr(normalized_genotype_expr)
    dataset.unpersist()
    grm = bm.T @ bm

    return KinshipMatrix._from_block_matrix(tstr,
                                            grm,
                                            [row.s for row in dataset.cols().select('s').collect()],
                                            n_variants)


@typecheck(call_expr=CallExpression)
def realized_relationship_matrix(call_expr):
    """Computes the Realized Relationship Matrix (RRM).

    .. include:: ../_templates/req_biallelic.rst

    Examples
    --------

    >>> kinship_matrix = hl.realized_relationship_matrix(dataset['GT'])

    Notes
    -----

    The Realized Relationship Matrix is defined as follows. Consider the
    :math:`n \\times m` matrix :math:`C` of raw genotypes, with rows indexed by
    :math:`n` samples and columns indexed by the :math:`m` bialellic autosomal
    variants; :math:`C_{ij}` is the number of alternate alleles of variant
    :math:`j` carried by sample :math:`i`, which can be 0, 1, 2, or missing. For
    each variant :math:`j`, the sample alternate allele frequency :math:`p_j` is
    computed as half the mean of the non-missing entries of column :math:`j`.
    Entries of :math:`M` are then mean-centered and variance-normalized as

    .. math::

        M_{ij} =
          \\frac{C_{ij}-2p_j}
                {\sqrt{\\frac{m}{n} \\sum_{k=1}^n (C_{ij}-2p_j)^2}},

    with :math:`M_{ij} = 0` for :math:`C_{ij}` missing (i.e. mean genotype
    imputation). This scaling normalizes each variant column to have empirical
    variance :math:`1/m`, which gives each sample row approximately unit total
    variance (assuming linkage equilibrium) and yields the :math:`n \\times n`
    sample correlation or realized relationship matrix (RRM) :math:`K` as simply

    .. math::

        K = MM^T

    Note that the only difference between the Realized Relationship Matrix and
    the Genetic Relationship Matrix (GRM) used in
    :func:`.realized_relationship_matrix` is the variant (column) normalization:
    where RRM uses empirical variance, GRM uses expected variance under
    Hardy-Weinberg Equilibrium.


    Parameters
    ----------
    call_expr : :class:`.CallExpression`
        Expression on a :class:`.MatrixTable` that gives the genotype call.

    Returns
        :return: Realized Relationship Matrix for all samples.
        :rtype: :class:`.KinshipMatrix`
    """
    source = call_expr._indices.source
    if not isinstance(source, MatrixTable):
        raise ValueError("Expect an expression of 'MatrixTable', found {}".format(
            "expression of '{}'".format(source.__class__) if source is not None else 'scalar expression'))
    dataset = source
    analyze('realized_relationship_matrix', call_expr, dataset._entry_indices)
    require_col_key_str(dataset, 'realized_relationship_matrix')
    dataset = dataset.annotate_entries(call=call_expr)
    dataset = require_biallelic(dataset, 'rrm')

    gt_expr = dataset.call.n_alt_alleles()
    dataset = dataset.annotate_rows(AC=agg.sum(gt_expr),
                                    ACsq=agg.sum(gt_expr * gt_expr),
                                    n_called=agg.count_where(hl.is_defined(dataset.call)))

    dataset = dataset.filter_rows((dataset.AC > 0) &
                                  (dataset.AC < 2 * dataset.n_called) &
                                  ((dataset.AC != dataset.n_called) |
                                   (dataset.ACsq != dataset.n_called)))

    n_samples = dataset.count_cols()
    n_variants = dataset.count_rows()
    if n_variants == 0:
        raise FatalError("Cannot run RRM: found 0 variants after filtering out monomorphic sites.")
    info("Computing RRM using {} variants.".format(n_variants))

    gt_expr = dataset.call.n_alt_alleles()
    normalized_genotype_expr = hl.bind(
        dataset.AC / dataset.n_called,
        lambda mean_gt: hl.bind(
            hl.sqrt((dataset.ACsq +
                     (n_samples - dataset.n_called) * mean_gt ** 2) /
                    n_samples - mean_gt ** 2),
            lambda stddev: hl.cond(hl.is_defined(dataset.call),
                                   (gt_expr - mean_gt) / stddev, 0)))

    bm = BlockMatrix.from_entry_expr(normalized_genotype_expr)
    dataset.unpersist()
    rrm = (bm.T @ bm) / n_variants

    return KinshipMatrix._from_block_matrix(tstr,
                                            rrm,
                                            dataset.col_key[0].collect(),
                                            n_variants)


@typecheck(n_populations=int,
           n_samples=int,
           n_variants=int,
           n_partitions=nullable(int),
           pop_dist=nullable(listof(numeric)),
           fst=nullable(listof(numeric)),
           af_dist=oneof(UniformDist, BetaDist, TruncatedBetaDist),
           seed=int,
           reference_genome=reference_genome_type)
def balding_nichols_model(n_populations, n_samples, n_variants, n_partitions=None,
                          pop_dist=None, fst=None, af_dist=UniformDist(0.1, 0.9),
                          seed=0, reference_genome='default'):
    r"""Generate a matrix table of variants, samples, and genotypes using the
    Balding-Nichols model.

    Examples
    --------
    Generate a matrix table of genotypes with 1000 variants and 100 samples
    across 3 populations:

    >>> bn_ds = hl.balding_nichols_model(3, 100, 1000)

    Generate a matrix table using 4 populations, 40 samples, 150 variants, 3
    partitions, population distribution ``[0.1, 0.2, 0.3, 0.4]``,
    :math:`F_{ST}` values ``[.02, .06, .04, .12]``, ancestral allele
    frequencies drawn from a truncated beta distribution with ``a = 0.01`` and
    ``b = 0.05`` over the interval ``[0.05, 1]``, and random seed 1:

    >>> from hail.stats import TruncatedBetaDist
    >>>
    >>> bn_ds = hl.balding_nichols_model(4, 40, 150, 3,
    ...          pop_dist=[0.1, 0.2, 0.3, 0.4],
    ...          fst=[.02, .06, .04, .12],
    ...          af_dist=TruncatedBetaDist(a=0.01, b=2.0, min=0.05, max=1.0),
    ...          seed=1)

    Notes
    -----
    This method simulates a matrix table of variants, samples, and genotypes
    using the Balding-Nichols model, which we now define.

    - :math:`K` populations are labeled by integers 0, 1, ..., K - 1.
    - :math:`N` samples are labeled by strings 0, 1, ..., N - 1.
    - :math:`M` variants are defined as ``1:1:A:C``, ``1:2:A:C``, ...,
      ``1:M:A:C``.
    - The default distribution for population assignment :math:`\pi` is uniform.
    - The default ancestral frequency distribution :math:`P_0` is uniform on
      ``[0.1, 0.9]``. Other options are :class:`.UniformDist`,
      :class:`.BetaDist`, and :class:`.TruncatedBetaDist`.
      All three classes are located in ``hail.stats``.
    - The default :math:`F_{ST}` values are all 0.1.

    The Balding-Nichols model models genotypes of individuals from a structured
    population comprising :math:`K` homogeneous modern populations that have
    each diverged from a single ancestral population (a `star phylogeny`). Each
    sample is assigned a population by sampling from the categorical
    distribution :math:`\pi`. Note that the actual size of each population is
    random.

    Variants are modeled as bi-allelic and unlinked. Ancestral allele
    frequencies are drawn independently for each variant from a frequency
    spectrum :math:`P_0`. The extent of genetic drift of each modern population
    from the ancestral population is defined by the corresponding :math:`F_{ST}`
    parameter :math:`F_k` (here and below, lowercase indices run over a range
    bounded by the corresponding uppercase parameter, e.g. :math:`k = 1, \ldots,
    K`). For each variant and population, allele frequencies are drawn from a
    `beta distribution <https://en.wikipedia.org/wiki/Beta_distribution>`__
    whose parameters are determined by the ancestral allele frequency and
    :math:`F_{ST}` parameter. The beta distribution gives a continuous
    approximation of the effect of genetic drift. We denote sample population
    assignments by :math:`k_n`, ancestral allele frequencies by :math:`p_m`,
    population allele frequencies by :math:`p_{k, m}`, and diploid, unphased
    genotype calls by :math:`g_{n, m}` (0, 1, and 2 correspond to homozygous
    reference, heterozygous, and homozygous variant, respectively).
    
    The generative model is then given by:

    .. math::
        k_n \,&\sim\, \pi

        p_m \,&\sim\, P_0

        p_{k,m} \mid p_m\,&\sim\, \mathrm{Beta}(\mu = p_m,\, \sigma^2 = F_k p_m (1 - p_m))

        g_{n,m} \mid k_n, p_{k, m} \,&\sim\, \mathrm{Binomial}(2, p_{k_n, m})

    The beta distribution by its mean and variance above; the usual parameters
    are :math:`a = (1 - p) \frac{1 - F}{F}` and :math:`b = p \frac{1 - F}{F}` with
    :math:`F = F_k` and :math:`p = p_m`.

    The resulting dataset has the following fields.

    Global fields:

    - `n_populations` (:py:data:`.tint32`) -- Number of populations.
    - `n_samples` (:py:data:`.tint32`) -- Number of samples.
    - `n_variants` (:py:data:`.tint32`) -- Number of variants.
    - `pop_dist` (:class:`.tarray` of :py:data:`.tfloat64`) -- Population distribution indexed by
      population.
    - `fst` (:class:`.tarray` of :py:data:`.tfloat64`) -- :math:`F_{ST}` values indexed by
      population.
    - `ancestral_af_dist` (:class:`.tstruct`) -- Description of the ancestral allele
      frequency distribution.
    - `seed` (:py:data:`.tint32`) -- Random seed.

    Row fields:

    - `locus` (:class:`.tlocus`) -- Variant locus (key field).
    - `alleles` (:class:`.tarray` of :py:data:`.tstr`) -- Variant alleles (key field).
    - `ancestral_af` (:py:data:`.tfloat64`) -- Ancestral allele frequency.
    - `af` (:class:`.tarray` of :py:data:`.tfloat64`) -- Modern allele frequencies indexed by
      population.

    Column fields:

    - `sample_idx` (:py:data:`.tint32`) - Sample index (key field).
    - `pop` (:py:data:`.tint32`) -- Population of sample.

    Entry fields:

    - `GT` (:py:data:`.tcall`) -- Genotype call (diploid, unphased).

    Parameters
    ----------
    n_populations : :obj:`int`
        Number of modern populations.
    n_samples : :obj:`int`
        Total number of samples.
    n_variants : :obj:`int`
        Number of variants.
    n_partitions : :obj:`int`, optional
        Number of partitions.
        Default is 1 partition per million entries or 8, whichever is larger.
    pop_dist : :obj:`list` of :obj:`float`, optional
        Unnormalized population distribution, a list of length
        ``n_populations`` with non-negative values.
        Default is ``[1, ..., 1]``.
    fst : :obj:`list` of :obj:`float`, optional
        :math:`F_{ST}` values, a list of length ``n_populations`` with values
        in (0, 1). Default is ``[0.1, ..., 0.1]``.
    af_dist : :class:`.UniformDist` or :class:`.BetaDist` or :class:`.TruncatedBetaDist`
        Ancestral allele frequency distribution.
        Default is ``UniformDist(0.1, 0.9)``.
    seed : :obj:`int`
        Random seed.
    reference_genome : :obj:`str` or :class:`.ReferenceGenome`
        Reference genome to use.

    Returns
    -------
    :class:`.MatrixTable`
        Simulated matrix table of variants, samples, and genotypes.
    """

    if pop_dist is None:
        jvm_pop_dist_opt = joption(pop_dist)
    else:
        jvm_pop_dist_opt = joption(jarray(Env.jvm().double, pop_dist))

    if fst is None:
        jvm_fst_opt = joption(fst)
    else:
        jvm_fst_opt = joption(jarray(Env.jvm().double, fst))

    jmt = Env.hc()._jhc.baldingNicholsModel(n_populations, n_samples, n_variants,
                                            joption(n_partitions),
                                            jvm_pop_dist_opt,
                                            jvm_fst_opt,
                                            af_dist._jrep(),
                                            seed,
                                            reference_genome._jrep)
    return MatrixTable(jmt)


class FilterAlleles(object):
    """Filter out a set of alternate alleles.  If all alternate alleles of
    a variant are filtered out, the variant itself is filtered out.
    `filter_expr` is an alternate allele indexed `Array[Boolean]`
    where the booleans, combined with `keep`, determine which
    alternate alleles to filter out.

    This object has bindings for values used in the filter alleles
    process: `old_to_new`, `new_to_old`, `new_locus`, and `new_alleles`.  Call
    :meth:`.FilterAlleles.annotate_rows` and/or
    :meth:`.FilterAlleles.annotate_entries` to update row-indexed and
    row- and column-indexed fields for the new, allele-filtered
    variant.  Finally, call :meth:`.FilterAlleles.filter` to perform
    filter alleles.

    Examples
    --------
    
    Filter alleles with zero AC count on a dataset with the HTS entry
    schema and update the ``info.AC`` and entry fields.

    >>> fa = hl.FilterAlleles(dataset.info.AC.map(lambda AC: AC == 0), keep=False)
    >>> fa.annotate_rows(
    ...     info = dataset.info.annotate(AC = fa.new_to_old[1:].map(lambda i: dataset.info.AC[i - 1])))
    >>> newPL = hl.cond(
    ...     hl.is_defined(dataset.PL),
    ...     hl.range(0, hl.triangle(fa.new_alleles.length())).map(
    ...         lambda newi: hl.bind(
    ...             hl.unphased_diploid_gt_index_call(newi),
    ...             lambda newc: dataset.PL[hl.call(fa.new_to_old[newc[0]], fa.new_to_old[newc[1]]).unphased_diploid_gt_index()])),
    ...     hl.null(hl.tarray(hl.tint32)))
    >>> fa.annotate_entries(
    ...     GT = hl.unphased_diploid_gt_index_call(hl.argmin(newPL, unique=True)),
    ...     AD = hl.cond(
    ...         hl.is_defined(dataset.AD),
    ...         hl.range(0, fa.new_alleles.length()).map(
    ...             lambda newi: dataset.AD[fa.new_to_old[newi]]),
    ...         hl.null(hl.tarray(hl.tint32))),
    ...     GQ = hl.gq_from_pl(newPL),
    ...     PL = newPL)
    >>> filtered_result = fa.filter()
    
    Parameters
    ----------
    filter_expr : :class:`.ArrayBooleanExpression`
        Boolean filter expression.
    keep : bool
        If ``True``, keep alternate alleles where the corresponding
        element of `filter_expr` is ``True``.  If False, remove the
        alternate alleles where the corresponding element is ``True``.
    left_aligned : bool
        If ``True``, variants are assumed to be left aligned and have
        unique loci.  This avoids a shuffle.  If the assumption is
        violated, an error is generated.
    keep_star : bool
        If ``True``, keep variants where the only unfiltered alternate
        alleles are ``*`` alleles.
    """

    @typecheck_method(filter_expr=expr_array(expr_bool), keep=bool, left_aligned=bool, keep_star=bool)
    def __init__(self, filter_expr, keep=True, left_aligned=False, keep_star=False):
        source = filter_expr._indices.source
        if not isinstance(source, MatrixTable):
            raise ValueError("Expect an expression of 'MatrixTable', found {}".format(
                "expression of '{}'".format(source.__class__) if source is not None else 'scalar expression'))
        ds = source
        require_biallelic(ds, 'FilterAlleles')

        analyze('FilterAlleles', filter_expr, ds._row_indices)

        self._ds = ds
        self._filter_expr = filter_expr
        self._keep = keep
        self._left_aligned = left_aligned
        self._keep_star = keep_star
        self._row_exprs = None
        self._entry_exprs = None

        self._old_to_new = construct_reference('oldToNew', tarray(tint32), ds._row_indices)
        self._new_to_old = construct_reference('newToOld', tarray(tint32), ds._row_indices)
        self._new_locus = construct_reference('newLocus', ds['locus'].dtype, ds._row_indices)
        self._new_alleles = construct_reference('newAlleles', ds['alleles'].dtype, ds._row_indices)

    @property
    def new_to_old(self):
        """The array of old allele indices, such that ``new_to_old[newIndex] =
        oldIndex`` and ``new_to_old[0] == 0``. A row-indexed expression.
        
        Returns
        -------
        :class:`.ArrayInt32Expression`
            The array of old indices.
        """
        return self._new_to_old

    @property
    def old_to_new(self):
        """The array of new allele indices. All old filtered alleles have new
        index 0. A row-indexed expression.
        
        Returns
        -------
        :class:`.ArrayInt32Expression`
            The array of new indices.
        """
        return self._old_to_new

    @property
    def new_locus(self):
        """The new locus. A row-indexed expression.

        Returns
        -------
        :class:`.LocusExpression`
        """
        return self._new_locus

    @property
    def new_alleles(self):
        """The new alleles. A row-indexed expression.

        Returns
        -------
        :class:`.ArrayStringExpression`
        """
        return self._new_alleles

    def annotate_rows(self, **named_exprs):
        """Create or update row-indexed fields for the new, allele-filtered
        variant.

        Parameters
        ----------
        named_exprs : keyword args of :class:`.Expression`
            Field names and the row-indexed expressions to compute them.
        """
        if self._row_exprs:
            raise RuntimeError('annotate_rows already called')
        for k, v in named_exprs.items():
            analyze('FilterAlleles', v, self._ds._row_indices)
        self._row_exprs = named_exprs

    def annotate_entries(self, **named_exprs):
        """Create or update row- and column-indexed fields (entry fields) for
        the new, allele-filtered variant.

        Parameters
        ----------
        named_exprs : keyword args of :class:`.Expression`
            Field names and the row- and column-indexed expressions to
            compute them.
        """
        if self._entry_exprs:
            raise RuntimeError('annotate_entries already called')
        for k, v in named_exprs.items():
            analyze('FilterAlleles', v, self._ds._entry_indices)
        self._entry_exprs = named_exprs

    def subset_entries_hts(self):
        """Use the subset algorithm to update the matrix table entries for the
        new, allele-filtered variant.  

        Notes
        -----

        :meth:`.FilterAlleles.subset_entries_hts` requires the dataset
        have the HTS schema, namely the following entry fields:

        .. code-block:: text

            GT: call
            AD: array<int32>
            DP: int32
            GQ: int32
            PL: array<int32>

        **Subset algorithm**

        We will illustrate the behavior on the example genotype below
        when filtering the first alternate allele (allele 1) at a site
        with 1 reference allele and 2 alternate alleles.

        .. code-block:: text

          GT: 1/2
          GQ: 10
          AD: 0,50,35

          0 | 1000
          1 | 1000   10
          2 | 1000   0     20
            +-----------------
               0     1     2

        The subset algorithm subsets the AD and PL arrays
        (i.e. removes entries corresponding to filtered alleles) and
        then sets GT to the genotype with the minimum PL.  Note that
        if the genotype changes (as in the example), the PLs are
        re-normalized (shifted) so that the most likely genotype has a
        PL of 0.  Qualitatively, subsetting corresponds to the belief
        that the filtered alleles are not real so we should discard
        any probability mass associated with them.

        The subset algorithm would produce the following:

        .. code-block:: text

          GT: 1/1
          GQ: 980
          AD: 0,50

          0 | 980
          1 | 980    0
            +-----------
               0      1

        In summary:

         - GT: Set to most likely genotype based on the PLs ignoring
           the filtered allele(s).
         - AD: The filtered alleles' columns are eliminated, e.g.,
           filtering alleles 1 and 2 transforms ``25,5,10,20`` to
           ``25,20``.
         - DP: Unchanged.
         - PL: Columns involving filtered alleles are eliminated and
           the remaining columns' values are shifted so the minimum
           value is 0.
         - GQ: The second-lowest PL (after shifting).
        """
        ds = self._ds
        newPL = hl.cond(
            hl.is_defined(ds.PL),
            hl.bind(hl.range(0, hl.triangle(self.new_alleles.length())).map(
                lambda newi: hl.bind(
                    hl.unphased_diploid_gt_index_call(newi),
                    lambda newc: ds.PL[hl.call(self.new_to_old[newc[0]],
                                               self.new_to_old[newc[1]]).unphased_diploid_gt_index()])),
                lambda unnorm: unnorm - hl.min(unnorm)),
            hl.null(tarray(tint32)))
        self.annotate_entries(
            GT=hl.unphased_diploid_gt_index_call(hl.argmin(newPL, unique=True)),
            AD=hl.cond(
                hl.is_defined(ds.AD),
                hl.range(0, self.new_alleles.length()).map(
                    lambda newi: ds.AD[self.new_to_old[newi]]),
                hl.null(tarray(tint32))),
            # DP unchanged
            GQ=hl.gq_from_pl(newPL),
            PL=newPL)

    def downcode_entries_hts(self):
        """Use the downcode algorithm to update the matrix table entries for
        the new, allele-filtered variant.  

        Notes
        ------

        :meth:`.FilterAlleles.downcode_entries_hts` requires the dataset have the
        HTS schema, namely the following entry fields:

        .. code-block:: text

            GT: call
            AD: array<int32>
            DP: int32
            GQ: int32
            PL: array<int32>

        **Downcode algorithm**

        We will illustrate the behavior on the example genotype below
        when filtering the first alternate allele (allele 1) at a site
        with 1 reference allele and 2 alternate alleles.

        .. code-block:: text

          GT: 1/2
          GQ: 10
          AD: 0,50,35

          0 | 1000
          1 | 1000   10
          2 | 1000   0     20
            +-----------------
               0     1     2

        The downcode algorithm recodes occurances of filtered alleles
        to occurances of the reference allele (e.g. 1 -> 0 in our
        example). So the depths of filtered alleles in the AD field
        are added to the depth of the reference allele. Where
        downcoding filtered alleles merges distinct genotypes, the
        minimum PL is used (since PL is on a log scale, this roughly
        corresponds to adding probabilities). The PLs are then
        re-normalized (shifted) so that the most likely genotype has a
        PL of 0, and GT is set to this genotype.  If an allele is
        filtered, this algorithm acts similarly to
        :func:`.split_multi_hts`.

        The downcode algorithm would produce the following:

        .. code-block:: text

          GT: 0/1
          GQ: 10
          AD: 35,50

          0 | 20
          1 | 0    10
            +-----------
              0    1

        In summary:

         - GT: Downcode filtered alleles to reference.
         - AD: Columns of filtered alleles are eliminated and their
           values are added to the reference column, e.g., filtering
           alleles 1 and 2 transforms ``25,5,10,20`` to ``40,20``.
         - DP: No change.
         - PL: Downcode filtered alleles to reference, combine PLs
           using minimum for each overloaded genotype, and shift so
           the overall minimum PL is 0.
         - GQ: The second-lowest PL (after shifting).
        """
        ds = self._ds
        newPL = hl.cond(
            hl.is_defined(ds.PL),
            (hl.range(0, hl.triangle(hl.len(self.new_alleles)))
                .map(lambda newi: hl.min(hl.range(0, hl.triangle(hl.len(ds.alleles)))
                                         .filter(lambda oldi: hl.bind(
                hl.unphased_diploid_gt_index_call(oldi),
                lambda oldc: hl.call(self.old_to_new[oldc[0]],
                                     self.old_to_new[oldc[1]]) == hl.unphased_diploid_gt_index_call(newi)))
                                         .map(lambda oldi: ds.PL[oldi])))),
            hl.null(tarray(tint32)))
        self.annotate_entries(
            GT=hl.call(self.old_to_new[ds.GT[0]],
                       self.old_to_new[ds.GT[1]]), AD=hl.cond(
                hl.is_defined(ds.AD),
                (hl.range(0, hl.len(self.new_alleles))
                    .map(lambda newi: hl.sum(hl.range(0, hl.len(ds.alleles))
                                             .filter(lambda oldi: self.old_to_new[oldi] == newi)
                                             .map(lambda oldi: ds.AD[oldi])))),
                hl.null(tarray(tint32))),
            # DP unchanged
            GQ=hl.gq_from_pl(newPL),
            PL=newPL)

    def filter(self):
        """Perform the filter alleles, returning a new matrix table.

        Returns
        -------
        :class:`.MatrixTable`
            Returns a matrix table with alleles filtered.
        """
        if not self._row_exprs:
            self._row_exprs = {}
        if not self._entry_exprs:
            self._entry_exprs = {}

        base, cleanup = self._ds._process_joins(*itertools.chain(
            [self._filter_expr], self._row_exprs.values(), self._entry_exprs.values()))

        filter_hql = self._filter_expr._ast.to_hql()

        row_hqls = []
        for k, v in self._row_exprs.items():
            row_hqls.append('va.`{k}` = {v}'.format(k=k, v=v._ast.to_hql()))
            check_collisions(base._fields, k, base._row_indices)
        row_hql = ',\n'.join(row_hqls)

        entry_hqls = []
        for k, v in self._entry_exprs.items():
            entry_hqls.append('g.`{k}` = {v}'.format(k=k, v=v._ast.to_hql()))
            check_collisions(base._fields, k, base._entry_indices)
        entry_hql = ',\n'.join(entry_hqls)

        m = MatrixTable(
            Env.hail().methods.FilterAlleles.apply(
                base._jvds, '({p})[aIndex - 1]'.format(p=filter_hql), row_hql, entry_hql, self._keep,
                self._left_aligned, self._keep_star))
        return cleanup(m)


@typecheck(ds=MatrixTable,
           n_cores=int,
           r2=numeric,
           window=int,
           memory_per_core=int)
def ld_prune(ds, n_cores, r2=0.2, window=1000000, memory_per_core=256):
    jmt = Env.hail().methods.LDPrune.apply(ds._jvds, n_cores, r2, window, memory_per_core)
    return MatrixTable(jmt)


@typecheck(ds=MatrixTable,
           left_aligned=bool)
def min_rep(ds, left_aligned=False):
    """Gives minimal, left-aligned representation of alleles. 

    .. include:: ../_templates/req_tvariant.rst

    Notes
    -----
    Note that this can change the variant position.

    Examples
    --------

    Simple trimming of a multi-allelic site, no change in variant
    position `1:10000:TAA:TAA,AA` => `1:10000:TA:T,A`

    Trimming of a bi-allelic site leading to a change in position
    `1:10000:AATAA,AAGAA` => `1:10002:T:G`

    Parameters
    ----------
    left_aligned : bool
        If ``True``, variants are assumed to be left aligned and have
        unique loci.  This avoids a shuffle.  If the assumption is
        violated, an error is generated.

    Returns
    -------
    :class:`.MatrixTable`
    """
    return MatrixTable(ds._jvds.minRep(left_aligned))
