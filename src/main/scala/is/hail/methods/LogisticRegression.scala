package is.hail.methods

import breeze.linalg._
import is.hail.annotations._
import is.hail.expr._
import is.hail.expr.types._
import is.hail.stats._
import is.hail.utils._
import is.hail.variant._
import org.apache.spark.rdd.RDD

object LogisticRegression {

  def apply(vsm: MatrixTable,
    test: String,
    yExpr: String,
    xExpr: String,
    covExpr: Array[String],
    root: String): MatrixTable = {
    val logRegTest = LogisticRegressionTest.tests(test)

    val ec = vsm.matrixType.genotypeEC
    val xf = RegressionUtils.parseFloat64Expr(xExpr, ec)

    val (y, cov, completeSampleIndex) = RegressionUtils.getPhenoCovCompleteSamples(vsm, yExpr, covExpr)

    if (!y.forall(yi => yi == 0d || yi == 1d))
      fatal(s"For logistic regression, phenotype must be bool or numeric with all present values equal to 0 or 1")

    val n = y.size
    val k = cov.cols
    val d = n - k - 1

    if (d < 1)
      fatal(s"$n samples and ${ k + 1 } ${ plural(k, "covariate") } (including x and intercept) implies $d degrees of freedom.")

    info(s"logistic_regression: running $test logistic regression on $n samples for response variable y,\n"
      + s"    with input variable x, intercept, and ${ k - 1 } additional ${ plural(k - 1, "covariate") }...")

    val nullModel = new LogisticRegressionModel(cov, y)
    var nullFit = nullModel.fit()

    if (!nullFit.converged)
      if (logRegTest == FirthTest)
        nullFit = LogisticRegressionFit(nullModel.bInterceptOnly(),
          None, None, 0, nullFit.nIter, exploded = nullFit.exploded, converged = false)
      else
        fatal("Failed to fit logistic regression null model (standard MLE with covariates only): " + (
          if (nullFit.exploded)
            s"exploded at Newton iteration ${ nullFit.nIter }"
          else
            "Newton iteration failed to converge"))

    val sc = vsm.sparkContext

    val localGlobalAnnotationBc = vsm.globals.broadcast
    val sampleAnnotationsBc = vsm.colValuesBc

    val completeSampleIndexBc = sc.broadcast(completeSampleIndex)
    val yBc = sc.broadcast(y)
    val XBc = sc.broadcast(new DenseMatrix[Double](n, k + 1, cov.toArray ++ Array.ofDim[Double](n)))
    val nullFitBc = sc.broadcast(nullFit)
    val logRegTestBc = sc.broadcast(logRegTest)

    val (newRVType, inserter) = vsm.rvRowType.unsafeStructInsert(logRegTest.schema, List(root))

    val fullRowType = vsm.rvRowType
    val localRowType = vsm.rowType
    val localEntriesIndex = vsm.entriesIndex

    val newMatrixType = vsm.matrixType.copy(rvRowType = newRVType)

    val newRVD = vsm.rvd.mapPartitionsPreservesPartitioning(newMatrixType.orvdType) { it =>
      val rvb = new RegionValueBuilder()
      val rv2 = RegionValue()

      val missingSamples = new ArrayBuilder[Int]()

      val fullRow = new UnsafeRow(fullRowType)
      val row = fullRow.deleteField(localEntriesIndex)

      val X = XBc.value.copy
      it.map { rv =>
        fullRow.set(rv)

        RegressionUtils.inputVector(X(::, -1),
          localGlobalAnnotationBc.value, sampleAnnotationsBc.value, row,
          fullRow.getAs[IndexedSeq[Annotation]](localEntriesIndex),
          ec, xf,
          completeSampleIndexBc.value, missingSamples)

        val logregAnnot = logRegTestBc.value.test(X, yBc.value, nullFitBc.value).toAnnotation

        rvb.set(rv.region)
        rvb.start(newRVType)
        inserter(rv.region, rv.offset, rvb,
          () => rvb.addAnnotation(logRegTest.schema, logregAnnot))

        rv2.set(rv.region, rvb.end())
        rv2
      }
    }

    vsm.copyMT(matrixType = newMatrixType,
      rvd = newRVD)
  }
}
