package is.hail.stats

import breeze.linalg._
import is.hail.annotations.Annotation
import is.hail.expr._
import is.hail.expr.types._
import is.hail.utils._
import is.hail.variant.{Call, HardCallView, MatrixTable}

object RegressionUtils {
  def inputVector(x: DenseVector[Double],
    globalAnnotation: Annotation, sampleAnnotations: IndexedSeq[Annotation],
    row: Annotation,
    entries: IndexedSeq[Annotation],
    ec: EvalContext,
    xf: () => java.lang.Double,
    completeSampleIndex: Array[Int],
    missingSamples: ArrayBuilder[Int]) {
    require(x.length == completeSampleIndex.length)

    ec.set(0, globalAnnotation)
    ec.set(1, row)

    missingSamples.clear()
    val n = completeSampleIndex.length
    val git = entries.iterator
    var i = 0
    var j = 0
    var sum = 0d
    while (j < n) {
      while (i < completeSampleIndex(j)) {
        git.next()
        i += 1
      }
      assert(completeSampleIndex(j) == i)

      val g = git.next()
      ec.set(2, sampleAnnotations(i))
      ec.set(3, g)
      val e = xf()
      if (e != null) {
        sum += e
        x(j) = e
      } else
        missingSamples += j
      i += 1
      j += 1
    }

    val nMissing = missingSamples.size
    val meanValue = sum / (n - nMissing)
    i = 0
    while (i < nMissing) {
      x(missingSamples(i)) = meanValue
      i += 1
    }
  }

  // !useHWE: mean 0, norm exactly sqrt(n), variance 1
  // useHWE: mean 0, norm approximately sqrt(m), variance approx. m / n
  // missing gt are mean imputed, constant variants return None, only HWE uses nVariants
  def normalizedHardCalls(view: HardCallView, nSamples: Int, useHWE: Boolean = false, nVariants: Int = -1): Option[Array[Double]] = {
    require(!(useHWE && nVariants == -1))
    val vals = Array.ofDim[Double](nSamples)
    var nMissing = 0
    var sum = 0
    var sumSq = 0

    var row = 0
    while (row < nSamples) {
      view.setGenotype(row)
      if (view.hasGT) {
        val gt = Call.unphasedDiploidGtIndex(view.getGT)
        vals(row) = gt
        (gt: @unchecked) match {
          case 0 =>
          case 1 =>
            sum += 1
            sumSq += 1
          case 2 =>
            sum += 2
            sumSq += 4
        }
      } else {
        vals(row) = -1
        nMissing += 1
      }
      row += 1
    }

    val nPresent = nSamples - nMissing
    val nonConstant = !(sum == 0 || sum == 2 * nPresent || sum == nPresent && sumSq == nPresent)

    if (nonConstant) {
      val mean = sum.toDouble / nPresent
      val stdDev = math.sqrt(
        if (useHWE)
          mean * (2 - mean) * nVariants / 2
        else {
          val meanSq = (sumSq + nMissing * mean * mean) / nSamples
          meanSq - mean * mean
        })

      val gtDict = Array(0, -mean / stdDev, (1 - mean) / stdDev, (2 - mean) / stdDev)
      var i = 0
      while (i < nSamples) {
        vals(i) = gtDict(vals(i).toInt + 1)
        i += 1
      }

      Some(vals)
    } else
      None
  }

  def parseFloat64Expr(expr: String, ec: EvalContext): () => java.lang.Double = {
    val (xt, xf0) = Parser.parseExpr(expr, ec)
    assert(xt.isOfType(TFloat64()))
    () => xf0().asInstanceOf[java.lang.Double]
  }

  // IndexedSeq indexed by samples, Array by annotations
  def getSampleAnnotations(vds: MatrixTable, annots: Array[String], ec: EvalContext): IndexedSeq[Array[Option[Double]]] = {
    val aQs = annots.map(parseFloat64Expr(_, ec))

    vds.colValues.map { sa =>
      ec.set(0, sa)
      aQs.map { aQ =>
        val a = aQ()
        if (a != null)
          Some(a: Double)
        else
          None
      }
    }
  }

  def getPhenoCovCompleteSamples(
    vsm: MatrixTable,
    yExpr: String,
    covExpr: Array[String]): (DenseVector[Double], DenseMatrix[Double], Array[Int]) = {
    
    val (y, covs, completeSamples) = getPhenosCovCompleteSamples(vsm, Array(yExpr), covExpr)
    
    (DenseVector(y.data), covs, completeSamples)
  }
  
  def getPhenosCovCompleteSamples(
    vsm: MatrixTable,
    yExpr: Array[String],
    covExpr: Array[String]): (DenseMatrix[Double], DenseMatrix[Double], Array[Int]) = {

    val nPhenos = yExpr.length
    val nCovs = covExpr.length + 1 // intercept

    if (nPhenos == 0)
      fatal("No phenotypes present.")

    val symTab = Map(
      "sa" -> (0, vsm.colType))

    val ec = EvalContext(symTab)

    val yIS = getSampleAnnotations(vsm, yExpr, ec)
    val covIS = getSampleAnnotations(vsm, covExpr, ec)

    val (yForCompleteSamples, covForCompleteSamples, completeSamples) =
      (yIS, covIS, 0 until vsm.numCols)
        .zipped
        .filter((y, c, s) => y.forall(_.isDefined) && c.forall(_.isDefined))

    val n = completeSamples.size
    if (n == 0)
      fatal("No complete samples: each sample is missing its phenotype or some covariate")

    val yArray = yForCompleteSamples.flatMap(_.map(_.get)).toArray
    val y = new DenseMatrix(rows = n, cols = nPhenos, data = yArray, offset = 0, majorStride = nPhenos, isTranspose = true)

    val covArray = covForCompleteSamples.flatMap(1.0 +: _.map(_.get)).toArray
    val cov = new DenseMatrix(rows = n, cols = nCovs, data = covArray, offset = 0, majorStride = nCovs, isTranspose = true)

    if (n < vsm.numCols)
      warn(s"${ vsm.numCols - n } of ${ vsm.numCols } samples have a missing phenotype or covariate.")

    (y, cov, completeSamples.toArray)
  }
}
