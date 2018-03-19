package is.hail

import java.net.URI
import java.nio.file.{Files, Paths}

import breeze.linalg.{DenseMatrix, Matrix, Vector}
import is.hail.methods.SplitMulti
import is.hail.table.Table
import is.hail.utils._
import is.hail.testUtils._
import is.hail.variant._
import org.apache.spark.SparkException

object TestUtils {

  import org.scalatest.Assertions._

  def interceptFatal(regex: String)(f: => Any) {
    val thrown = intercept[HailException](f)
    val p = regex.r.findFirstIn(thrown.getMessage).isDefined
    if (!p)
      println(
        s"""expected fatal exception with pattern `$regex'
           |  Found: ${ thrown.getMessage } """.stripMargin)
    assert(p)
  }

  def interceptSpark(regex: String)(f: => Any) {
    val thrown = intercept[SparkException](f)
    val p = regex.r.findFirstIn(thrown.getMessage).isDefined
    if (!p)
      println(
        s"""expected fatal exception with pattern `$regex'
           |  Found: ${ thrown.getMessage } """.stripMargin)
    assert(p)
  }

  def interceptAssertion(regex: String)(f: => Any) {
    val thrown = intercept[AssertionError](f)
    val p = regex.r.findFirstIn(thrown.getMessage).isDefined
    if (!p)
      println(
        s"""expected assertion error with pattern `$regex'
           |  Found: ${ thrown.getMessage } """.stripMargin)
    assert(p)
  }

  def assertVectorEqualityDouble(A: Vector[Double], B: Vector[Double], tolerance: Double = utils.defaultTolerance) {
    assert(A.size == B.size)
    assert((0 until A.size).forall(i => D_==(A(i), B(i), tolerance)))
  }

  def assertMatrixEqualityDouble(A: Matrix[Double], B: Matrix[Double], tolerance: Double = utils.defaultTolerance) {
    assert(A.rows == B.rows)
    assert(A.cols == B.cols)
    assert((0 until A.rows).forall(i => (0 until A.cols).forall(j => D_==(A(i, j), B(i, j), tolerance))))
  }

  def isConstant(A: Vector[Int]): Boolean = {
    (0 until A.length - 1).foreach(i => if (A(i) != A(i + 1)) return false)
    true
  }

  def removeConstantCols(A: DenseMatrix[Int]): DenseMatrix[Int] = {
    val data = (0 until A.cols).flatMap { j =>
      val col = A(::, j)
      if (TestUtils.isConstant(col))
        Array[Int]()
      else
        col.toArray
    }.toArray

    val newCols = data.length / A.rows
    new DenseMatrix(A.rows, newCols, data)
  }

  // missing is -1
  def vdsToMatrixInt(vds: MatrixTable): DenseMatrix[Int] =
    new DenseMatrix[Int](
      vds.numCols,
      vds.countRows().toInt,
      vds.typedRDD[Locus].map(_._2._2.map { g =>
        Genotype.call(g)
          .map(Call.nNonRefAlleles)
          .getOrElse(-1)
      }).collect().flatten)

  // missing is Double.NaN
  def vdsToMatrixDouble(vds: MatrixTable): DenseMatrix[Double] =
    new DenseMatrix[Double](
      vds.numCols,
      vds.countRows().toInt,
      vds.rdd.map(_._2._2.map { g =>
        Genotype.call(g)
          .map(Call.nNonRefAlleles)
          .map(_.toDouble)
          .getOrElse(Double.NaN)
      }).collect().flatten)

  def unphasedDiploidGtIndicesToBoxedCall(m: DenseMatrix[Int]): DenseMatrix[BoxedCall] = {
    m.map(g => if (g == -1) null: BoxedCall else Call2.fromUnphasedDiploidGtIndex(g): BoxedCall)
  }

  def indexedSeqBoxedDoubleEquals(tol: Double)
    (xs: IndexedSeq[java.lang.Double], ys: IndexedSeq[java.lang.Double]): Boolean =
    (xs, ys).zipped.forall { case (x, y) =>
      if (x == null || y == null)
        x == null && y == null
      else
        D_==(x.doubleValue(), y.doubleValue(), tolerance = tol)
    }

  def keyTableBoxedDoubleToMap[T](kt: Table): Map[T, IndexedSeq[java.lang.Double]] =
    kt.collect().map { r =>
      val s = r.toSeq
      s.head.asInstanceOf[T] -> s.tail.map(_.asInstanceOf[java.lang.Double]).toIndexedSeq
    }.toMap

  def matrixToString(A: DenseMatrix[Double], separator: String): String = {
    val sb = new StringBuilder
    for (i <- 0 until A.rows) {
      for (j <- 0 until A.cols) {
        if (j == (A.cols - 1))
          sb.append(A(i, j))
        else {
          sb.append(A(i, j))
          sb.append(separator)
        }
      }
      sb += '\n'
    }
    sb.result()
  }

  def fileHaveSameBytes(file1: String, file2: String): Boolean =
    Files.readAllBytes(Paths.get(URI.create(file1))) sameElements Files.readAllBytes(Paths.get(URI.create(file2)))

  def splitMultiHTS(mt: MatrixTable): MatrixTable = {
    if (!mt.entryType.isOfType(Genotype.htsGenotypeType))
      fatal(s"split_multi: genotype_schema must be the HTS genotype schema, found: ${ mt.entryType }")
    val pl = """if (isDefined(g.PL))
    range(3).map(i => range(g.PL.size()).filter(j => downcode(UnphasedDiploidGtIndexCall(j), aIndex) == UnphasedDiploidGtIndexCall(i)).map(j => g.PL[j]).min())
    else
    NA: Array[Int]"""
    SplitMulti(mt, "va.aIndex = aIndex, va.wasSplit = wasSplit",
      s"""g.GT = downcode(g.GT, aIndex),
      g.AD = if (isDefined(g.AD))
          let sum = g.AD.sum() and adi = g.AD[aIndex] in [sum - adi, adi]
        else
          NA: Array[Int],
          g.DP = g.DP,
      g.PL = $pl,
      g.GQ = gqFromPL($pl)""")
  }
}
