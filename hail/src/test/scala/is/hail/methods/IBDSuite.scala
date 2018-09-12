package is.hail.methods

import is.hail.SparkSuite
import is.hail.annotations.Annotation
import is.hail.check.Prop._
import is.hail.check.{Gen, Properties}
import is.hail.expr.types._
import is.hail.io.vcf.ExportVCF
import is.hail.utils.AbsoluteFuzzyComparable._
import is.hail.utils.{AbsoluteFuzzyComparable, TextTableReader, _}
import is.hail.variant._
import org.apache.spark.sql.Row
import org.testng.annotations.Test

import scala.language._
import scala.sys.process._

class IBDSuite extends SparkSuite {

  def toI(a: Any): Int =
    a.asInstanceOf[Int]

  def toD(a: Any): Double =
    a.asInstanceOf[Double]

  def toS(a: Any): String =
    a.asInstanceOf[String]

  implicit object ibdAbsoluteFuzzyComparable extends AbsoluteFuzzyComparable[IBDInfo] {
    def absoluteEq(tolerance: Double, x: IBDInfo, y: IBDInfo) = {
      def feq(x: Double, y: Double) = AbsoluteFuzzyComparable.absoluteEq(tolerance, x, y)

      def NaNorFeq(x: Double, y: Double) =
        x.isNaN && y.isNaN || feq(x, y)

      NaNorFeq(x.Z0, y.Z0) && NaNorFeq(x.Z1, y.Z1) && NaNorFeq(x.Z2, y.Z2) && NaNorFeq(x.PI_HAT, y.PI_HAT)
    }
  }

  implicit object eibdAbsoluteFuzzyComparable extends AbsoluteFuzzyComparable[ExtendedIBDInfo] {
    def absoluteEq(tolerance: Double, x: ExtendedIBDInfo, y: ExtendedIBDInfo) = {
      def feq(x: Double, y: Double) = AbsoluteFuzzyComparable.absoluteEq(tolerance, x, y)

      AbsoluteFuzzyComparable.absoluteEq(tolerance, x.ibd, y.ibd) &&
        feq(x.ibs0, y.ibs0) && feq(x.ibs1, y.ibs1) && feq(x.ibs2, y.ibs2)
    }
  }

  private def runPlinkIBD(vds: MatrixTable,
    min: Option[Double] = None,
    max: Option[Double] = None,
    maf: Option[Double] = None): Map[(Annotation, Annotation), ExtendedIBDInfo] = {

    val tmpdir = tmpDir.createTempFile(prefix = "plinkIBD")
    val localTmpdir = tmpDir.createLocalTempFile(prefix = "plinkIBD")

    val vcfFile = tmpdir + ".vcf"
    val localVCFFile = localTmpdir + ".vcf"

    ExportVCF(vds, vcfFile)

    hadoopConf.copy(vcfFile, localVCFFile)

    val thresholdString = min.map(x => s" --min $x").getOrElse("") +
      max.map(x => s" --max $x").getOrElse("") +
      maf.map(x => s" --maf $x").getOrElse("")

    s"plink --double-id --allow-extra-chr --vcf ${ uriPath(localVCFFile) } --genome full --out ${ uriPath(localTmpdir) } " + thresholdString !

    val genomeFile = tmpdir + ".genome"
    val localGenomeFile = localTmpdir + ".genome"

    hadoopConf.copy(localGenomeFile, genomeFile)

    val rdd = TextTableReader.read(hc)(Array(tmpdir + ".genome"),
      types = Map(("IID1", TString()), ("IID2", TString()), ("Z0", TFloat64()), ("Z1", TFloat64()), ("Z2", TFloat64()),
        ("PI_HAT", TFloat64()), ("IBS0", TInt32()), ("IBS1", TInt32()), ("IBS2", TInt32())),
      separator = " +"
    ).rdd

    rdd.collect()
      .map { row =>
        // _, fid1, iid1, fid2, iid2, rt, ez, z0, z1, z2, pihat, phe, dst, ppc, ratio, ibs0, ibs1, ibs2, homhom, hethet
        val iid1 = toS(row(2)): Annotation
        val iid2 = toS(row(4)): Annotation
        val z0 = toD(row(7))
        val z1 = toD(row(8))
        val z2 = toD(row(9))
        val pihat = toD(row(10))
        val ibs0 = toI(row(15))
        val ibs1 = toI(row(16))
        val ibs2 = toI(row(17))
        ((iid1, iid2), ExtendedIBDInfo(IBDInfo(z0, z1, z2, pihat), ibs0, ibs1, ibs2))
      }
      // if min or max is enabled, we remove NaNs, plink does not
      .filter { case (_, eibd) => (min.isEmpty && max.isEmpty) || !eibd.hasNaNs }
      .toMap
  }

  // plink rounds to the nearest ten-thousandth
  val tolerance = 5e-5

  @Test def ibdPlinkSameOnRealVCF() {
    val vds = hc.importVCF("src/test/resources/sample.vcf")

    val us = IBD.toRDD(IBD(vds)).collect().toMap

    val plink = runPlinkIBD(vds)
    val sampleIds = vds.stringSampleIds

    assert(mapSameElements(us, plink,
      (x: ExtendedIBDInfo, y: ExtendedIBDInfo) => AbsoluteFuzzyComparable.absoluteEq(tolerance, x, y)))
  }

  @Test def ibdSchemaCorrect() {
    val vds = hc.importVCF("src/test/resources/sample.vcf")
    val us = IBD(vds).typeCheck()
  }
}
