package is.hail.stats

import is.hail.SparkSuite
import is.hail.check.Prop._
import is.hail.check.Properties
import is.hail.io.vcf.ExportVCF
import is.hail.methods.VariantQC
import is.hail.utils._
import is.hail.testUtils._
import is.hail.variant._
import org.testng.annotations.Test

import scala.language._
import scala.sys.process._


class InbreedingCoefficientSuite extends SparkSuite {

  def parsePlinkHet(file: String): Map[String, (Option[Double], Option[Long], Option[Double], Option[Long])] =
    hadoopConf.readLines(file)(_.drop(1).map(_.map { line =>
      val Array(fid, iid, obsHom, expHom, numCalled, f) = line.trim.split("\\s+")
      val fMod = f match {
        case "nan" => None
        case x: String => Option(x.toDouble)
        case _ => throw new IllegalArgumentException
      }

      (iid, (fMod, Option(obsHom.toLong), Option(expHom.toDouble), Option(numCalled.toLong)))
    }.value
    ).toMap)

  object Spec extends Properties("InbreedingCoefficient") {

    val plinkSafeBiallelicVDS = MatrixTable.gen(hc, VSMSubgen.plinkSafeBiallelic)
      .resize(1000)
      .map { vds =>
        vds.filterRowsExpr("va.locus.isAutosomal()")
      }
      .filter(vds => vds.countRows > 2 && vds.numCols >= 2)

    property("hail generates same results as PLINK v1.9") =
      forAll(plinkSafeBiallelicVDS) { case (vds: MatrixTable) =>

        val vds2 = VariantQC(vds)
          .filterRowsExpr("va.qc.AC > 1 && va.qc.AF >= 1e-8 && va.qc.n_called * 2 - va.qc.AC > 1 && va.qc.AF <= 1d - 1e-8")

        if (vds2.numCols < 5 || vds2.countRows() < 5) {
          true
        } else {
          val localRoot = tmpDir.createLocalTempFile("ibcCheck")
          val localVCFFile = localRoot + ".vcf"
          val localIbcFile = localRoot + ".het"

          val root = tmpDir.createTempFile("ibcCheck")
          val vcfFile = root + ".vcf"
          val ibcFile = root + ".het"

          val vds3 = vds2.annotateColsExpr("het = AGG.map(g => g.GT).inbreeding(g => va.qc.AF)")

          ExportVCF(vds3, vcfFile)

          hadoopConf.copy(vcfFile, localVCFFile)

          s"plink --vcf ${ uriPath(localVCFFile) } --allow-extra-chr --const-fid --het --silent --out ${ uriPath(localRoot) }" !

          hadoopConf.copy(localIbcFile, ibcFile)

          val plinkResult = parsePlinkHet(ibcFile)

          val (_, fQuery) = vds3.querySA("sa.het.f_stat")
          val (_, obsHomQuery) = vds3.querySA("sa.het.observed_homs")
          val (_, expHomQuery) = vds3.querySA("sa.het.expected_homs")
          val (_, nCalledQuery) = vds3.querySA("sa.het.n_called")

          val hailResult = vds3.stringSampleIds.zip(vds3.colValues).map { case (sample, sa) =>
            (sample, (Option(fQuery(sa)).map(_.asInstanceOf[Double]), Option(obsHomQuery(sa)).map(_.asInstanceOf[Long]), Option(expHomQuery(sa)).map(_.asInstanceOf[Double]), Option(nCalledQuery(sa)).map(_.asInstanceOf[Long])))
          }.toMap

          assert(plinkResult.keySet == hailResult.keySet)

          val result = plinkResult.forall { case (sample, (plinkF, plinkObsHom, plinkExpHom, plinkNCalled)) =>

            val (hailF, hailObsHom, hailExpHom, hailNCalled) = hailResult(sample)

            val resultF = plinkF.liftedZip(hailF).forall { case (p, h) => math.abs(p - h) < 1e-2 }
            val resultObsHom = plinkObsHom.liftedZip(hailObsHom).forall { case (p, h) => p == h }
            val resultExpHom = plinkExpHom.liftedZip(hailExpHom).forall { case (p, h) => math.abs(p - h) < 1e-2 }
            //plink only gives two decimal places
            val resultNCalled = plinkNCalled.liftedZip(hailNCalled).forall { case (p, h) => p == h }

            if (resultF && resultObsHom && resultExpHom && resultNCalled)
              true
            else {
              println(s"$sample plink=${
                plinkF.liftedZip(plinkObsHom).liftedZip(plinkExpHom).liftedZip(plinkNCalled).getOrElse("NA")
              } hail=${
                hailF.liftedZip(hailObsHom).liftedZip(hailExpHom).liftedZip(hailNCalled).getOrElse("NA")
              } $resultF $resultObsHom $resultExpHom $resultNCalled")
              false
            }
          }

          result
        }
      }
  }

  @Test def testIbcPlinkVersion() {
    Spec.check()
  }

  @Test def signatureIsCorrect() {
    assert(InbreedingCombiner.signature.typeCheck((new InbreedingCombiner()).asAnnotation))
  }
}

