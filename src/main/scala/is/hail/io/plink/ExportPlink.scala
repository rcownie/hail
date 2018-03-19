package is.hail.io.plink

import is.hail.expr.{EvalContext, Parser}
import is.hail.expr.types._
import is.hail.variant.MatrixTable
import is.hail.utils._

object ExportPlink {
  def apply(vsm: MatrixTable, path: String, famExpr: String = "id = sa.s") {
    vsm.requireColKeyString("export_plink")

    val ec = EvalContext(Map(
      "sa" -> (0, vsm.colType),
      "global" -> (1, vsm.globalType)))

    ec.set(1, vsm.globals.value)

    type Formatter = (Option[Any]) => String

    val formatID: Formatter = _.map(_.asInstanceOf[String]).getOrElse("0")
    val formatIsFemale: Formatter = _.map { a =>
      if (a.asInstanceOf[Boolean])
        "2"
      else
        "1"
    }.getOrElse("0")
    val formatIsCase: Formatter = _.map { a =>
      if (a.asInstanceOf[Boolean])
        "2"
      else
        "1"
    }.getOrElse("NA")
    val formatQPheno: Formatter = a => a.map(_.toString).getOrElse("NA")

    val famColumns: Map[String, (Type, Int, Formatter)] = Map(
      "fam_id" -> (TString(), 0, formatID),
      "id" -> (TString(), 1, formatID),
      "pat_id" -> (TString(), 2, formatID),
      "mat_id" -> (TString(), 3, formatID),
      "is_female" -> (TBoolean(), 4, formatIsFemale),
      "quant_pheno" -> (TFloat64(), 5, formatQPheno),
      "is_case" -> (TBoolean(), 5, formatIsCase))

    val (names, types, f) = Parser.parseNamedExprs(famExpr, ec)

    val famFns: Array[(Array[Option[Any]]) => String] = Array(
      _ => "0", _ => "0", _ => "0", _ => "0", _ => "NA", _ => "NA")

    (names.zipWithIndex, types).zipped.foreach { case ((name, i), t) =>
      famColumns.get(name) match {
        case Some((colt, j, formatter)) =>
          if (colt != t)
            fatal(s"invalid type for .fam file column $i: expected $colt, got $t")
          famFns(j) = (a: Array[Option[Any]]) => formatter(a(i))

        case None =>
          fatal(s"no .fam file column $name")
      }
    }

    val spaceRegex = """\s+""".r
    val badSampleIds = vsm.stringSampleIds.filter(id => spaceRegex.findFirstIn(id).isDefined)
    if (badSampleIds.nonEmpty) {
      fatal(
        s"""Found ${ badSampleIds.length } sample IDs with whitespace
           |  Fix this problem before exporting to plink format
           |  Bad sample IDs: @1 """.stripMargin, badSampleIds)
    }

    val bedHeader = Array[Byte](108, 27, 1)

    // FIXME: don't reevaluate the upstream RDD twice
    vsm.rvd.mapPartitions(
      ExportBedBimFam.bedRowTransformer(vsm.numCols, vsm.rvd.typ.rowType)
    ).saveFromByteArrays(path + ".bed", vsm.hc.tmpDir, header = Some(bedHeader))

    vsm.rvd.mapPartitions(
      ExportBedBimFam.bimRowTransformer(vsm.rvd.typ.rowType)
    ).writeTable(path + ".bim", vsm.hc.tmpDir)

    val famRows = vsm
      .colValues
      .map { sa =>
        ec.set(0, sa)
        val a = f().map(Option(_))
        famFns.map(_ (a)).mkString("\t")
      }

    vsm.hc.hadoopConf.writeTextFile(path + ".fam")(out =>
      famRows.foreach(line => {
        out.write(line)
        out.write("\n")
      }))
  }
}
