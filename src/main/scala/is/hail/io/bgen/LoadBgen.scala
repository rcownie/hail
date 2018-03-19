package is.hail.io.bgen

import is.hail.HailContext
import is.hail.annotations._
import is.hail.expr.types._
import is.hail.io.vcf.LoadVCF
import is.hail.io.{HadoopFSDataBinaryReader, IndexBTree}
import is.hail.rvd.OrderedRVD
import is.hail.utils._
import is.hail.variant._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.io.Source

case class BgenHeader(compressed: Boolean, nSamples: Int, nVariants: Int,
  headerLength: Int, dataStart: Int, hasIds: Boolean, version: Int)

case class BgenResult[T <: BgenRecord](file: String, nSamples: Int, nVariants: Int, rdd: RDD[(LongWritable, T)])

object LoadBgen {

  def load(hc: HailContext,
    files: Array[String],
    sampleFile: Option[String] = None,
    includeGT: Boolean,
    includeGP: Boolean,
    includeDosage: Boolean,
    nPartitions: Option[Int] = None,
    rg: Option[ReferenceGenome] = Some(ReferenceGenome.defaultReference),
    contigRecoding: Map[String, String] = Map.empty[String, String],
    tolerance: Double): MatrixTable = {

    require(files.nonEmpty)
    val hadoop = hc.hadoopConf

    val sampleIds = sampleFile.map(file => LoadBgen.readSampleFile(hadoop, file))
      .getOrElse(LoadBgen.readSamples(hadoop, files.head))

    LoadVCF.warnDuplicates(sampleIds)

    val nSamples = sampleIds.length

    hadoop.setDouble("tolerance", tolerance)
    hadoop.setBoolean("includeGT", includeGT)
    hadoop.setBoolean("includeGP", includeGP)
    hadoop.setBoolean("includeDosage", includeDosage)

    val sc = hc.sc
    val results = files.map { file =>
      val bState = readState(sc.hadoopConfiguration, file)

      bState.version match {
        case 1 =>
          BgenResult(file, bState.nSamples, bState.nVariants,
            sc.hadoopFile(file, classOf[BgenInputFormatV11], classOf[LongWritable], classOf[BgenRecordV11], nPartitions.getOrElse(sc.defaultMinPartitions)))
        case 2 =>
          BgenResult(file, bState.nSamples, bState.nVariants,
            sc.hadoopFile(file, classOf[BgenInputFormatV12], classOf[LongWritable], classOf[BgenRecordV12], nPartitions.getOrElse(sc.defaultMinPartitions)))
        case x => fatal(s"Hail does not support BGEN v1.$x.")
      }
    }

    val unequalSamples = results.filter(_.nSamples != nSamples).map(x => (x.file, x.nSamples))
    if (unequalSamples.length > 0)
      fatal(
        s"""The following BGEN files did not contain the expected number of samples $nSamples:
           |  ${ unequalSamples.map(x => s"""(${ x._2 } ${ x._1 }""").mkString("\n  ") }""".stripMargin)

    val noVariants = results.filter(_.nVariants == 0).map(_.file)
    if (noVariants.length > 0)
      fatal(
        s"""The following BGEN files did not contain at least 1 variant:
           |  ${ noVariants.mkString("\n  ") })""".stripMargin)

    val nVariants = results.map(_.nVariants).sum

    info(s"Number of BGEN files parsed: ${ results.length }")
    info(s"Number of samples in BGEN files: $nSamples")
    info(s"Number of variants across all BGEN files: $nVariants")

    val signature = TStruct("locus" -> TLocus.schemaFromRG(rg),
      "alleles" -> TArray(TString()),
      "rsid" -> TString(),
      "varid" -> TString())

    val entryFields = Array(
      (includeGT, "GT" -> TCall()),
      (includeGP, "GP" -> TArray(TFloat64())),
      (includeDosage, "dosage" -> TFloat64()))
      .withFilter(_._1).map(_._2)

    val matrixType: MatrixType = MatrixType.fromParts(
      globalType = TStruct.empty(),
      colKey = Array("s"),
      colType = TStruct("s" -> TString()),
      rowType = signature,
      rowKey = Array("locus", "alleles"),
      rowPartitionKey = Array("locus"),
      entryType = TStruct(entryFields: _*))

    val kType = matrixType.orvdType.kType
    val rowType = matrixType.rvRowType

    val fastKeys = sc.union(results.map(_.rdd.mapPartitions { it =>
      val region = Region()
      val rvb = new RegionValueBuilder(region)
      val rv = RegionValue(region)

      it.map { case (_, record) =>
        val (contig, pos, alleles) = record.getKey
        val contigRecoded = contigRecoding.getOrElse(contig, contig)

        region.clear()
        rvb.start(kType)
        rvb.startStruct()
        rvb.addAnnotation(kType.types(0), Locus.annotation(contigRecoded, pos, rg))

        val nAlleles = alleles.length
        rvb.startArray(nAlleles)
        var i = 0
        while (i < nAlleles) {
          rvb.addString(alleles(i))
          i += 1
        }
        rvb.endArray()
        rvb.endStruct()

        rv.setOffset(rvb.end())
        rv
      }
    }))

    val rdd2 = sc.union(results.map(_.rdd.mapPartitions { it =>
      val region = Region()
      val rvb = new RegionValueBuilder(region)
      val rv = RegionValue(region)

      it.map { case (_, record) =>
        val (contig, pos, alleles) = record.getKey
        val va = record.getAnnotation.asInstanceOf[Row]

        val contigRecoded = contigRecoding.getOrElse(contig, contig)

        region.clear()
        rvb.start(rowType)
        rvb.startStruct()
        rvb.addAnnotation(kType.types(0), Locus.annotation(contigRecoded, pos, rg))

        val nAlleles = alleles.length
        rvb.startArray(nAlleles)
        var i = 0
        while (i < nAlleles) {
          rvb.addString(alleles(i))
          i += 1
        }
        rvb.endArray()
        rvb.addAnnotation(rowType.types(2), va.get(0))
        rvb.addAnnotation(rowType.types(3), va.get(1))
        record.getValue(rvb) // gs
        rvb.endStruct()

        rv.setOffset(rvb.end())
        rv
      }
    }))

    new MatrixTable(hc, matrixType,
      BroadcastValue(Annotation.empty, matrixType.globalType, sc),
      sampleIds.map(x => Annotation(x)),
      OrderedRVD(matrixType.orvdType, rdd2, Some(fastKeys), None))
  }

  def index(hConf: org.apache.hadoop.conf.Configuration, file: String) {
    val indexFile = file + ".idx"

    val bState = readState(hConf, file)

    val dataBlockStarts = new Array[Long](bState.nVariants + 1)
    var position: Long = bState.dataStart

    dataBlockStarts(0) = position

    hConf.readFile(file) { is =>
      val reader = new HadoopFSDataBinaryReader(is)
      reader.seek(0)

      for (i <- 1 to bState.nVariants) {
        reader.seek(position)

        if (bState.version == 1)
          reader.readInt() // nRows for v1.1 only

        val snpid = reader.readLengthAndString(2)
        val rsid = reader.readLengthAndString(2)
        val chr = reader.readLengthAndString(2)
        val pos = reader.readInt()

        val nAlleles = if (bState.version == 2) reader.readShort() else 2
        assert(nAlleles >= 2, s"Number of alleles must be greater than or equal to 2. Found $nAlleles alleles for variant '$snpid'")
        (0 until nAlleles).foreach { i => reader.readLengthAndString(4) }

        position = bState.version match {
          case 1 =>
            if (bState.compressed)
              reader.readInt() + reader.getPosition
            else
              reader.getPosition + 6 * bState.nSamples
          case 2 =>
            reader.readInt() + reader.getPosition
        }

        dataBlockStarts(i) = position
      }
    }

    IndexBTree.write(dataBlockStarts, indexFile, hConf)

  }

  def readSamples(hConf: org.apache.hadoop.conf.Configuration, file: String): Array[String] = {
    val bState = readState(hConf, file)
    if (bState.hasIds) {
      hConf.readFile(file) { is =>
        val reader = new HadoopFSDataBinaryReader(is)

        reader.seek(bState.headerLength + 4)
        val sampleIdSize = reader.readInt()
        val nSamples = reader.readInt()

        if (nSamples != bState.nSamples)
          fatal("BGEN file is malformed -- number of sample IDs in header does not equal number in file")

        if (sampleIdSize + bState.headerLength > bState.dataStart - 4)
          fatal("BGEN file is malformed -- offset is smaller than length of header")

        (0 until nSamples).map { i =>
          reader.readLengthAndString(2)
        }.toArray
      }
    } else {
      warn(s"BGEN file `$file' contains no sample ID block and no sample ID file given.\n" +
        s"  Using _0, _1, ..., _N as sample IDs.")
      (0 until bState.nSamples).map(i => s"_$i").toArray
    }
  }

  def readSampleFile(hConf: org.apache.hadoop.conf.Configuration, file: String): Array[String] = {
    hConf.readFile(file) { s =>
      Source.fromInputStream(s)
        .getLines()
        .drop(2)
        .filter(line => !line.isEmpty)
        .map { line =>
          val arr = line.split("\\s+")
          arr(0)
        }
        .toArray
    }
  }

  def readState(hConf: org.apache.hadoop.conf.Configuration, file: String): BgenHeader = {
    hConf.readFile(file) { is =>
      val reader = new HadoopFSDataBinaryReader(is)
      readState(reader)
    }
  }

  def readState(reader: HadoopFSDataBinaryReader): BgenHeader = {
    reader.seek(0)
    val allInfoLength = reader.readInt()
    val headerLength = reader.readInt()
    val dataStart = allInfoLength + 4

    assert(headerLength <= allInfoLength)
    val nVariants = reader.readInt()
    val nSamples = reader.readInt()

    val magicNumber = reader.readBytes(4)
      .map(_.toInt)
      .toSeq

    if (magicNumber != Seq(0, 0, 0, 0) && magicNumber != Seq(98, 103, 101, 110))
      fatal(s"expected magic number [0000] or [bgen], got [${ magicNumber.mkString }]")

    if (headerLength > 20)
      reader.skipBytes(headerLength.toInt - 20)

    val flags = reader.readInt()
    val compressType = flags & 3

    if (compressType != 0 && compressType != 1)
      fatal(s"Hail only supports zlib compression.")

    val isCompressed = compressType != 0

    val version = (flags >>> 2) & 0xf
    if (version != 1 && version != 2)
      fatal(s"Hail supports BGEN version 1.1 and 1.2, got version 1.$version")

    val hasIds = (flags >> 31 & 1) != 0
    BgenHeader(isCompressed, nSamples, nVariants, headerLength, dataStart, hasIds, version)
  }
}
