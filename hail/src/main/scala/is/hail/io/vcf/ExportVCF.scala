package is.hail.io.vcf

import is.hail
import is.hail.annotations.Region
import is.hail.expr.types._
import is.hail.io.{VCFAttributes, VCFFieldAttributes, VCFMetadata}
import is.hail.utils._
import is.hail.variant.{Call, MatrixTable, RegionValueVariant}

import scala.io.Source

object ExportVCF {
  def infoNumber(t: Type): String = t match {
    case TBoolean(_) => "0"
    case TArray(_, _) => "."
    case TSet(_, _) => "."
    case _ => "1"
  }

  def strVCF(sb: StringBuilder, elementType: Type, m: Region, offset: Long) {
    elementType match {
      case TInt32(_) =>
        val x = m.loadInt(offset)
        sb.append(x)
      case TInt64(_) =>
        val x = m.loadLong(offset)
        if (x > Int.MaxValue || x < Int.MinValue)
          fatal(s"Cannot convert Long to Int if value is greater than Int.MaxValue (2^31 - 1) " +
            s"or less than Int.MinValue (-2^31). Found $x.")
        sb.append(x)
      case TFloat32(_) =>
        val x = m.loadFloat(offset)
        if (x.isNaN)
          sb += '.'
        else
          sb.append(x.formatted("%.5e"))
      case TFloat64(_) =>
        val x = m.loadDouble(offset)
        if (x.isNaN)
          sb += '.'
        else
          sb.append(x.formatted("%.5e"))
      case TString(_) =>
        sb.append(TString.loadString(m, offset))
      case TCall(_) =>
        val c = m.loadInt(offset)
        Call.vcfString(c, sb)
      case _ =>
        fatal(s"VCF does not support type $elementType")
    }
  }
  
  def iterableVCF(sb: StringBuilder, t: TIterable, m: Region, length: Int, offset: Long) {
    if (length > 0) {
      var i = 0
      while (i < length) {
        if (i > 0)
          sb += ','
        if (t.isElementDefined(m, offset, i)) {
          val eOffset = t.loadElement(m, offset, length, i)
          strVCF(sb, t.elementType, m, eOffset)
        } else
          sb += '.'
        i += 1
      }
    } else
      sb += '.'
  }

  def emitInfo(sb: StringBuilder, f: Field, m: Region, offset: Long, wroteLast: Boolean): Boolean = {
    f.typ match {
      case it: TIterable if !it.elementType.isOfType(TBoolean()) =>
        val length = it.loadLength(m, offset)
        if (length == 0)
          wroteLast
        else {
          if (wroteLast)
            sb += ';'
          sb.append(f.name)
          sb += '='
          iterableVCF(sb, it, m, length, offset)
          true
        }
      case TBoolean(_) =>
        if (m.loadBoolean(offset)) {
          if (wroteLast)
            sb += ';'
          sb.append(f.name)
          true
        } else
          wroteLast
      case t =>
        if (wroteLast)
          sb += ';'
        sb.append(f.name)
        sb += '='
        strVCF(sb, t, m, offset)
        true
    }
  }

  def infoType(t: Type): Option[String] = t match {
    case _: TInt32 | _: TInt64 => Some("Integer")
    case _: TFloat64 | _: TFloat32 => Some("Float")
    case _: TString => Some("String")
    case _: TBoolean => Some("Flag")
    case _ => None
  }

  def infoType(f: Field): String = {
    val tOption = f.typ match {
      case TArray(TBoolean(_), _) | TSet(TBoolean(_), _) => None
      case TArray(elt, _) => infoType(elt)
      case TSet(elt, _) => infoType(elt)
      case t => infoType(t)
    }
    tOption match {
      case Some(s) => s
      case _ => fatal(s"INFO field '${ f.name }': VCF does not support type `${ f.typ }'.")
    }
  }

  def formatType(t: Type): Option[String] = t match {
    case _: TInt32 | _: TInt64 => Some("Integer")
    case _: TFloat64 | _: TFloat32 => Some("Float")
    case _: TString => Some("String")
    case _: TCall => Some("String")
    case _ => None
  }

  def formatType(f: Field): String = {
    val tOption = f.typ match {
      case TArray(elt, _) => formatType(elt)
      case TSet(elt, _) => formatType(elt)
      case t => formatType(t)
    }

    tOption match {
      case Some(s) => s
      case _ => fatal(s"FORMAT field '${ f.name }': VCF does not support type `${ f.typ }'.")
    }
  }

  def validFormatType(typ: Type): Boolean = {
    typ match {
      case _: TString => true
      case _: TFloat64 => true
      case _: TFloat32 => true
      case _: TInt32 => true
      case _: TInt64 => true
      case _: TCall => true
      case _ => false
    }
  }
  
  def checkFormatSignature(tg: TStruct) {
    tg.fields.foreach { fd =>
      val valid = fd.typ match {
        case it: TIterable => validFormatType(it.elementType)
        case t => validFormatType(t)
      }
      if (!valid)
        fatal(s"Invalid type for format field '${ fd.name }'. Found '${ fd.typ }'.")
    }
  }
  
  def emitGenotype(sb: StringBuilder, formatFieldOrder: Array[Int], tg: TStruct, m: Region, offset: Long) {
    formatFieldOrder.foreachBetween { j =>
      val fIsDefined = tg.isFieldDefined(m, offset, j)
      val fOffset = tg.loadField(m, offset, j)

      tg.fields(j).typ match {
        case it: TIterable =>
          if (fIsDefined) {
            val fLength = it.loadLength(m, fOffset)
            iterableVCF(sb, it, m, fLength, fOffset)
          } else
            sb += '.'
        case t =>
          if (fIsDefined)
            strVCF(sb, t, m, fOffset)
          else if (t.isOfType(TCall()))
            sb.append("./.")
          else
            sb += '.'
      }
    }(sb += ':')
  }

  def getAttributes(k1: String, attributes: Option[VCFMetadata]): Option[VCFAttributes] =
    attributes.flatMap(_.get(k1))

  def getAttributes(k1: String, k2: String, attributes: Option[VCFMetadata]): Option[VCFFieldAttributes] =
    getAttributes(k1, attributes).flatMap(_.get(k2))

  def getAttributes(k1: String, k2: String, k3: String, attributes: Option[VCFMetadata]): Option[String] =
    getAttributes(k1, k2, attributes).flatMap(_.get(k3))

  def apply(vsm: MatrixTable, path: String, append: Option[String] = None,
    exportType: Int = ExportType.CONCATENATED, metadata: Option[VCFMetadata] = None) {
    
    vsm.requireColKeyString("export_vcf")
    vsm.requireRowKeyVariant("export_vcf")
    
    val tg = vsm.entryType match {
      case t: TStruct => t
      case t =>
        fatal(s"export_vcf requires g to have type TStruct, found $t")
    }

    checkFormatSignature(tg)
        
    val formatFieldOrder: Array[Int] = tg.fieldIdx.get("GT") match {
      case Some(i) => (i +: tg.fields.filter(fd => fd.name != "GT").map(_.index)).toArray
      case None => tg.fields.indices.toArray
    }
    val formatFieldString = formatFieldOrder.map(i => tg.fields(i).name).mkString(":")

    val tinfo =
      if (vsm.rowType.hasField("info")) {
        vsm.rowType.field("info").typ match {
          case t: TStruct => t.asInstanceOf[TStruct]
          case t =>
            warn(s"export_vcf found row field 'info' of type $t, but expected type 'Struct'. Emitting no INFO fields.")
            TStruct.empty()
        }
      } else {
        warn(s"export_vcf found no row field 'info'. Emitting no INFO fields.")
        TStruct.empty()
      }

    val rg = vsm.referenceGenome
    val assembly = rg.name
    
    val localNSamples = vsm.numCols
    val hasSamples = localNSamples > 0

    def header: String = {
      val sb = new StringBuilder()

      sb.append("##fileformat=VCFv4.2\n")
      sb.append(s"##hailversion=${ hail.HAIL_PRETTY_VERSION }\n")

      tg.fields.foreach { f =>
        val attrs = getAttributes("format", f.name, metadata).getOrElse(Map.empty[String, String])
        sb.append("##FORMAT=<ID=")
        sb.append(f.name)
        sb.append(",Number=")
        sb.append(attrs.getOrElse("Number", infoNumber(f.typ)))
        sb.append(",Type=")
        sb.append(formatType(f))
        sb.append(",Description=\"")
        sb.append(attrs.getOrElse("Description", ""))
        sb.append("\">\n")
      }

      val filters = getAttributes("filter", metadata).getOrElse(Map.empty[String, Any]).keys.toArray.sorted
      filters.foreach { id =>
        val attrs = getAttributes("filter", id, metadata).getOrElse(Map.empty[String, String])
        sb.append("##FILTER=<ID=")
        sb.append(id)
        sb.append(",Description=\"")
        sb.append(attrs.getOrElse("Description", ""))
        sb.append("\">\n")
      }

      tinfo.fields.foreach { f =>
        val attrs = getAttributes("info", f.name, metadata).getOrElse(Map.empty[String, String])
        sb.append("##INFO=<ID=")
        sb.append(f.name)
        sb.append(",Number=")
        sb.append(attrs.getOrElse("Number", infoNumber(f.typ)))
        sb.append(",Type=")
        sb.append(infoType(f))
        sb.append(",Description=\"")
        sb.append(attrs.getOrElse("Description", ""))
        sb.append("\">\n")
      }

      append.foreach { f =>
        vsm.sparkContext.hadoopConfiguration.readFile(f) { s =>
          Source.fromInputStream(s)
            .getLines()
            .filterNot(_.isEmpty)
            .foreach { line =>
              sb.append(line)
              sb += '\n'
            }
        }
      }

      rg.contigs.foreachBetween { c =>
        sb.append("##contig=<ID=")
        sb.append(c)
        sb.append(",length=")
        sb.append(rg.contigLength(c))
        sb.append(",assembly=")
        sb.append(assembly)
        sb += '>'
      }(sb += '\n')

      sb += '\n'

      sb.append("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO")
      if (hasSamples)
        sb.append("\tFORMAT")
      vsm.stringSampleIds.foreach { id =>
        sb += '\t'
        sb.append(id)
      }
      sb.result()
    }

    val fieldIdx = vsm.rowType.fieldIdx

    def lookupVAField(fieldName: String, vcfColName: String, expectedTypeOpt: Option[Type]): (Boolean, Int) = {
      fieldIdx.get(fieldName) match {
        case Some(idx) =>
          val t = vsm.rowType.types(idx)
          if (expectedTypeOpt.forall(t == _)) // FIXME: make sure this is right
            (true, idx)
          else {
            warn(s"export_vcf found row field $fieldName with type '$t', but expected type ${ expectedTypeOpt.get }. " +
              s"Emitting missing $vcfColName.")
            (false, 0)
          }
        case None => (false, 0)
      }
    }
    
    val (idExists, idIdx) = lookupVAField("rsid", "ID", Some(TString()))
    val (qualExists, qualIdx) = lookupVAField("qual", "QUAL", Some(TFloat64()))
    val (filtersExists, filtersIdx) = lookupVAField("filters", "FILTERS", Some(TSet(TString())))
    val (infoExists, infoIdx) = lookupVAField("info", "INFO", None)
    
    val fullRowType = vsm.rvRowType
    val localEntriesIndex = vsm.entriesIndex
    val localEntriesType = vsm.matrixType.entryArrayType

    vsm.rvd.mapPartitions { it =>
      val sb = new StringBuilder
      var m: Region = null

      val rvv = new RegionValueVariant(fullRowType)
      it.map { rv =>
        sb.clear()

        m = rv.region
        rvv.setRegion(rv)

        sb.append(rvv.contig())
        sb += '\t'
        sb.append(rvv.position())
        sb += '\t'
  
        if (idExists && fullRowType.isFieldDefined(rv, idIdx)) {
          val idOffset = fullRowType.loadField(rv, idIdx)
          sb.append(TString.loadString(m, idOffset))
        } else
          sb += '.'
  
        sb += '\t'
        sb.append(rvv.alleles()(0))
        sb += '\t'
        rvv.alleles().tail.foreachBetween(aa =>
          sb.append(aa))(sb += ',')
        sb += '\t'

        if (qualExists && fullRowType.isFieldDefined(rv, qualIdx)) {
          val qualOffset = fullRowType.loadField(rv, qualIdx)
          sb.append(m.loadDouble(qualOffset).formatted("%.2f"))
        } else
          sb += '.'
        
        sb += '\t'
        
        if (filtersExists && fullRowType.isFieldDefined(rv, filtersIdx)) {
          val filtersOffset = fullRowType.loadField(rv, filtersIdx)
          val filtersLength = TSet(TString()).loadLength(m, filtersOffset)
          if (filtersLength == 0)
            sb.append("PASS")
          else
            iterableVCF(sb, TSet(TString()), m, filtersLength, filtersOffset)
        } else
          sb += '.'

        sb += '\t'
        
        var wroteAnyInfo: Boolean = false
        if (infoExists && fullRowType.isFieldDefined(rv, infoIdx)) {
          var wrote: Boolean = false
          val infoOffset = fullRowType.loadField(rv, infoIdx)
          var i = 0
          while (i < tinfo.size) {
            if (tinfo.isFieldDefined(m, infoOffset, i)) {
              wrote = emitInfo(sb, tinfo.fields(i), m, tinfo.loadField(m, infoOffset, i), wrote)
              wroteAnyInfo = wroteAnyInfo || wrote
            }
            i += 1
          }
        }
        if (!wroteAnyInfo)
          sb += '.'

        if (hasSamples) {
          sb += '\t'
          sb.append(formatFieldString)

          val gsOffset = fullRowType.loadField(rv, localEntriesIndex)
          var i = 0
          while (i < localNSamples) {
            sb += '\t'
            if (localEntriesType.isElementDefined(m, gsOffset, i))
              emitGenotype(sb, formatFieldOrder, tg, m, localEntriesType.loadElement(m, gsOffset, localNSamples, i))
            else
              sb.append("./.")

            i += 1
          }
        }
        
        sb.result()
      }
    }.writeTable(path, vsm.hc.tmpDir, Some(header), exportType = exportType)
  }
}
