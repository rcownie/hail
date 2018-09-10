package is.hail.expr.types.physical

import is.hail.annotations.{Annotation, AnnotationPathException, _}
import is.hail.asm4s.Code
import is.hail.expr.Parser
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.expr.types.{Field, TStruct}
import is.hail.utils._
import org.apache.spark.sql.Row
import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

import scala.collection.JavaConverters._

object PStruct {
  private val requiredEmpty = PStruct(Array.empty[PField], true)
  private val optionalEmpty = PStruct(Array.empty[PField], false)

  def empty(required: Boolean = false): PStruct = if (required) requiredEmpty else optionalEmpty

  def apply(required: Boolean, args: (String, PType)*): PStruct =
    PStruct(args
      .iterator
      .zipWithIndex
      .map { case ((n, t), i) => PField(n, t, i) }
      .toArray,
      required)

  def apply(args: (String, PType)*): PStruct =
    apply(false, args: _*)

  def apply(names: java.util.ArrayList[String], types: java.util.ArrayList[PType], required: Boolean): PStruct = {
    val sNames = names.asScala.toArray
    val sTypes = types.asScala.toArray
    if (sNames.length != sTypes.length)
      fatal(s"number of names does not match number of types: found ${ sNames.length } names and ${ sTypes.length } types")

    val t = PStruct(sNames.zip(sTypes): _*)
    t.setRequired(required).asInstanceOf[PStruct]
  }
}

final case class PStruct(fields: IndexedSeq[PField], override val required: Boolean = false) extends PBaseStruct {
  def virtualType: TStruct = TStruct(fields.map(f => Field(f.name, f.typ.virtualType, f.index)), required)

  assert(fields.zipWithIndex.forall { case (f, i) => f.index == i })

  val types: Array[PType] = fields.map(_.typ).toArray

  val fieldRequired: Array[Boolean] = types.map(_.required)

  val fieldIdx: Map[String, Int] =
    fields.map(f => (f.name, f.index)).toMap

  val fieldNames: Array[String] = fields.map(_.name).toArray

  if (!fieldNames.areDistinct()) {
    val duplicates = fieldNames.duplicates()
    fatal(s"cannot create struct with duplicate ${plural(duplicates.size, "field")}: " +
      s"${fieldNames.map(prettyIdentifier).mkString(", ")}", fieldNames.duplicates())
  }

  val size: Int = fields.length

  override def truncate(newSize: Int): PStruct =
    PStruct(fields.take(newSize), required)

  val missingIdx = new Array[Int](size)
  val nMissing: Int = PBaseStruct.getMissingness(types, missingIdx)
  val nMissingBytes = (nMissing + 7) >>> 3
  val byteOffsets = new Array[Long](size)
  override val byteSize: Long = PBaseStruct.getByteSizeAndOffsets(types, nMissingBytes, byteOffsets)
  override val alignment: Long = PBaseStruct.alignment(types)

  def codeOrdering(mb: EmitMethodBuilder, other: PType): CodeOrdering = {
    assert(other isOfType this)
    CodeOrdering.rowOrdering(this, other.asInstanceOf[PStruct], mb)
  }

  def fieldByName(name: String): PField = fields(fieldIdx(name))

  override def canCompare(other: PType): Boolean = other match {
    case t: PStruct => size == t.size && fields.zip(t.fields).forall { case (f1, f2) =>
      f1.name == f2.name && f1.typ.canCompare(f2.typ)
    }
    case _ => false
  }

  override def unify(concrete: PType): Boolean = concrete match {
    case PStruct(cfields, _) =>
      fields.length == cfields.length &&
        (fields, cfields).zipped.forall { case (f, cf) =>
          f.unify(cf)
        }
    case _ => false
  }

  override def subst() = PStruct(fields.map(f => f.copy(typ = f.typ.subst().asInstanceOf[PType])))

  def index(str: String): Option[Int] = fieldIdx.get(str)

  def selfField(name: String): Option[PField] = fieldIdx.get(name).map(i => fields(i))

  def hasField(name: String): Boolean = fieldIdx.contains(name)

  def field(name: String): PField = fields(fieldIdx(name))

  def toTTuple: PTuple = PTuple(types, required)

  override def fieldOption(path: List[String]): Option[PField] =
    if (path.isEmpty)
      None
    else {
      val f = selfField(path.head)
      if (path.length == 1)
        f
      else
        f.flatMap(_.typ.fieldOption(path.tail))
    }

  def unsafeStructInsert(typeToInsert: PType, path: List[String]): (PStruct, UnsafeInserter) = {
    assert(typeToInsert.isInstanceOf[PStruct] || path.nonEmpty)
    val (t, i) = unsafeInsert(typeToInsert, path)
    (t.asInstanceOf[PStruct], i)
  }

  override def unsafeInsert(typeToInsert: PType, path: List[String]): (PType, UnsafeInserter) = {
    val vt = virtualType
    if (path.isEmpty) {
      (typeToInsert, (region, offset, rvb, inserter) => inserter())
    } else {
      val localSize = size
      val key = path.head
      selfField(key) match {
        case Some(f) =>
          val j = f.index
          val (insertedFieldType, fieldInserter) = f.typ.unsafeInsert(typeToInsert, path.tail)

          (updateKey(key, j, insertedFieldType), { (region, offset, rvb, inserter) =>
            rvb.startStruct()
            var i = 0
            while (i < j) {
              if (region != null)
                rvb.addField(vt, region, offset, i)
              else
                rvb.setMissing()
              i += 1
            }
            if (region != null && isFieldDefined(region, offset, j))
              fieldInserter(region, loadField(region, offset, j), rvb, inserter)
            else
              fieldInserter(null, 0, rvb, inserter)
            i += 1
            while (i < localSize) {
              if (region != null)
                rvb.addField(vt, region, offset, i)
              else
                rvb.setMissing()
              i += 1
            }
            rvb.endStruct()
          })

        case None =>
          val (insertedFieldType, fieldInserter) = PStruct.empty().unsafeInsert(typeToInsert, path.tail)

          (appendKey(key, insertedFieldType), { (region, offset, rvb, inserter) =>
            rvb.startStruct()
            var i = 0
            while (i < localSize) {
              if (region != null)
                rvb.addField(vt, region, offset, i)
              else
                rvb.setMissing()
              i += 1
            }
            fieldInserter(null, 0, rvb, inserter)
            rvb.endStruct()
          })
      }
    }
  }

  def updateKey(key: String, i: Int, sig: PType): PStruct = {
    assert(fieldIdx.contains(key))

    val newFields = Array.fill[PField](fields.length)(null)
    for (i <- fields.indices)
      newFields(i) = fields(i)
    newFields(i) = PField(key, sig, i)
    PStruct(newFields, required)
  }

  def deleteKey(key: String, index: Int): PStruct = {
    assert(fieldIdx.contains(key))
    if (fields.length == 1)
      PStruct.empty()
    else {
      val newFields = Array.fill[PField](fields.length - 1)(null)
      for (i <- 0 until index)
        newFields(i) = fields(i)
      for (i <- index + 1 until fields.length)
        newFields(i - 1) = fields(i).copy(index = i - 1)
      PStruct(newFields, required)
    }
  }

  def appendKey(key: String, sig: PType): PStruct = {
    assert(!fieldIdx.contains(key))
    val newFields = Array.fill[PField](fields.length + 1)(null)
    for (i <- fields.indices)
      newFields(i) = fields(i)
    newFields(fields.length) = PField(key, sig, fields.length)
    PStruct(newFields, required)
  }


  def rename(m: Map[String, String]): PStruct = {
    val newFieldsBuilder = new ArrayBuilder[(String, PType)]()
    fields.foreach { fd =>
      val n = fd.name
      newFieldsBuilder += (m.getOrElse(n, n), fd.typ)
    }
    PStruct(newFieldsBuilder.result(): _*)
  }

  def ++(that: PStruct): PStruct = {
    val overlapping = fields.map(_.name).toSet.intersect(
      that.fields.map(_.name).toSet)
    if (overlapping.nonEmpty)
      fatal(s"overlapping fields in struct concatenation: ${ overlapping.mkString(", ") }")

    PStruct(fields.map(f => (f.name, f.typ)) ++ that.fields.map(f => (f.name, f.typ)): _*)
  }

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("struct{")
    fields.foreachBetween({ field =>
      sb.append(prettyIdentifier(field.name))
      sb.append(": ")
      field.typ.pyString(sb)
    }) { sb.append(", ")}
    sb.append('}')
  }

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean) {
    if (compact) {
      sb.append("Struct{")
      fields.foreachBetween(_.pretty(sb, indent, compact))(sb += ',')
      sb += '}'
    } else {
      if (size == 0)
        sb.append("Struct { }")
      else {
        sb.append("Struct {")
        sb += '\n'
        fields.foreachBetween(_.pretty(sb, indent + 4, compact))(sb.append(",\n"))
        sb += '\n'
        sb.append(" " * indent)
        sb += '}'
      }
    }
  }

  def typeAfterSelect(keep: IndexedSeq[Int]): PStruct =
    PStruct(keep.map(i => fieldNames(i) -> types(i)): _*)

  override val fundamentalType: PStruct = {
    val fundamentalFieldTypes = fields.map(f => f.typ.fundamentalType)
    if ((fields, fundamentalFieldTypes).zipped
      .forall { case (f, ft) => f.typ == ft })
      this
    else {
      val t = PStruct((fields, fundamentalFieldTypes).zipped.map { case (f, ft) => (f.name, ft) }: _*)
      t.setRequired(required).asInstanceOf[PStruct]
    }
  }

  def loadField(region: Code[Region], offset: Code[Long], fieldName: String): Code[Long] = {
    val f = field(fieldName)
    loadField(region, fieldOffset(offset, f.index), f.index)
  }
}
