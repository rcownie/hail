package is.hail.annotations

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import is.hail.expr.types._
import is.hail.utils._
import is.hail.variant.{Locus, RGBase}
import org.apache.spark.sql.Row
import sun.reflect.generics.reflectiveObjects.NotImplementedException

trait UnKryoSerializable extends KryoSerializable {
  def write(kryo: Kryo, output: Output): Unit = {
    throw new NotImplementedException()
  }

  def read(kryo: Kryo, input: Input): Unit = {
    throw new NotImplementedException()
  }
}

object UnsafeIndexedSeq {
  def apply(t: TArray, elements: Array[RegionValue]): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(elements.length)
    var i = 0
    while (i < elements.length) {
      rvb.addRegionValue(t.elementType, elements(i))
      i += 1
    }
    rvb.endArray()

    new UnsafeIndexedSeq(t, region, rvb.end())
  }

  def apply(t: TArray, a: IndexedSeq[Annotation]): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(a.length)
    var i = 0
    while (i < a.length) {
      rvb.addAnnotation(t.elementType, a(i))
      i += 1
    }
    rvb.endArray()
    new UnsafeIndexedSeq(t, region, rvb.end())
  }

  def empty(t: TArray): UnsafeIndexedSeq = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    rvb.start(t)
    rvb.startArray(0)
    rvb.endArray()
    new UnsafeIndexedSeq(t, region, rvb.end())
  }
}

class UnsafeIndexedSeq(
  var t: TContainer,
  var region: Region, var aoff: Long) extends IndexedSeq[Annotation] with UnKryoSerializable {

  var length: Int = t.loadLength(region, aoff)

  def apply(i: Int): Annotation = {
    if (i < 0 || i >= length)
      throw new IndexOutOfBoundsException(i.toString)
    if (t.isElementDefined(region, aoff, i)) {
      UnsafeRow.read(t.elementType, region, t.loadElement(region, aoff, length, i))
    } else
      null
  }
}

object UnsafeRow {
  def readBinary(region: Region, boff: Long): Array[Byte] = {
    val binLength = TBinary.loadLength(region, boff)
    region.loadBytes(TBinary.bytesOffset(boff), binLength)
  }

  def readArray(t: TContainer, region: Region, aoff: Long): IndexedSeq[Any] =
    new UnsafeIndexedSeq(t, region, aoff)

  def readBaseStruct(t: TBaseStruct, region: Region, offset: Long): UnsafeRow =
    new UnsafeRow(t, region, offset)

  def readString(region: Region, boff: Long): String =
    new String(readBinary(region, boff))

  def readLocus(region: Region, offset: Long, rg: RGBase): Locus = {
    val ft = rg.locusType.fundamentalType.asInstanceOf[TStruct]
    Locus(
      readString(region, ft.loadField(region, offset, 0)),
      region.loadInt(ft.loadField(region, offset, 1)))
  }

  def read(t: Type, region: Region, offset: Long): Any = {
    t match {
      case _: TBoolean =>
        region.loadBoolean(offset)
      case _: TInt32 | _: TCall => region.loadInt(offset)
      case _: TInt64 => region.loadLong(offset)
      case _: TFloat32 => region.loadFloat(offset)
      case _: TFloat64 => region.loadDouble(offset)
      case t: TArray =>
        readArray(t, region, offset)
      case t: TSet =>
        readArray(t, region, offset).toSet
      case _: TString => readString(region, offset)
      case _: TBinary => readBinary(region, offset)
      case td: TDict =>
        val a = readArray(td, region, offset)
        a.asInstanceOf[IndexedSeq[Row]].map(r => (r.get(0), r.get(1))).toMap
      case t: TBaseStruct => readBaseStruct(t, region, offset)
      case x: TLocus => readLocus(region, offset, x.rg)
      case x: TInterval =>
        val ft = x.fundamentalType.asInstanceOf[TStruct]
        val start: Annotation =
          if (ft.isFieldDefined(region, offset, 0))
            read(x.pointType, region, ft.loadField(region, offset, 0))
          else
            null
        val end =
          if (ft.isFieldDefined(region, offset, 1))
            read(x.pointType, region, ft.loadField(region, offset, 1))
          else
            null
        val includesStart = read(TBooleanRequired, region, ft.loadField(region, offset, 2)).asInstanceOf[Boolean]
        val includesEnd = read(TBooleanRequired, region, ft.loadField(region, offset, 3)).asInstanceOf[Boolean]
        Interval(start, end, includesStart, includesEnd)
    }
  }
}

class UnsafeRow(var t: TBaseStruct,
  var region: Region, var offset: Long) extends Row with UnKryoSerializable {

  def this(t: TBaseStruct, rv: RegionValue) = this(t, rv.region, rv.offset)

  def this(t: TBaseStruct) = this(t, null, 0)

  def this() = this(null, null, 0)

  def set(newRegion: Region, newOffset: Long) {
    region = newRegion
    offset = newOffset
  }

  def set(rv: RegionValue): Unit = set(rv.region, rv.offset)

  def length: Int = t.size

  private def assertDefined(i: Int) {
    if (isNullAt(i))
      throw new NullPointerException(s"null value at index $i")
  }

  def get(i: Int): Any = {
    if (isNullAt(i))
      null
    else
      UnsafeRow.read(t.types(i), region, t.loadField(region, offset, i))
  }

  def copy(): Row = new UnsafeRow(t, region, offset)

  def pretty(): String = region.pretty(t, offset)

  override def getInt(i: Int): Int = {
    assertDefined(i)
    region.loadInt(t.loadField(region, offset, i))
  }

  override def getLong(i: Int): Long = {
    assertDefined(i)
    region.loadLong(t.loadField(region, offset, i))
  }

  override def getFloat(i: Int): Float = {
    assertDefined(i)
    region.loadFloat(t.loadField(region, offset, i))
  }

  override def getDouble(i: Int): Double = {
    assertDefined(i)
    region.loadDouble(t.loadField(region, offset, i))
  }

  override def getBoolean(i: Int): Boolean = {
    assertDefined(i)
    region.loadBoolean(t.loadField(region, offset, i))
  }

  override def getByte(i: Int): Byte = {
    assertDefined(i)
    region.loadByte(t.loadField(region, offset, i))
  }

  override def isNullAt(i: Int): Boolean = {
    if (i < 0 || i >= t.size)
      throw new IndexOutOfBoundsException(i.toString)
    !t.isFieldDefined(region, offset, i)
  }

  private def writeObject(s: ObjectOutputStream): Unit = {
    throw new NotImplementedException()
  }

  private def readObject(s: ObjectInputStream): Unit = {
    throw new NotImplementedException()
  }

  def sameVerbose(b: UnsafeRow, verbose: Boolean = false): Boolean = {
    val regionA = region
    val regionB = b.region

    // Step-by-step comparison with verbose error messages
    def sameRecurse(curType: Type, addrA: Long, offA: Long, addrB: Long, offB: Long, name: String): Boolean = {
      var same = true
      if (verbose) System.err.println(s"DEBUG: ${name}")
      curType.fundamentalType match {
        case typ: TBaseStruct =>
          if (typ.nMissingBytes > 0) {
            if (verbose) System.err.println(s"DEBUG: ${name}.missing")
            var idx = 0
            while (same && (idx < typ.fields.length)) {
              if (!typ.fieldRequired(idx)) {
                val m = typ.missingIdx(idx)
                val bitA = regionA.loadBit(addrA+offA, m)
                val bitB = regionB.loadBit(addrB+offB, m)
                if (bitA != bitB) {
                  System.err.println(s"FAIL: ${name}.missing(${m}=${typ.fields(idx).name}) ${bitA} != ${bitB}")
                  System.err.println(s"FAIL: + addrA ${addrA.toHexString}+${offA} addrB ${addrB.toHexString}+${offB}")
                  System.err.println(s"FAIL: + in struct ${typ}")
                  same = false
                }
              }
              idx += 1
            }
          }
          var idx = 0
          while (same && (idx < typ.fields.length)) {
            if (typ.fieldRequired(idx) || !regionA.loadBit(addrA+offA, typ.missingIdx(idx))) {
              val fieldOffset = typ.byteOffsets(idx)
              val fieldSame = sameRecurse(
                typ.types(idx),
                addrA+offA, fieldOffset,
                addrB+offB, fieldOffset,
                name + s".${typ.fields(idx).name}"
              )
              if (!fieldSame) {
                if (same && typ.fields(idx).name.equals("END")) {
                  System.err.println(s"FAIL: + offset ${fieldOffset} in ${typ}")
                }
                same = false
              }
            }
            idx += 1
          }

        case typ: TArray =>
          if (verbose) System.err.println(s"DEBUG: array ptr")
          val ptrA = regionA.loadLong(addrA+offA)
          val ptrB = regionB.loadLong(addrB+offB)
          if (verbose) System.err.println(s"DEBUG: array len")
          val lenA = regionA.loadInt(ptrA)
          val lenB = regionB.loadInt(ptrB)
          if (lenA != lenB) {
            System.err.println(s"FAIL: ${name}.len ${lenA} != ${lenB} at ${ptrA.toHexString} and ${ptrB.toHexString}")
            same = false
          }
          val haveMissing = !typ.elementType.required
          if (haveMissing) {
            if (verbose && (lenA > 0)) System.err.println(s"DEBUG: array missing")
            var idx = 0
            while (same && (idx < lenA)) {
              val bitA = regionA.loadBit(ptrA+4, idx)
              val bitB = regionB.loadBit(ptrB+4, idx)
              if (bitA != bitB) {
                System.err.println(s"FAIL: ${name}.missing(${idx})) ${bitA} != ${bitB}")
                same = false
              }
              idx += 1
            }
          }          
          var idx = 0
          while (same && (idx < lenA)) {
            if (typ.elementType.required || !regionA.loadBit(ptrA+4, idx)) {
              val elementSame = sameRecurse(
                typ.elementType,
                ptrA, typ.elementOffset(0L, lenA, idx),
                ptrB, typ.elementOffset(0L, lenA, idx),
                name + s"[${idx}]"
              )
              if (same && !elementSame) {
                same = false
                System.err.println(s"FAIL: + ${name} at [${idx}] of length ${lenA}")
              }
            }
            idx += 1
          }

        case typ: TBinary =>
          if (verbose) System.err.println(s"DEBUG: binary ptr")
          val ptrA = regionA.loadLong(addrA+offA)
          val ptrB = regionB.loadLong(addrB+offB)
          val lenA = regionA.loadInt(ptrA)
          val lenB = regionB.loadInt(ptrB)
          if (lenA != lenB) {
            System.err.println(s"FAIL: binary ${name}.len ${lenA} != ${lenB} at ${ptrA.toHexString} and ${ptrB.toHexString}")
            same = false
          }
          var idx = 0
          while (same && (idx < lenA)) {
            val valA = regionA.loadByte(ptrA+4+idx)
            val valB = regionB.loadByte(ptrB+4+idx)
            if (valA != valB) {
              System.err.println(s"FAIL: binary ${name}[${idx}] ${valA.toHexString} != ${valB.toHexString}")
              same = false
            }
            idx += 1
          }

        case typ: TBoolean =>
          if (verbose) System.err.println(s"DEBUG: ${name} bool")
          val valA = regionA.loadByte(addrA+offA)
          val valB = regionB.loadByte(addrB+offB)
          if (valA != valB) {
            System.err.println(s"FAIL: ${name} byte ${valA.toHexString} != ${valB.toHexString}")
            same = false
          }
        case typ: TInt32 =>
          if (verbose) System.err.println(s"DEBUG: ${name} int32")
          val valA = regionA.loadInt(addrA+offA)
          val valB = regionB.loadInt(addrB+offB)
          if (valA != valB) {
            System.err.println(s"FAIL: ${name} int ${valA} != ${valB}")
            same = false
          }          
        case typ: TInt64 =>
          if (verbose) System.err.println(s"DEBUG: ${name} int64")
          val valA = regionA.loadLong(addrA+offA)
          val valB = regionB.loadLong(addrB+offB)
          if (valA != valB) {
            System.err.println(s"FAIL: ${name} long ${valA} != ${valB}")
            same = false
          }
        case typ: TFloat32 =>
          if (verbose) System.err.println(s"DEBUG: ${name} float32")
          val valA = regionA.loadFloat(addrA+offA)
          val valB = regionB.loadFloat(addrB+offB)
          if (valA != valB) {
            System.err.println(s"FAIL: ${name} float ${valA} != ${valB}")
            same = false
          }        
        case typ: TFloat64 =>
          if (verbose) System.err.println(s"DEBUG: ${name} float64")
          val valA = regionA.loadDouble(addrA+offA)
          val valB = regionB.loadDouble(addrB+offB)
          if (valA != valB) {
            //System.err.println(s"FAIL: ${name} double ${valA} != ${valB}")
            //same = false
          }
        
        case _ =>
          assert(false)
      }
      same
    }

    val result = sameRecurse(t, offset, 0L, b.offset, 0L, "root")
    System.err.println(s"FAIL: sameVerbose() -> ${result}")
    result
  }
}

object SafeRow {
  def apply(t: TBaseStruct, region: Region, off: Long): Row = {
    Annotation.copy(t, new UnsafeRow(t, region, off)).asInstanceOf[Row]
  }

  def apply(t: TBaseStruct, rv: RegionValue): Row = SafeRow(t, rv.region, rv.offset)

  def selectFields(t: TBaseStruct, region: Region, off: Long)(selectIdx: Array[Int]): Row = {
    val fullRow = new UnsafeRow(t, region, off)
    Row(selectIdx.map(i => Annotation.copy(t.types(i), fullRow.get(i))): _*)
  }

  def selectFields(t: TBaseStruct, rv: RegionValue)(selectIdx: Array[Int]): Row =
    SafeRow.selectFields(t, rv.region, rv.offset)(selectIdx)

  def read(t: Type, region: Region, off: Long): Annotation =
    Annotation.copy(t, UnsafeRow.read(t, region, off))

  def read(t: Type, rv: RegionValue): Annotation =
    read(t, rv.region, rv.offset)
}

object SafeIndexedSeq {
  def apply(t: TArray, region: Region, off: Long): IndexedSeq[Annotation] =
    Annotation.copy(t, new UnsafeIndexedSeq(t, region, off))
      .asInstanceOf[IndexedSeq[Annotation]]

  def apply(t: TArray, rv: RegionValue): IndexedSeq[Annotation] =
    apply(t, rv.region, rv.offset)
}

class KeyedRow(var row: Row, keyFields: Array[Int]) extends Row {
  def length: Int = row.size
  def get(i: Int): Any = row.get(keyFields(i))
  def copy(): Row = new KeyedRow(row, keyFields)
  def set(newRow: Row): KeyedRow = {
    row = newRow
    this
  }
}
