package is.hail.annotations

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import is.hail.expr.types._
import is.hail.utils._
import is.hail.nativecode._

object Region {
  def apply(sizeHint: Long = 128): Region = {
    new Region()
  }

  def scoped[T](f: Region => T): T =
    using(Region())(f)
}

final class Region() extends NativeBase() with Serializable {
  @native def nativeCtor(): Unit
  nativeCtor()
  
  def this(b: Region) {
    this()
    copyAssign(b)
  }  

  /*
  def this(b: Array[Byte], size: Long) {
    this()
  }
  */

  // FIXME: not sure what this should mean ...
  def setFrom(b: Region) { }

  final def copyAssign(b: Region) = super.copyAssign(b)
  final def moveAssign(b: Region) = super.moveAssign(b)
  
  @native def clearButKeepMem(): Unit
  final def clear(): Unit = clearButKeepMem()
  
  @native def nativeAlign(alignment: Long): Unit
  @native def nativeAlignAllocate(alignment: Long, n: Long): Long
  @native def nativeAllocate(n: Long): Long
  
  final def align(a: Long) = nativeAlign(a)
  final def allocate(a: Long, n: Long) = nativeAlignAllocate(a, n)
  final def allocate(n: Long) = nativeAllocate(n)
  
  final def loadInt(addr: Long): Int = Memory.loadInt(addr)
  final def loadLong(addr: Long): Long = Memory.loadLong(addr)
  final def loadFloat(addr: Long): Float = Memory.loadFloat(addr)
  final def loadDouble(addr: Long): Double = Memory.loadDouble(addr)
  final def loadAddress(addr: Long): Long = Memory.loadLong(addr)
  final def loadByte(addr: Long): Byte = Memory.loadByte(addr)
  
  final def storeInt(addr: Long, v: Int) = Memory.storeInt(addr, v)
  final def storeLong(addr: Long, v: Long) = Memory.storeLong(addr, v)
  final def storeFloat(addr: Long, v: Float) = Memory.storeFloat(addr, v)
  final def storeDouble(addr: Long, v: Double) = Memory.storeDouble(addr, v)
  final def storeAddress(addr: Long, v: Long) = Memory.storeAddress(addr, v)
  final def storeByte(addr: Long, v: Byte) = Memory.storeByte(addr, v)
  
  final def loadBoolean(addr: Long): Boolean = if (Memory.loadByte(addr) == 0) false else true
  final def storeBoolean(addr: Long, v: Boolean) = Memory.storeByte(addr, if (v) 1 else 0)

  final def loadBytes(addr: Long, n: Int): Array[Byte] = {
    val a = new Array[Byte](n)
    Memory.copyToArray(a, 0, addr, n)
    a
  }

  final def loadBytes(addr: Long, dst: Array[Byte], dstOff: Long, n: Long): Unit = {
    Memory.copyToArray(dst, dstOff, addr, n)
  }

  final def storeBytes(addr: Long, src: Array[Byte]) {
    Memory.copyFromArray(addr, src, 0, src.length)
  }

  final def storeBytes(addr: Long, src: Array[Byte], srcOff: Long, n: Long) {
    Memory.copyFromArray(addr, src, srcOff, n)
  }

  final def copyFrom(src: Region, srcOff: Long, dstOff: Long, n: Long) {
    Memory.memcpy(dstOff, srcOff, n)
  }

  final def loadBit(byteOff: Long, bitOff: Long): Boolean = {
    val b = byteOff + (bitOff >> 3)
    (loadByte(b) & (1 << (bitOff & 7))) != 0
  }

  final def setBit(byteOff: Long, bitOff: Long) {
    val b = byteOff + (bitOff >> 3)
    storeByte(b,
      (loadByte(b) | (1 << (bitOff & 7))).toByte)
  }

  final def clearBit(byteOff: Long, bitOff: Long) {
    val b = byteOff + (bitOff >> 3)
    storeByte(b,
      (loadByte(b) & ~(1 << (bitOff & 7))).toByte)
  }

  final def storeBit(byteOff: Long, bitOff: Long, b: Boolean) {
    if (b)
      setBit(byteOff, bitOff)
    else
      clearBit(byteOff, bitOff)
  }

  /*
  final def appendInt(v: Int): Long = {
    val addr = allocate(4, 4)
    storeInt(addr, v)
    addr
  }
  */

  def appendBinary(v: Array[Byte]): Long = {
    var grain = TBinary.contentAlignment
    if (grain < 4) grain = 4
    val addr = allocate(grain, 4+v.length)
    storeInt(addr, v.length)
    storeBytes(addr, v)
    addr
  }

  final def appendArrayInt(v: Array[Int]): Long = {
    val len: Int = v.length
    val addr = allocate(4, 4*(1+len))
    storeInt(addr, len)
    val data = addr+4
    for (idx <- 0 to len-1)
      storeInt(data+4*idx, v(idx))
    addr
  }

  def visit(t: Type, off: Long, v: ValueVisitor) {
    t match {
      case _: TBoolean => v.visitBoolean(loadBoolean(off))
      case _: TInt32 => v.visitInt32(loadInt(off))
      case _: TInt64 => v.visitInt64(loadLong(off))
      case _: TFloat32 => v.visitFloat32(loadFloat(off))
      case _: TFloat64 => v.visitFloat64(loadDouble(off))
      case _: TString =>
        val boff = off
        v.visitString(TString.loadString(this, boff))
      case _: TBinary =>
        val boff = off
        val length = TBinary.loadLength(this, boff)
        val b = loadBytes(TBinary.bytesOffset(boff), length)
        v.visitBinary(b)
      case t: TContainer =>
        val aoff = off
        val length = t.loadLength(this, aoff)
        v.enterArray(t, length)
        var i = 0
        while (i < length) {
          v.enterElement(i)
          if (t.isElementDefined(this, aoff, i))
            visit(t.elementType, t.loadElement(this, aoff, length, i), v)
          else
            v.visitMissing(t.elementType)
          i += 1
        }
        v.leaveArray()
      case t: TStruct =>
        v.enterStruct(t)
        var i = 0
        while (i < t.size) {
          val f = t.fields(i)
          v.enterField(f)
          if (t.isFieldDefined(this, off, i))
            visit(f.typ, t.loadField(this, off, i), v)
          else
            v.visitMissing(f.typ)
          v.leaveField()
          i += 1
        }
        v.leaveStruct()
      case t: TTuple =>
        v.enterTuple(t)
        var i = 0
        while (i < t.size) {
          v.enterElement(i)
          if (t.isFieldDefined(this, off, i))
            visit(t.types(i), t.loadField(this, off, i), v)
          else
            v.visitMissing(t.types(i))
          v.leaveElement()
          i += 1
        }
        v.leaveTuple()
      case t: ComplexType =>
        visit(t.representation, off, v)
    }
  }

  def pretty(t: Type, off: Long): String = {
    val v = new PrettyVisitor()
    visit(t, off, v)
    v.result()
  }

}

final class OldRegion(
  private var mem: Array[Byte],
  private var end: Long = 0
) extends KryoSerializable with Serializable with AutoCloseable {
  def size: Long = end

  def capacity: Long = mem.length

  def copyFrom(other: OldRegion, readStart: Long, writeStart: Long, n: Long) {
    assert(size <= capacity)
    assert(other.size <= other.capacity)
    assert(n >= 0)
    assert(readStart >= 0 && readStart + n <= other.size)
    assert(writeStart >= 0 && writeStart + n <= size)
    Memory.memcpy(mem, writeStart, other.mem, readStart, n)
  }

  def loadInt(off: Long): Int = {
    assert(size <= capacity)
    assert(off >= 0 && off + 4 <= size)
    Memory.loadInt(mem, off)
  }

  def loadLong(off: Long): Long = {
    assert(size <= capacity)
    assert(off >= 0 && off + 8 <= size)
    Memory.loadLong(mem, off)
  }

  def loadFloat(off: Long): Float = {
    assert(size <= capacity)
    assert(off >= 0 && off + 4 <= size)
    Memory.loadFloat(mem, off)
  }

  def loadDouble(off: Long): Double = {
    assert(size <= capacity)
    assert(off >= 0 && off + 8 <= size)
    Memory.loadDouble(mem, off)
  }

  def loadAddress(off: Long): Long = {
    assert(size <= capacity)
    assert(off >= 0 && off + 8 <= size)
    Memory.loadAddress(mem, off)
  }

  def loadByte(off: Long): Byte = {
    assert(size <= capacity)
    assert(off >= 0 && off + 1 <= size)
    Memory.loadByte(mem, off)
  }

  def loadBytes(off: Long, n: Int): Array[Byte] = {
    assert(size <= capacity)
    assert(off >= 0 && off + n <= size)
    val a = new Array[Byte](n)
    Memory.memcpy(a, 0, mem, off, n)
    a
  }

  def loadBytes(off: Long, to: Array[Byte], toOff: Long, n: Long) {
    assert(size <= capacity)
    assert(off >= 0 && off + n <= size)
    assert(toOff + n <= to.length)
    Memory.memcpy(to, toOff, mem, off, n)
  }

  def storeInt(off: Long, i: Int) {
    assert(size <= capacity)
    assert(off >= 0 && off + 4 <= size)
    Memory.storeInt(mem, off, i)
  }

  def storeLong(off: Long, l: Long) {
    assert(size <= capacity)
    assert(off >= 0 && off + 8 <= size)
    Memory.storeLong(mem, off, l)
  }

  def storeFloat(off: Long, f: Float) {
    assert(size <= capacity)
    assert(off >= 0 && off + 4 <= size)
    Memory.storeFloat(mem, off, f)
  }

  def storeDouble(off: Long, d: Double) {
    assert(size <= capacity)
    assert(off >= 0 && off + 8 <= size)
    Memory.storeDouble(mem, off, d)
  }

  def storeAddress(off: Long, a: Long) {
    assert(size <= capacity)
    assert(off >= 0 && off + 8 <= size)
    Memory.storeAddress(mem, off, a)
  }

  def storeByte(off: Long, b: Byte) {
    assert(size <= capacity)
    assert(off >= 0 && off + 1 <= size)
    Memory.storeByte(mem, off, b)
  }

  def storeBytes(off: Long, bytes: Array[Byte]) {
    storeBytes(off, bytes, 0, bytes.length)
  }

  def storeBytes(off: Long, bytes: Array[Byte], bytesOff: Long, n: Int) {
    assert(size <= capacity)
    assert(off >= 0 && off + n <= size)
    assert(bytesOff + n <= bytes.length)
    Memory.memcpy(mem, off, bytes, bytesOff, n)
  }

  def ensure(n: Long) {
    val required = size + n
    if (capacity < required) {
      val newLength = (capacity * 2).max(required)
      mem = util.Arrays.copyOf(mem, newLength.toInt)
    }
  }

  private def align(alignment: Long) {
    assert(alignment > 0)
    assert((alignment & (alignment - 1)) == 0) // power of 2
    end = (end + (alignment - 1)) & ~(alignment - 1)
  }

  private def allocate(n: Long): Long = {
    assert(n >= 0)
    val off = end
    ensure(n)
    end += n
    off
  }

  def allocate(alignment: Long, n: Long): Long = {
    align(alignment)
    allocate(n)
  }

  def loadBoolean(off: Long): Boolean = {
    val b = loadByte(off)
    assert(b == 1 || b == 0)
    b != 0
  }

  def loadBit(byteOff: Long, bitOff: Long): Boolean = {
    val b = byteOff + (bitOff >> 3)
    (loadByte(b) & (1 << (bitOff & 7))) != 0
  }

  def setBit(byteOff: Long, bitOff: Long) {
    val b = byteOff + (bitOff >> 3)
    storeByte(b,
      (loadByte(b) | (1 << (bitOff & 7))).toByte)
  }

  def clearBit(byteOff: Long, bitOff: Long) {
    val b = byteOff + (bitOff >> 3)
    storeByte(b,
      (loadByte(b) & ~(1 << (bitOff & 7))).toByte)
  }

  def storeBit(byteOff: Long, bitOff: Long, b: Boolean) {
    if (b)
      setBit(byteOff, bitOff)
    else
      clearBit(byteOff, bitOff)
  }

  def appendInt(i: Int): Long = {
    val off = allocate(4, 4)
    storeInt(off, i)
    off
  }

  def appendBoolean(b: Boolean): Long =
    appendByte(b.toByte)

  def appendLong(l: Long): Long = {
    val off = allocate(8, 8)
    storeLong(off, l)
    off
  }

  def appendFloat(f: Float): Long = {
    val off = allocate(4, 4)
    storeFloat(off, f)
    off
  }

  def appendDouble(d: Double): Long = {
    val off = allocate(8, 8)
    storeDouble(off, d)
    off
  }

  def appendByte(b: Byte): Long = {
    val off = allocate(1)
    storeByte(off, b)
    off
  }

  def appendBytes(bytes: Array[Byte]): Long = {
    val off = allocate(bytes.length)
    storeBytes(off, bytes)
    off
  }

  def appendBytes(bytes: Array[Byte], bytesOff: Long, n: Int): Long = {
    assert(bytesOff + n <= bytes.length)
    val off = allocate(n)
    storeBytes(off, bytes, bytesOff, n)
    off
  }

  def appendBinary(bytes: Array[Byte]): Long = {
    align(TBinary.contentAlignment)
    val startOff = appendInt(bytes.length)
    appendBytes(bytes)
    startOff
  }

  def appendString(s: String): Long =
    appendBinary(s.getBytes)

  def appendArrayInt(a: Array[Int]): Long = {
    val off = appendInt(a.length)
    var i = 0
    while (i < a.length) {
      appendInt(a(i))
      i += 1
    }
    off
  }

  def clear() {
    end = 0
  }

  def setFrom(from: OldRegion) {
    if (from.end > capacity) {
      val newLength = math.max((capacity * 3) / 2, from.end)
      mem = new Array[Byte](newLength.toInt)
    }
    Memory.memcpy(mem, 0, from.mem, 0, from.end)
    end = from.end
  }

  def copy(): OldRegion = {
    new OldRegion(util.Arrays.copyOf(mem, end.toInt), end)
  }

  override def write(kryo: Kryo, output: Output) {
    output.writeLong(end)

    assert(end <= Int.MaxValue)
    val smallEnd = end.toInt
    output.write(mem, 0, smallEnd)
  }

  override def read(kryo: Kryo, input: Input) {
    end = input.readLong()
    assert(end <= Int.MaxValue)
    val smallEnd = end.toInt
    mem = new Array[Byte](smallEnd)
    input.read(mem)
  }

  private def writeObject(out: ObjectOutputStream) {
    out.writeLong(end)

    assert(end <= Int.MaxValue)
    val smallEnd = end.toInt
    out.write(mem, 0, smallEnd)
  }

  private def readObject(in: ObjectInputStream) {
    end = in.readLong()
    assert(end <= Int.MaxValue)
    val smallOffset = end.toInt
    mem = new Array[Byte](smallOffset)
    in.read(mem)
  }

  /*
  def visit(t: Type, off: Long, v: ValueVisitor) {
    t match {
      case _: TBoolean => v.visitBoolean(loadBoolean(off))
      case _: TInt32 => v.visitInt32(loadInt(off))
      case _: TInt64 => v.visitInt64(loadLong(off))
      case _: TFloat32 => v.visitFloat32(loadFloat(off))
      case _: TFloat64 => v.visitFloat64(loadDouble(off))
      case _: TString =>
        val boff = off
        v.visitString(TString.loadString(this, boff))
      case _: TBinary =>
        val boff = off
        val length = TBinary.loadLength(this, boff)
        val b = loadBytes(TBinary.bytesOffset(boff), length)
        v.visitBinary(b)
      case t: TContainer =>
        val aoff = off
        val length = t.loadLength(this, aoff)
        v.enterArray(t, length)
        var i = 0
        while (i < length) {
          v.enterElement(i)
          if (t.isElementDefined(this, aoff, i))
            visit(t.elementType, t.loadElement(this, aoff, length, i), v)
          else
            v.visitMissing(t.elementType)
          i += 1
        }
        v.leaveArray()
      case t: TStruct =>
        v.enterStruct(t)
        var i = 0
        while (i < t.size) {
          val f = t.fields(i)
          v.enterField(f)
          if (t.isFieldDefined(this, off, i))
            visit(f.typ, t.loadField(this, off, i), v)
          else
            v.visitMissing(f.typ)
          v.leaveField()
          i += 1
        }
        v.leaveStruct()
      case t: TTuple =>
        v.enterTuple(t)
        var i = 0
        while (i < t.size) {
          v.enterElement(i)
          if (t.isFieldDefined(this, off, i))
            visit(t.types(i), t.loadField(this, off, i), v)
          else
            v.visitMissing(t.types(i))
          v.leaveElement()
          i += 1
        }
        v.leaveTuple()
      case t: ComplexType =>
        visit(t.representation, off, v)
    }
  }

  def pretty(t: Type, off: Long): String = {
    val v = new PrettyVisitor()
    visit(t, off, v)
    v.result()
  }
  */

  def close(): Unit = ()
}
