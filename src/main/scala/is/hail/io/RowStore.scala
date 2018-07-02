package is.hail.io

import is.hail.annotations._
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.types._
import is.hail.io.compress.LZ4Utils
import is.hail.nativecode._
import is.hail.rvd.{OrderedRVDPartitioner, OrderedRVDSpec, RVDContext, RVDSpec, UnpartitionedRVDSpec}
import is.hail.sparkextras._
import is.hail.utils._
import org.apache.spark.rdd.RDD
import org.json4s.{Extraction, JValue}
import org.json4s.jackson.JsonMethods
import java.io.{Closeable, InputStream, OutputStream, PrintWriter}
import scala.collection.mutable.ArrayBuffer

import is.hail.asm4s._
import is.hail.expr.ir.{EmitUtils, EstimableEmitter, MethodBuilderLike}
import is.hail.utils.richUtils.ByteTrackingOutputStream
import org.apache.spark.{ExposedMetrics, TaskContext}

trait BufferSpec extends Serializable {
  def buildInputBuffer(in: InputStream): InputBuffer

  def buildOutputBuffer(out: OutputStream): OutputBuffer
}

final class LEB128BufferSpec(child: BufferSpec) extends BufferSpec {
  def buildInputBuffer(in: InputStream): InputBuffer = new LEB128InputBuffer(child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBuffer = new LEB128OutputBuffer(child.buildOutputBuffer(out))
}

final class BlockingBufferSpec(blockSize: Int, child: BlockBufferSpec) extends BufferSpec {
  def buildInputBuffer(in: InputStream): InputBuffer = new BlockingInputBuffer(blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBuffer = new BlockingOutputBuffer(blockSize, child.buildOutputBuffer(out))
}

trait BlockBufferSpec extends Serializable {
  def buildInputBuffer(in: InputStream): InputBlockBuffer

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer
}

final class LZ4BlockBufferSpec(blockSize: Int, child: BlockBufferSpec) extends BlockBufferSpec {
  def buildInputBuffer(in: InputStream): InputBlockBuffer = new LZ4InputBlockBuffer(blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new LZ4OutputBlockBuffer(blockSize, child.buildOutputBuffer(out))
}

object StreamBlockBufferSpec {
  def extract(jv: JValue): StreamBlockBufferSpec = new StreamBlockBufferSpec
}

final class StreamBlockBufferSpec extends BlockBufferSpec {
  def buildInputBuffer(in: InputStream): InputBlockBuffer = new StreamBlockInputBuffer(in)

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new StreamBlockOutputBuffer(out)
}

object CodecSpec {
  val default: CodecSpec = new PackCodecSpec(
    new LEB128BufferSpec(
      new BlockingBufferSpec(32 * 1024,
        new LZ4BlockBufferSpec(32 * 1024,
          new StreamBlockBufferSpec))))

  val defaultUncompressed = new PackCodecSpec(
    new BlockingBufferSpec(32 * 1024,
      new StreamBlockBufferSpec))

  val blockSpecs: Array[BufferSpec] = Array(
    new BlockingBufferSpec(64 * 1024,
      new StreamBlockBufferSpec),
    new BlockingBufferSpec(32 * 1024,
      new LZ4BlockBufferSpec(32 * 1024,
        new StreamBlockBufferSpec)))

  val bufferSpecs: Array[BufferSpec] = blockSpecs.flatMap { blockSpec =>
    Array(blockSpec,
      new LEB128BufferSpec(blockSpec))
  }

  val codecSpecs: Array[CodecSpec] = bufferSpecs.flatMap { bufferSpec =>
    Array(new PackCodecSpec(bufferSpec))
  }

  val supportedCodecSpecs: Array[CodecSpec] = bufferSpecs.flatMap { bufferSpec =>
    Array(new PackCodecSpec(bufferSpec))
  }
}

trait CodecSpec extends Serializable {
  def buildEncoder(t: Type): (OutputStream) => Encoder

  def buildDecoder(t: Type, requestedType: Type): (InputStream) => Decoder

  override def toString: String = {
    implicit val formats = RVDSpec.formats
    val jv = Extraction.decompose(this)
    JsonMethods.compact(JsonMethods.render(jv))
  }
}

final case class PackCodecSpec(child: BufferSpec) extends CodecSpec {

  def buildEncoder(t: Type): (OutputStream) => Encoder = { out: OutputStream =>
    new PackEncoder(t, child.buildOutputBuffer(out))
  }

  // def buildDecoder(t: Type)(in: InputStream): Decoder = new PackDecoder(t, child.buildInputBuffer(in))

  def buildDecoder(t: Type, requestedType: Type): (InputStream) => Decoder = {
    System.err.println("DEBUG: PackCodecSpec using CompiledPackDecoder")
    val sb = new StringBuilder()
    NativeDecode.appendCode(sb, t, requestedType)
    val mod = new NativeModule("", sb.toString(), false)
    val st = new NativeStatus()
    mod.findOrBuild(st)
    if (st.fail) System.err.println("findOrBuild ${st}")
    st.clear()
    val makeDecoder = mod.findPtrFuncL0(st, "make_decoder")
    if (st.fail) System.err.println("make_decoder ${st}")
    st.clear()
    val decodeRvFunc = mod.findLongFuncL3(st, "decode_until_done_or_need_push")
    if (st.fail) System.err.println("decode_until_done_or_need_push")
    val decodeOneByteFunc = mod.findLongFuncL2(st, "decode_one_byte")
    if (st.fail) System.err.println("decode_one_byte")
    mod.close()
    System.err.println(s"DEBUG: NativeDecode ${sb}")
    throw new IllegalArgumentException("getting out")
    val f = EmitPackDecoder(t, requestedType)
    (in: InputStream) => new CompiledPackDecoder(child.buildInputBuffer(in), f)
  }
}

trait OutputBlockBuffer extends Closeable {
  def writeBlock(buf: Array[Byte], len: Int): Unit
}

trait InputBlockBuffer extends Closeable {
  def close(): Unit

  def readBlock(buf: Array[Byte]): Int
}

final class StreamBlockOutputBuffer(out: OutputStream) extends OutputBlockBuffer {
  private val lenBuf = new Array[Byte](4)

  def close() {
    out.close()
  }

  def writeBlock(buf: Array[Byte], len: Int): Unit = {
    Memory.storeInt(lenBuf, 0, len)
    out.write(lenBuf, 0, 4)
    out.write(buf, 0, len)
  }
}

final class StreamBlockInputBuffer(in: InputStream) extends InputBlockBuffer {
  private val lenBuf = new Array[Byte](4)

  def close() {
    in.close()
  }

  def readBlock(buf: Array[Byte]): Int = {
    in.readFully(lenBuf, 0, 4)
    val len = Memory.loadInt(lenBuf, 0)
    in.readFully(buf, 0, len)
    len
  }
}

trait OutputBuffer extends Closeable {
  def flush(): Unit

  def close(): Unit

  def writeByte(b: Byte): Unit

  def writeInt(i: Int): Unit

  def writeLong(l: Long): Unit

  def writeFloat(f: Float): Unit

  def writeDouble(d: Double): Unit

  def writeBytes(region: Region, off: Long, n: Int): Unit

  def writeDoubles(from: Array[Double], fromOff: Int, n: Int): Unit

  def writeDoubles(from: Array[Double]): Unit = writeDoubles(from, 0, from.length)

  def writeBoolean(b: Boolean) {
    writeByte(b.toByte)
  }
}

final class LEB128OutputBuffer(out: OutputBuffer) extends OutputBuffer {
  def flush(): Unit = out.flush()

  def close() {
    out.close()
  }

  def writeByte(b: Byte): Unit = out.writeByte(b)

  def writeInt(i: Int): Unit = {
    var j = i
    do {
      var b = j & 0x7f
      j >>>= 7
      if (j != 0)
        b |= 0x80
      out.writeByte(b.toByte)
    } while (j != 0)
  }

  def writeLong(l: Long): Unit = {
    var j = l
    do {
      var b = j & 0x7f
      j >>>= 7
      if (j != 0)
        b |= 0x80
      out.writeByte(b.toByte)
    } while (j != 0)
  }

  def writeFloat(f: Float): Unit = out.writeFloat(f)

  def writeDouble(d: Double): Unit = out.writeDouble(d)

  def writeBytes(region: Region, off: Long, n: Int): Unit = out.writeBytes(region, off, n)

  def writeDoubles(from: Array[Double], fromOff: Int, n: Int): Unit = out.writeDoubles(from, fromOff, n)
}

final class LZ4OutputBlockBuffer(blockSize: Int, out: OutputBlockBuffer) extends OutputBlockBuffer {
  private val comp = new Array[Byte](4 + LZ4Utils.maxCompressedLength(blockSize))

  def close() {
    out.close()
  }

  def writeBlock(buf: Array[Byte], decompLen: Int): Unit = {
    val compLen = LZ4Utils.compress(comp, 4, buf, decompLen)
    Memory.storeInt(comp, 0, decompLen) // decompLen
    out.writeBlock(comp, compLen + 4)
  }
}

final class BlockingOutputBuffer(blockSize: Int, out: OutputBlockBuffer) extends OutputBuffer {
  private val buf: Array[Byte] = new Array[Byte](blockSize)
  private var off: Int = 0

  private def writeBlock() {
    out.writeBlock(buf, off)
    off = 0
  }

  def flush() {
    writeBlock()
  }

  def close() {
    flush()
    out.close()
  }

  def writeByte(b: Byte) {
    if (off + 1 > buf.length)
      writeBlock()
    Memory.storeByte(buf, off, b)
    off += 1
  }

  def writeInt(i: Int) {
    if (off + 4 > buf.length)
      writeBlock()
    Memory.storeInt(buf, off, i)
    off += 4
  }

  def writeLong(l: Long) {
    if (off + 8 > buf.length)
      writeBlock()
    Memory.storeLong(buf, off, l)
    off += 8
  }

  def writeFloat(f: Float) {
    if (off + 4 > buf.length)
      writeBlock()
    Memory.storeFloat(buf, off, f)
    off += 4
  }

  def writeDouble(d: Double) {
    if (off + 8 > buf.length)
      writeBlock()
    Memory.storeDouble(buf, off, d)
    off += 8
  }

  def writeBytes(fromRegion: Region, fromOff0: Long, n0: Int) {
    assert(n0 >= 0)
    var fromOff = fromOff0
    var n = n0

    while (off + n > buf.length) {
      val p = buf.length - off
      fromRegion.loadBytes(fromOff, buf, off, p)
      off += p
      fromOff += p
      n -= p
      assert(off == buf.length)
      writeBlock()
    }
    fromRegion.loadBytes(fromOff, buf, off, n)
    off += n
  }

  def writeDoubles(from: Array[Double], fromOff0: Int, n0: Int) {
    assert(n0 >= 0)
    assert(fromOff0 >= 0)
    assert(fromOff0 <= from.length - n0)
    var fromOff = fromOff0
    var n = n0

    while (off + (n << 3) > buf.length) {
      val p = (buf.length - off) >>> 3
      Memory.memcpy(buf, off, from, fromOff, p)
      off += (p << 3)
      fromOff += p
      n -= p
      writeBlock()
    }
    Memory.memcpy(buf, off, from, fromOff, n)
    off += (n << 3)
  }
}

trait InputBuffer extends Closeable {
  def close(): Unit

  def readByte(): Byte

  def readInt(): Int

  def readLong(): Long

  def readFloat(): Float

  def readDouble(): Double

  def readBytes(toRegion: Region, toOff: Long, n: Int): Unit

  def skipBoolean(): Unit = skipByte()

  def skipByte(): Unit

  def skipInt(): Unit

  def skipLong(): Unit

  def skipFloat(): Unit

  def skipDouble(): Unit

  def skipBytes(n: Int): Unit

  def readDoubles(to: Array[Double], off: Int, n: Int): Unit

  def readDoubles(to: Array[Double]): Unit = readDoubles(to, 0, to.length)

  def readBoolean(): Boolean = readByte() != 0
}

final class LEB128InputBuffer(in: InputBuffer) extends InputBuffer {
  def close() {
    in.close()
  }

  def readByte(): Byte = in.readByte()

  def readInt(): Int = {
    var b: Byte = readByte()
    var x: Int = b & 0x7f
    var shift: Int = 7
    while ((b & 0x80) != 0) {
      b = readByte()
      x |= ((b & 0x7f) << shift)
      shift += 7
    }
    x
  }

  def readLong(): Long = {
    var b: Byte = readByte()
    var x: Long = b & 0x7fL
    var shift: Int = 7
    while ((b & 0x80) != 0) {
      b = readByte()
      x |= ((b & 0x7fL) << shift)
      shift += 7
    }
    x
  }

  def readFloat(): Float = in.readFloat()

  def readDouble(): Double = in.readDouble()

  def readBytes(toRegion: Region, toOff: Long, n: Int): Unit = in.readBytes(toRegion, toOff, n)

  def skipByte(): Unit = in.skipByte()

  def skipInt() {
    var b: Byte = readByte()
    while ((b & 0x80) != 0)
      b = readByte()
  }

  def skipLong() {
    var b: Byte = readByte()
    while ((b & 0x80) != 0)
      b = readByte()
  }

  def skipFloat(): Unit = in.skipFloat()

  def skipDouble(): Unit = in.skipDouble()

  def skipBytes(n: Int): Unit = in.skipBytes(n)

  def readDoubles(to: Array[Double], toOff: Int, n: Int): Unit = in.readDoubles(to, toOff, n)
}

final class LZ4InputBlockBuffer(blockSize: Int, in: InputBlockBuffer) extends InputBlockBuffer {
  private val comp = new Array[Byte](4 + LZ4Utils.maxCompressedLength(blockSize))

  def close() {
    in.close()
  }

  def readBlock(buf: Array[Byte]): Int = {
    val blockLen = in.readBlock(comp)
    val compLen = blockLen - 4
    val decompLen = Memory.loadInt(comp, 0)

    LZ4Utils.decompress(buf, 0, decompLen, comp, 4, compLen)

    decompLen
  }
}

final class BlockingInputBuffer(blockSize: Int, in: InputBlockBuffer) extends InputBuffer {
  private val buf = new Array[Byte](blockSize)
  private var end: Int = 0
  private var off: Int = 0

  private def readBlock() {
    assert(off == end)
    end = in.readBlock(buf)
    off = 0
  }

  private def ensure(n: Int) {
    if (off == end)
      readBlock()
    assert(off + n <= end)
  }

  def close() {
    in.close()
  }

  def readByte(): Byte = {
    ensure(1)
    val b = Memory.loadByte(buf, off)
    off += 1
    b
  }

  def readInt(): Int = {
    ensure(4)
    val i = Memory.loadInt(buf, off)
    off += 4
    i
  }

  def readLong(): Long = {
    ensure(8)
    val l = Memory.loadLong(buf, off)
    off += 8
    l
  }

  def readFloat(): Float = {
    ensure(4)
    val f = Memory.loadFloat(buf, off)
    off += 4
    f
  }

  def readDouble(): Double = {
    ensure(8)
    val d = Memory.loadDouble(buf, off)
    off += 8
    d
  }

  def readBytes(toRegion: Region, toOff0: Long, n0: Int) {
    assert(n0 >= 0)
    var toOff = toOff0
    var n = n0

    while (n > 0) {
      if (end == off)
        readBlock()
      val p = math.min(end - off, n)
      assert(p > 0)
      toRegion.storeBytes(toOff, buf, off, p)
      toOff += p
      n -= p
      off += p
    }
  }

  def skipByte() {
    ensure(1)
    off += 1
  }

  def skipInt() {
    ensure(4)
    off += 4
  }

  def skipLong() {
    ensure(8)
    off += 8
  }

  def skipFloat() {
    ensure(4)
    off += 4
  }

  def skipDouble() {
    ensure(8)
    off += 8
  }

  def skipBytes(n0: Int) {
    var n = n0
    while (n > 0) {
      if (end == off)
        readBlock()
      val p = math.min(end - off, n)
      n -= p
      off += p
    }
  }

  def readDoubles(to: Array[Double], toOff0: Int, n0: Int) {
    assert(toOff0 >= 0)
    assert(n0 >= 0)
    assert(toOff0 <= to.length - n0)
    var toOff = toOff0
    var n = n0

    while (n > 0) {
      if (end == off)
        readBlock()
      val p = math.min(end - off, n << 3) >>> 3
      assert(p > 0)
      Memory.memcpy(to, toOff, buf, off, p)
      toOff += p
      n -= p
      off += (p << 3)
    }
  }
}

trait Decoder extends Closeable {
  def close()

  def readRegionValue(region: Region): Long

  def readByte(): Byte
}

class MethodBuilderSelfLike(val mb: MethodBuilder) extends MethodBuilderLike[MethodBuilderSelfLike] {
  type MB = MethodBuilder

  def newMethod(paramInfo: Array[TypeInfo[_]], returnInfo: TypeInfo[_]): MethodBuilderSelfLike =
    new MethodBuilderSelfLike(mb.fb.newMethod(paramInfo, returnInfo))
}

object EmitPackDecoder {
  self =>

  type Emitter = EstimableEmitter[MethodBuilderSelfLike]

  def emitTypeSize(t: Type): Int = {
    t match {
      case t: TArray => 120 + emitTypeSize(t.elementType)
      case t: TStruct => 100
      case _ => 20
    }
  }

  def emitBinary(
    t: TBinary,
    mb: MethodBuilder,
    in: Code[InputBuffer],
    srvb: StagedRegionValueBuilder): Code[Unit] = {
    val length = mb.newLocal[Int]
    val boff = mb.newLocal[Long]

    Code(
      length := in.readInt(),
      boff := srvb.allocateBinary(length),
      in.readBytes(srvb.region, boff + const(4), length))
  }

  def emitBaseStruct(
    t: TBaseStruct,
    requestedType: TBaseStruct,
    mb: MethodBuilder,
    in: Code[InputBuffer],
    srvb: StagedRegionValueBuilder): Code[Unit] = {
    val region = srvb.region

    val moff = mb.newField[Long]

    val initCode = Code(
      srvb.start(init = true),
      moff := region.allocate(const(1), const(t.nMissingBytes)),
      in.readBytes(region, moff, t.nMissingBytes))

    val fieldEmitters = new Array[Emitter](t.size)

    assert(t.isInstanceOf[TTuple] || t.isInstanceOf[TStruct])

    var i = 0
    var j = 0
    while (i < t.size) {
      val f = t.fields(i)
      fieldEmitters(i) =
        if (t.isInstanceOf[TTuple] ||
          (j < requestedType.size && requestedType.fields(j).name == f.name)) {
          val rf = requestedType.fields(j)
          assert(f.typ.required == rf.typ.required)
          j += 1

          new Emitter {
            def emit(mbLike: MethodBuilderSelfLike): Code[Unit] = {
              val readElement = self.emit(f.typ, rf.typ, mbLike.mb, in, srvb)
              Code(
                if (f.typ.required)
                  readElement
                else {
                  region.loadBit(moff, const(t.missingIdx(f.index))).mux(
                    srvb.setMissing(),
                    readElement)
                },
                srvb.advance())
            }

            def estimatedSize: Int = emitTypeSize(f.typ)
          }
        } else {
          new Emitter {
            def emit(mbLike: MethodBuilderSelfLike): Code[Unit] = {
              val skipField = skip(f.typ, mbLike.mb, in, region)
              if (f.typ.required)
                skipField
              else {
                region.loadBit(moff, const(t.missingIdx(f.index))).mux(
                  Code._empty,
                  skipField)
              }
            }

            def estimatedSize: Int = emitTypeSize(f.typ)
          }
        }
      i += 1
    }
    assert(j == requestedType.size)

    Code(initCode,
      EmitUtils.wrapToMethod(fieldEmitters, new MethodBuilderSelfLike(mb)),
      Code._empty)
  }

  def emitArray(
    t: TArray,
    requestedType: TArray,
    mb: MethodBuilder,
    in: Code[InputBuffer],
    srvb: StagedRegionValueBuilder): Code[Unit] = {
    val length = mb.newLocal[Int]
    val i = mb.newLocal[Int]
    val aoff = mb.newLocal[Long]

    Code(
      length := in.readInt(),
      srvb.start(length, init = false),
      aoff := srvb.offset,
      srvb.region.storeInt(aoff, length),
      if (t.elementType.required)
        Code._empty
      else
        in.readBytes(srvb.region, aoff + const(4), (length + 7) >>> 3),
      i := 0,
      Code.whileLoop(
        i < length,
        Code({
          val readElement = emit(t.elementType, requestedType.elementType, mb, in, srvb)
          if (t.elementType.required)
            readElement
          else
            t.isElementDefined(srvb.region, aoff, i).mux(
              readElement,
              srvb.setMissing())
        },
          srvb.advance(),
          i := i + const(1))))
  }

  def skipBaseStruct(t: TBaseStruct, mb: MethodBuilder, in: Code[InputBuffer], region: Code[Region]): Code[Unit] = {
    val moff = mb.newField[Long]

    val fieldEmitters = t.fields.map { f =>
      new Emitter {
        def emit(mbLike: MethodBuilderSelfLike): Code[Unit] = {
          val skipField = skip(f.typ, mbLike.mb, in, region)
          if (f.typ.required)
            skipField
          else
            region.loadBit(moff, const(t.missingIdx(f.index))).mux(
              Code._empty,
              skipField)
        }

        def estimatedSize: Int = emitTypeSize(f.typ)
      }
    }

    Code(
      moff := region.allocate(const(1), const(t.nMissingBytes)),
      in.readBytes(region, moff, t.nMissingBytes),
      EmitUtils.wrapToMethod(fieldEmitters, new MethodBuilderSelfLike(mb)))
  }

  def skipArray(t: TArray,
    mb: MethodBuilder,
    in: Code[InputBuffer],
    region: Code[Region]): Code[Unit] = {
    val length = mb.newLocal[Int]
    val i = mb.newLocal[Int]

    if (t.elementType.required) {
      Code(
        length := in.readInt(),
        i := 0,
        Code.whileLoop(i < length,
          Code(
            skip(t.elementType, mb, in, region),
            i := i + const(1))))
    } else {
      val moff = mb.newLocal[Long]
      val nMissing = mb.newLocal[Int]
      Code(
        length := in.readInt(),
        nMissing := ((length + 7) >>> 3),
        moff := region.allocate(const(1), nMissing.toL),
        in.readBytes(region, moff, nMissing),
        i := 0,
        Code.whileLoop(i < length,
          region.loadBit(moff, i.toL).mux(
            Code._empty,
            skip(t.elementType, mb, in, region)),
          i := i + const(1)))
    }
  }

  def skipBinary(t: Type, mb: MethodBuilder, in: Code[InputBuffer]): Code[Unit] = {
    val length = mb.newLocal[Int]
    Code(
      length := in.readInt(),
      in.skipBytes(length))
  }

  def skip(t: Type, mb: MethodBuilder, in: Code[InputBuffer], region: Code[Region]): Code[Unit] = {
    t match {
      case t2: TBaseStruct =>
        skipBaseStruct(t2, mb, in, region)
      case t2: TArray =>
        skipArray(t2, mb, in, region)
      case _: TBoolean => in.skipBoolean()
      case _: TInt64 => in.skipLong()
      case _: TInt32 => in.skipInt()
      case _: TFloat32 => in.skipFloat()
      case _: TFloat64 => in.skipDouble()
      case t2: TBinary => skipBinary(t2, mb, in)
    }
  }

  def emit(
    t: Type,
    requestedType: Type,
    mb: MethodBuilder,
    in: Code[InputBuffer],
    srvb: StagedRegionValueBuilder): Code[Unit] = {
    t match {
      case t2: TBaseStruct =>
        val requestedType2 = requestedType.asInstanceOf[TBaseStruct]
        srvb.addBaseStruct(requestedType2, { srvb2 =>
          emitBaseStruct(t2, requestedType2, mb, in, srvb2)
        })
      case t2: TArray =>
        val requestedType2 = requestedType.asInstanceOf[TArray]
        srvb.addArray(requestedType2, { srvb2 =>
          emitArray(t2, requestedType2, mb, in, srvb2)
        })
      case _: TBoolean => srvb.addBoolean(in.readBoolean())
      case _: TInt64 => srvb.addLong(in.readLong())
      case _: TInt32 => srvb.addInt(in.readInt())
      case _: TFloat32 => srvb.addFloat(in.readFloat())
      case _: TFloat64 => srvb.addDouble(in.readDouble())
      case t2: TBinary => emitBinary(t2, mb, in, srvb)
    }
  }

  def apply(t: Type, requestedType: Type): () => AsmFunction2[Region, InputBuffer, Long] = {
    val fb = new Function2Builder[Region, InputBuffer, Long]
    val mb = fb.apply_method
    val in = mb.getArg[InputBuffer](2).load()
    val srvb = new StagedRegionValueBuilder(mb, requestedType)

    var c = t.fundamentalType match {
      case t: TBaseStruct =>
        emitBaseStruct(t, requestedType.fundamentalType.asInstanceOf[TBaseStruct], mb, in, srvb)
      case t: TArray =>
        emitArray(t, requestedType.fundamentalType.asInstanceOf[TArray], mb, in, srvb)
    }

    mb.emit(Code(
      c,
      Code._return(srvb.end())))

    fb.result()
  }
}

//
// Generate the Type-specific C++ code for a PackDecoder
//
object NativeDecode {

  def appendCode(sb: StringBuilder, rowType: Type, wantType: Type): Unit = {
    var seen = new ArrayBuffer[Int]()
    val stateDefs = new StringBuilder()
    val localDefs = new StringBuilder()
    val flushCode = new StringBuilder()
    val entryCode = new StringBuilder()
    val mainCode = new StringBuilder()
    
    def stateVarType(name: String): String = {
      name match {
        case "len" => "ssize_t"
        case "idx" => "ssize_t"
        case _ => "char*"
      }
    }

    def stateVar(name: String, depth: Int): String = {
      val bit = name match {
        case "len"  => 0x01
        case "idx"  => 0x02
        case "addr" => 0x04
        case "ptr"  => 0x08
        case "data" => 0x10
        case "miss" => 0x20
      }
      if (seen.length <= depth) seen = seen.padTo(depth+1, 0)
      val result = s"${name}${depth}"
      if ((seen(depth) & bit) == 0) {
        seen(depth) = (seen(depth) | bit)
        val typ = stateVarType(name)
        val initVal = if (!typ.equals("char*")) "0"
        else if (depth == 0) "&rv_base_"
        else "nullptr"
        stateDefs.append(s"  ${typ} ${result}_ = ${initVal};\n")
        localDefs.append(s"    ${typ} ${result} = ${result}_;\n")
        flushCode.append(s"    ${result}_ = ${result};\n")
      }
      result
    }

    var numStates = 0
    def allocState(name: String): Int = {
      val s = numStates
      numStates += 1
      entryCode.append(s"      case ${s}: goto entry${s};\n")
      mainCode.append(s"    entry${s}: // ${name}\n")
      s
    }

    def isResumePoint(t: Type): Boolean = {
      t match {
        case _: TBaseStruct => false
        case _ => true
      }
    }

    def scan(depth: Int, numIndent: Int, name: String, typ: Type, wantType: Type, skip: Boolean) {
      val r1 = if (isResumePoint(typ)) allocState(name) else -1
      val addr = if (skip) "addr_undefined" else stateVar("addr", depth)
      val ind = "  " * numIndent
      typ.fundamentalType match {
        case t: TBoolean =>
          val call = if (skip) "skip_byte()" else s"decode_byte((int8_t*)${addr})"
          mainCode.append(s"${ind}  if (!${call}) { s = ${r1}; goto pull; }\n")
        case t: TInt32 =>
          val call = if (skip) "skip_int()" else s"decode_int((int32_t*)${addr})"
          mainCode.append(s"${ind}  if (!${call}) { s = ${r1}; goto pull; }\n")
        case t: TInt64 =>
          val call = if (skip) "skip_long()" else s"decode_long((int64_t*)${addr})"
          mainCode.append(s"${ind}  if (!${call}) { s = ${r1}; goto pull; }\n")
        case t: TFloat32 =>
          val call = if (skip) "skip_float()" else s"decode_float((float*)${addr})"
          mainCode.append(s"${ind}  if (!${call}) { s = ${r1}; goto pull; }\n")
        case t: TFloat64 =>
          val call = if (skip) "skip_double()" else s"decode_double((double*)${addr})"
          mainCode.append(s"${ind}  if (!${call}) { s = ${r1}; goto pull; }\n")

        case t: TBinary =>
          // TBinary - usually a string - has an int length, followed by that number of bytes
          val ptr = stateVar("ptr", depth)
          val len = stateVar("len", depth)
          val idx = stateVar("idx", depth)
          mainCode.append(s"${ind}  if (!decode_length(&${len})) { s = ${r1}; goto pull; }\n")
          if (skip) {
            mainCode.append(s"${ind}  for (${idx} = 0; ${idx} < ${len};) {\n")
            val r2 = allocState(s"${name}.bytes");
            mainCode.append(s"${ind}    auto ngot = skip_bytes(${len}-${idx});\n")
            mainCode.append(s"${ind}    if (ngot <= 0) { s = ${r2}; goto pull; }\n")
            mainCode.append(s"${ind}    ${idx} += ngot;\n")
            mainCode.append(s"${ind}  }\n")            
          } else {
            mainCode.append(s"${ind}  ${ptr} = region->allocate(4, 4+${len});\n")
            mainCode.append(s"${ind}  *(int32_t*)${ptr} = ${len};\n")
            mainCode.append(s"${ind}  for (${idx} = 0; ${idx} < ${len};) {\n")
            val r2 = allocState(s"${name}.bytes");
            mainCode.append(s"${ind}    auto ngot = decode_bytes(${ptr}+4+${idx}, ${len}-${idx});\n")
            mainCode.append(s"${ind}    if (ngot <= 0) { s = ${r2}; goto pull; }\n")
            mainCode.append(s"${ind}    ${idx} += ngot;\n")
            mainCode.append(s"${ind}  }\n")
          }

        case t: TArray =>
          val ptr = stateVar("ptr", depth)
          val len = stateVar("len", depth)
          val idx = stateVar("idx", depth)
          val data = if (skip) "data_undefined" else stateVar("data", depth)
          mainCode.append(s"${ind}  if (!decode_length(&${len})) { s = ${r1}; goto pull; }\n")
          val wantArray = wantType.asInstanceOf[TArray]
          val grain = if (wantArray.elementType.alignment > 4) t.elementType.alignment else 4
          val esize = wantArray.elementType.byteSize
          val req = if (t.elementType.required) "true" else "false"          
          mainCode.append(s"${ind}  { ssize_t data_offset = elements_offset(${len}, ${req}, ${grain});\n")
          var haveTmpMissing = false
          if (skip) {
            if (!t.elementType.required) {
              haveTmpMissing = true
              mainCode.append(s"${ind}   ${ptr} = (char*)malloc(data_offset);\n")
            }
            mainCode.append(s"${ind}  }\n")
          } else {
            mainCode.append(s"${ind}    ssize_t size = data_offset + ${esize}*${len};\n")
            mainCode.append(s"${ind}    ${ptr} = region->allocate(${grain}, size);\n");
            mainCode.append(s"${ind}    ${data} = ${ptr} + data_offset;\n")
            mainCode.append(s"${ind}  }\n")
            mainCode.append(s"${ind}  *(char**)${addr} = ${ptr};\n")
            mainCode.append(s"${ind}  *(int32_t*)${ptr} = ${len};\n")
          }
          if (!t.elementType.required) {
            mainCode.append(s"${ind}  for (${idx} = 0; ${idx} < missing_bytes(${len});) {\n")
            val r2 = allocState(s"${name}.missing");
            mainCode.append(s"${ind}    auto ngot = decode_bytes(${ptr}+4+${idx}, missing_bytes(${len})-${idx});\n")
            mainCode.append(s"${ind}    if (ngot <= 0) { s = ${r2}; goto pull; }\n")
            mainCode.append(s"${ind}    ${idx} += ngot;\n")
            mainCode.append(s"${ind}  }\n")
          }
          mainCode.append(  s"${ind}  for (${idx} = 0; ${idx} < ${len};) {\n")
          if (!t.elementType.required) {
            mainCode.append(s"${ind}    if (is_missing(${ptr}+4, ${idx})) continue;\n")
          }
          if (!skip) {
            mainCode.append(  s"${ind}    ${stateVar("addr", depth+1)} = ${data} + ${idx}*${esize};\n")
          }
          scan(depth+1, numIndent+1, s"${name}(${idx})", t.elementType, wantArray.elementType, skip)
          mainCode.append(  s"${ind}  }\n")
          if (haveTmpMissing) {
            mainCode.append(s"${ind}  free(${ptr});\n")
          }

        case t: TBaseStruct =>
          val wantStruct = wantType.fundamentalType.asInstanceOf[TBaseStruct];
          val ptr = stateVar("ptr", depth)
          var miss = if (t.nMissingBytes == 0) "miss_undefined" else stateVar("miss", depth)
          var haveTmpMissing = false
          if (skip) {
            if (t.nMissingBytes > 0) {
              mainCode.append(s"${ind}  ${miss} = (char*)malloc(${t.nMissingBytes});\n")
              haveTmpMissing = true
            }
          } else {
            mainCode.append(s"${ind}  ${ptr} = region->allocate(${wantStruct.alignment}, ${wantStruct.byteSize});\n")
            mainCode.append(s"${ind}  *(char**)${addr} = ${ptr};\n")
            if (wantStruct.fields.length != t.fields.length) {
              mainCode.append(s"${ind}  memset(${ptr}, 0xff, ${wantStruct.nMissingBytes});\n")
              mainCode.append(s"${ind}  ${miss} = (char*)malloc(${t.nMissingBytes});\n")
              haveTmpMissing = true
            } else {
              miss = ptr
            }
          }
          if (t.nMissingBytes > 0) {
            // Ack! We have to read this missing bytes, but shuffle bits needed for wantStruct
            val idx = stateVar("idx", depth)
            mainCode.append(s"${ind}  for (${idx} = 0; ${idx} < ${t.nMissingBytes};) {\n")
            val r2 = allocState(s"${name}.missing")
            mainCode.append(s"${ind}    auto ngot = decode_bytes(${miss}+${idx}, ${t.nMissingBytes}-${idx});\n")
            mainCode.append(s"${ind}    if (ngot <= 0) { s = ${r2}; goto pull; }\n")
            mainCode.append(s"${ind}    ${idx} += ngot;\n")
            mainCode.append(s"${ind}  }\n")
          }
          var fieldIdx = 0
          while (fieldIdx < t.fields.length) {
            val field = t.fields(fieldIdx)
            val fieldOffset = t.byteOffsets(fieldIdx)
            val fieldType = t.types(fieldIdx)
            var wantIdx = -1
            if (!skip) {
              var j = 0
              while (j < wantStruct.fields.length) {
                if (wantStruct.fields(j).name.equals(field.name)) wantIdx = j
                j += 1
              }
            }
            val fieldSkip = (wantIdx < 0)
            val wantType = if (fieldSkip) fieldType else wantStruct.types(wantIdx)
            if (!t.fieldRequired(fieldIdx)) {
              val m = t.missingIdx(fieldIdx)
              mainCode.append(s"${ind}  if (!is_missing(${miss}, ${m})) {\n")
              if (!fieldSkip) {
                if (haveTmpMissing) {
                  val mbit = wantStruct.missingIdx(wantIdx)
                  mainCode.append(s"${ind}    ${ptr}[${mbit>>3}] &= ~(1<<${mbit&0x7});\n")
                }
                val wantOffset = wantStruct.byteOffsets(wantIdx)
                mainCode.append(s"${ind}    ${stateVar("addr", depth+1)} = ${ptr} + ${wantOffset};\n")
              }
              scan(depth+1, numIndent+1, s"${name}.${field.name}", fieldType, wantType, fieldSkip)
              mainCode.append(s"${ind}  }\n")
            } else {
              if (!fieldSkip) {
                mainCode.append(s"${ind}  ${stateVar("addr", depth+1)} = ${ptr} + ${fieldOffset};\n")
              }
              scan(depth+1, numIndent, s"${name}.${field.name}", fieldType, wantType, fieldSkip)
            }
            fieldIdx += 1
          }
          if (haveTmpMissing) {
            mainCode.append(s"  free(${miss});\n")
          }
        
        case _ =>
          mainCode.append(s"${ind}  // unknown type ${typ}\n")
          assert(false)
                   
      }
    }
    
    scan(0, 1, "root", rowType, wantType, false)

    sb.append("#include \"hail/hail.h\"\n")
    sb.append("#include \"hail/PackDecoder.h\"\n")
    sb.append("#include \"hail/NativeStatus.h\"\n")
    sb.append("#include \"hail/Region.h\"\n")
    sb.append("#include <cstdint>\n")
    sb.append("#include <cstring>\n")
    sb.append("\n")
    sb.append("NAMESPACE_HAIL_MODULE_BEGIN\n")
    sb.append("\n")
    sb.append("class Decoder : public PackDecoderBase {\n")
    sb.append("public:\n")
    sb.append("  int s_ = 0;\n")
    sb.append(stateDefs)
    sb.append("\n")
    sb.append("  virtual ssize_t decode_until_done_or_need_push(Region* region, ssize_t push_size) {\n")
    sb.append("    size_ += push_size;\n")
    sb.append("    int s = s_;\n")
    sb.append(localDefs)
    sb.append("    switch (s) {\n")
    sb.append(entryCode)
    sb.append("    }\n")
    sb.append(mainCode)
    sb.append("    return 0;\n")
    sb.append("  pull:\n")
    sb.append("    s_ = s;\n")
    sb.append(flushCode)
    sb.append("    return prepare_for_push();\n")
    sb.append("  }\n")
    sb.append("};\n")
    sb.append("\n")
    sb.append("NativeObjPtr make_decoder() { return std::make_shared<Decoder>(); }\n")
    sb.append("\n")
    sb.append("ssize_t decode_until_done_or_need_push(NativeStatus*, long decoder, long region, long push_size) {\n")
    sb.append("  return ((Decoder*)decoder)->decode_until_done_or_need_push((Region*)region, push_size);\n")
    sb.append("}\n")
    sb.append("\n")
    sb.append("ssize_t decode_one_byte(NativeStatus*, long decoder, long push_size) {\n")
    sb.append("  return ((Decoder*)decoder)->decode_one_byte(push_size);\n")
    sb.append("}\n")
    sb.append("\n")
    sb.append("NAMESPACE_HAIL_MODULE_END\n")
  }
}

final class NativePackDecoder(in: InputBuffer, mod: NativeModule) extends Decoder {
  var st = new NativeStatus()
  val make_decoder = mod.findPtrFuncL0(st, "make_decoder")
  val decode_until_done_or_need_push = mod.findLongFuncL3(st, "decode_until_done_or_need_push")
  val decode_one_byte = mod.findLongFuncL2(st, "decode_one_byte")
  val decoder = new NativePtr(makeDecoder, st)
  val bufOffset = decoder.getFieldOffset(8, "buf_")
  val sizeOffset = decoder.getFieldOffset(8, "size_")
  val rvBaseOffset = decoder.getFieldOffset(8, "rv_base_")
  st.close()
  mod.close()

  def close(): Unit = {
    in.close()
    decoder.close()
    make_decoder.close()
    decode_until_done_or_need_push.close()
    decode_one_byte.close()
  }

  def pushData(size: Long): Long = {

  }

  def readByte(): Byte = {
    var rc = 0L
    var pushSize = 0L
    var done = false
    while (!done) {
      rc = decode_one_byte(st, decoder.get())
      if ((0x00 <= rc) && (rc <= 0xff)) {
        done = true;
      } else {
        pushSize = pushData(rc)
        if (pushSize <= 0) done = true
      }
    }
    val result: Byte = rc
    result
  }

  def readRegionValue(region: Region): Long = {
    var rc = 0L
    var pushSize = 0L
    var done = false
    while (!done) {
      rc = decode_until_done_or_need_push(st, decoder.get(), region.get(), pushSize)
      if (rc <= 0) {
        done = true
      } else {
        pushSize = pushData(rc)
        if (pushSize <= 0) done = true
      }
    }
    val result = Memory.loadLong(decoder.get()+rvBaseOffset)
  }

}

final class CompiledPackDecoder(in: InputBuffer, f: () => AsmFunction2[Region, InputBuffer, Long]) extends Decoder {
  def close() {
    in.close()
  }

  def readByte(): Byte = in.readByte()

  def readRegionValue(region: Region): Long = {
    f()(region, in)
  }
}

final class PackDecoder(rowType: Type, in: InputBuffer) extends Decoder {
  def close() {
    in.close()
  }

  def readByte(): Byte = in.readByte()

  def readBinary(region: Region, off: Long) {
    val length = in.readInt()
    val boff = region.allocate(4, 4 + length)
    region.storeAddress(off, boff)
    region.storeInt(boff, length)
    in.readBytes(region, boff + 4, length)
  }

  def readArray(t: TArray, region: Region): Long = {
    val length = in.readInt()

    val contentSize = t.contentsByteSize(length)
    val aoff = region.allocate(t.contentsAlignment, contentSize)

    region.storeInt(aoff, length)
    if (!t.elementType.required) {
      val nMissingBytes = (length + 7) >>> 3
      in.readBytes(region, aoff + 4, nMissingBytes)
    }

    val elemsOff = aoff + t.elementsOffset(length)
    val elemSize = t.elementByteSize

    if (t.elementType == TInt32Required) { // fast path
      var i = 0
      while (i < length) {
        val off = elemsOff + i * elemSize
        region.storeInt(off, in.readInt())
        i += 1
      }
    } else {
      var i = 0
      while (i < length) {
        if (t.isElementDefined(region, aoff, i)) {
          val off = elemsOff + i * elemSize
          t.elementType match {
            case t2: TBaseStruct => readBaseStruct(t2, region, off)
            case t2: TArray =>
              val aoff = readArray(t2, region)
              region.storeAddress(off, aoff)
            case _: TBoolean => region.storeByte(off, in.readBoolean().toByte)
            case _: TInt64 => region.storeLong(off, in.readLong())
            case _: TInt32 => region.storeInt(off, in.readInt())
            case _: TFloat32 => region.storeFloat(off, in.readFloat())
            case _: TFloat64 => region.storeDouble(off, in.readDouble())
            case _: TBinary => readBinary(region, off)
          }
        }
        i += 1
      }
    }

    aoff
  }

  def readBaseStruct(t: TBaseStruct, region: Region, offset: Long) {
    val nMissingBytes = t.nMissingBytes
    in.readBytes(region, offset, nMissingBytes)

    var i = 0
    while (i < t.size) {
      if (t.isFieldDefined(region, offset, i)) {
        val off = offset + t.byteOffsets(i)
        t.types(i) match {
          case t2: TBaseStruct => readBaseStruct(t2, region, off)
          case t2: TArray =>
            val aoff = readArray(t2, region)
            region.storeAddress(off, aoff)
          case _: TBoolean => region.storeByte(off, in.readBoolean().toByte)
          case _: TInt32 => region.storeInt(off, in.readInt())
          case _: TInt64 => region.storeLong(off, in.readLong())
          case _: TFloat32 => region.storeFloat(off, in.readFloat())
          case _: TFloat64 => region.storeDouble(off, in.readDouble())
          case _: TBinary => readBinary(region, off)
        }
      }
      i += 1
    }
  }

  def readRegionValue(region: Region): Long = {
    rowType.fundamentalType match {
      case t: TBaseStruct =>
        val start = region.allocate(t.alignment, t.byteSize)
        readBaseStruct(t, region, start)
        start

      case t: TArray =>
        readArray(t, region)
    }
  }
}

trait Encoder extends Closeable {
  def flush(): Unit

  def close(): Unit

  def writeRegionValue(region: Region, offset: Long): Unit

  def writeByte(b: Byte): Unit
}

final class PackEncoder(rowType: Type, out: OutputBuffer) extends Encoder {
  def flush() {
    out.flush()
  }

  def close() {
    out.close()
  }

  def writeByte(b: Byte): Unit = out.writeByte(b)

  def writeBinary(region: Region, offset: Long) {
    val boff = region.loadAddress(offset)
    val length = region.loadInt(boff)
    out.writeInt(length)
    out.writeBytes(region, boff + 4, length)
  }

  def writeArray(t: TArray, region: Region, aoff: Long) {
    val length = region.loadInt(aoff)

    out.writeInt(length)
    if (!t.elementType.required) {
      val nMissingBytes = (length + 7) >>> 3
      out.writeBytes(region, aoff + 4, nMissingBytes)
    }

    val elemsOff = aoff + t.elementsOffset(length)
    val elemSize = t.elementByteSize
    if (t.elementType.isInstanceOf[TInt32]) { // fast case
      var i = 0
      while (i < length) {
        if (t.isElementDefined(region, aoff, i)) {
          val off = elemsOff + i * elemSize
          out.writeInt(region.loadInt(off))
        }
        i += 1
      }
    } else {
      var i = 0
      while (i < length) {
        if (t.isElementDefined(region, aoff, i)) {
          val off = elemsOff + i * elemSize
          t.elementType match {
            case t2: TBaseStruct => writeBaseStruct(t2, region, off)
            case t2: TArray => writeArray(t2, region, region.loadAddress(off))
            case _: TBoolean => out.writeBoolean(region.loadByte(off) != 0)
            case _: TInt64 => out.writeLong(region.loadLong(off))
            case _: TFloat32 => out.writeFloat(region.loadFloat(off))
            case _: TFloat64 => out.writeDouble(region.loadDouble(off))
            case _: TBinary => writeBinary(region, off)
          }
        }

        i += 1
      }
    }
  }

  def writeBaseStruct(t: TBaseStruct, region: Region, offset: Long) {
    val nMissingBytes = t.nMissingBytes
    out.writeBytes(region, offset, nMissingBytes)

    var i = 0
    while (i < t.size) {
      if (t.isFieldDefined(region, offset, i)) {
        val off = offset + t.byteOffsets(i)
        t.types(i) match {
          case t2: TBaseStruct => writeBaseStruct(t2, region, off)
          case t2: TArray => writeArray(t2, region, region.loadAddress(off))
          case _: TBoolean => out.writeBoolean(region.loadByte(off) != 0)
          case _: TInt32 => out.writeInt(region.loadInt(off))
          case _: TInt64 => out.writeLong(region.loadLong(off))
          case _: TFloat32 => out.writeFloat(region.loadFloat(off))
          case _: TFloat64 => out.writeDouble(region.loadDouble(off))
          case _: TBinary => writeBinary(region, off)
        }
      }

      i += 1
    }
  }

  def writeRegionValue(region: Region, offset: Long) {
    (rowType.fundamentalType: @unchecked) match {
      case t: TBaseStruct =>
        writeBaseStruct(t, region, offset)
      case t: TArray =>
        writeArray(t, region, offset)
    }
  }
}

object RichContextRDDRegionValue {
  def writeRowsPartition(makeEnc: (OutputStream) => Encoder)(ctx: RVDContext, it: Iterator[RegionValue], os: OutputStream): Long = {
    val context = TaskContext.get
    val outputMetrics =
      if (context != null)
        context.taskMetrics().outputMetrics
      else
        null
    val trackedOS = new ByteTrackingOutputStream(os)
    val en = makeEnc(trackedOS)
    var rowCount = 0L

    it.foreach { rv =>
      en.writeByte(1)
      en.writeRegionValue(rv.region, rv.offset)
      en.flush()
      ctx.region.clear()
      rowCount += 1

      if (outputMetrics != null) {
        ExposedMetrics.setBytes(outputMetrics, trackedOS.bytesWritten)
        ExposedMetrics.setRecords(outputMetrics, rowCount)
      }
    }

    en.writeByte(0) // end
    en.flush()
    os.close()

    rowCount
  }
}

class RichContextRDDRegionValue(val crdd: ContextRDD[RVDContext, RegionValue]) extends AnyVal {
  def writeRows(path: String, t: TStruct, codecSpec: CodecSpec): (Array[String], Array[Long]) = {
    crdd.writePartitions(path, RichContextRDDRegionValue.writeRowsPartition(codecSpec.buildEncoder(t)))
  }

  def writeRowsSplit(path: String, t: MatrixType, codecSpec: CodecSpec, partitioner: OrderedRVDPartitioner): Array[Long] = {
    val sc = crdd.sparkContext
    val hConf = sc.hadoopConfiguration

    hConf.mkDir(path + "/rows/rows/parts")
    hConf.mkDir(path + "/entries/rows/parts")

    val sHConfBc = sc.broadcast(new SerializableHadoopConfiguration(hConf))

    val nPartitions = crdd.getNumPartitions
    val d = digitsNeeded(nPartitions)

    val fullRowType = t.rvRowType
    val rowsRVType = t.rowType
    val localEntriesIndex = t.entriesIdx
    val entriesRVType = t.entriesRVType

    val makeRowsEnc = codecSpec.buildEncoder(rowsRVType)

    val makeEntriesEnc = codecSpec.buildEncoder(t.entriesRVType)

    val partFilePartitionCounts = crdd.cmapPartitionsWithIndex { (i, ctx, it) =>
      val hConf = sHConfBc.value.value
      val context = TaskContext.get
      val f = partFile(d, i, context)
      val outputMetrics = context.taskMetrics().outputMetrics

      val rowsPartPath = path + "/rows/rows/parts/" + f
      hConf.writeFile(rowsPartPath) { rowsOS =>
        val trackedRowsOS = new ByteTrackingOutputStream(rowsOS)
        using(makeRowsEnc(trackedRowsOS)) { rowsEN =>

          val entriesPartPath = path + "/entries/rows/parts/" + f
          hConf.writeFile(entriesPartPath) { entriesOS =>
            val trackedEntriesOS = new ByteTrackingOutputStream(entriesOS)
            using(makeEntriesEnc(trackedEntriesOS)) { entriesEN =>

              var rowCount = 0L

              val rvb = new RegionValueBuilder()
              val fullRow = new UnsafeRow(fullRowType)

              it.foreach { rv =>
                fullRow.set(rv)
                val row = fullRow.deleteField(localEntriesIndex)

                val region = rv.region
                rvb.set(region)
                rvb.start(rowsRVType)
                rvb.addAnnotation(rowsRVType, row)

                rowsEN.writeByte(1)
                rowsEN.writeRegionValue(region, rvb.end())

                rvb.start(entriesRVType)
                rvb.startStruct()
                rvb.addField(fullRowType, rv, localEntriesIndex)
                rvb.endStruct()

                entriesEN.writeByte(1)
                entriesEN.writeRegionValue(region, rvb.end())

                ctx.region.clear()

                rowCount += 1

                ExposedMetrics.setBytes(outputMetrics, trackedRowsOS.bytesWritten + trackedEntriesOS.bytesWritten)
                ExposedMetrics.setRecords(outputMetrics, 2 * rowCount)
              }

              rowsEN.writeByte(0) // end
              entriesEN.writeByte(0)

              rowsEN.flush()
              entriesEN.flush()
              ExposedMetrics.setBytes(outputMetrics, trackedRowsOS.bytesWritten + trackedEntriesOS.bytesWritten)

              Iterator.single(f -> rowCount)
            }
          }
        }
      }
    }.collect()

    val (partFiles, partitionCounts) = partFilePartitionCounts.unzip

    val rowsSpec = OrderedRVDSpec(t.rowORVDType,
      codecSpec,
      partFiles,
      JSONAnnotationImpex.exportAnnotation(partitioner.rangeBounds, partitioner.rangeBoundsType))
    rowsSpec.write(hConf, path + "/rows/rows")

    val entriesSpec = UnpartitionedRVDSpec(entriesRVType, codecSpec, partFiles)
    entriesSpec.write(hConf, path + "/entries/rows")

    info(s"wrote ${ partitionCounts.sum } items in $nPartitions partitions to $path")

    partitionCounts
  }
}
