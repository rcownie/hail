package is.hail.annotations

import is.hail.SparkSuite
import is.hail.asm4s._
import is.hail.expr.types._
import is.hail.utils._
import org.testng.annotations.Test

class StagedRegionValueSuite extends SparkSuite {

  val showRVInfo = true

  @Test
  def testString() {
    val rt = TString()
    val input = "hello"
    val fb = FunctionBuilder.functionBuilder[Region, String, Long]
    val srvb = new StagedRegionValueBuilder(fb, rt)

    fb.emit(
      Code(
        srvb.start(),
        srvb.addString(fb.getArg[String](2)),
        srvb.end()
      )
    )

    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "string")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    rv2.setOffset(region2.appendString(input))

    if (showRVInfo) {
      printRegion(region2, "string")
      println(rv2.pretty(rt))
    }

    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(TString.loadString(rv.region, rv.offset) ==
      TString.loadString(rv2.region, rv2.offset))
  }

  @Test
  def testInt() {
    val rt = TInt32()
    val input = 3
    val fb = FunctionBuilder.functionBuilder[Region, Int, Long]
    val srvb = new StagedRegionValueBuilder(fb, rt)

    fb.emit(
      Code(
        srvb.start(),
        srvb.addInt(fb.getArg[Int](2)),
        srvb.end()
      )
    )

    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "int")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    rv2.setOffset(region2.appendInt(input))

    if (showRVInfo) {
      printRegion(region2, "int")
      println(rv2.pretty(rt))
    }

    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(rv.region.loadInt(rv.offset) == rv2.region.loadInt(rv2.offset))
  }

  @Test
  def testArray() {
    val rt = TArray(TInt32())
    val input = 3
    val fb = FunctionBuilder.functionBuilder[Region, Int, Long]
    val srvb = new StagedRegionValueBuilder(fb, TArray(TInt32()))

    fb.emit(
      Code(
        srvb.start(1),
        srvb.addInt(fb.getArg[Int](2)),
        srvb.advance(),
        srvb.end()
      )
    )

    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "array")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    rv2.setOffset(ScalaToRegionValue(region2, TArray(TInt32()), FastIndexedSeq(input)))

    if (showRVInfo) {
      printRegion(region2, "array")
      println(rv2.pretty(rt))
    }

    assert(rt.loadLength(rv.region, rv.offset) == 1)
    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(rv.region.loadInt(rt.loadElement(rv.region, rv.offset, 0)) ==
      rv2.region.loadInt(rt.loadElement(rv2.region, rv2.offset, 0)))
  }

  @Test
  def testStruct() {
    val rt = TStruct("a" -> TString(), "b" -> TInt32())
    val input = 3
    val fb = FunctionBuilder.functionBuilder[Region, Int, Long]
    val srvb = new StagedRegionValueBuilder(fb, rt)

    fb.emit(
      Code(
        srvb.start(),
        srvb.addString("hello"),
        srvb.advance(),
        srvb.addInt(fb.getArg[Int](2)),
        srvb.end()
      )
    )

    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "struct")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    rv2.setOffset(ScalaToRegionValue(region2, TStruct("a" -> TString(), "b" -> TInt32()), Annotation("hello", input)))

    if (showRVInfo) {
      printRegion(region2, "struct")
      println(rv2.pretty(rt))
    }

    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(TString.loadString(rv.region, rt.loadField(rv.region, rv.offset, 0)) ==
      TString.loadString(rv2.region, rt.loadField(rv2.region, rv2.offset, 0)))
    assert(rv.region.loadInt(rt.loadField(rv.region, rv.offset, 1)) ==
      rv2.region.loadInt(rt.loadField(rv2.region, rv2.offset, 1)))
  }

  @Test
  def testArrayOfStruct() {
    val rt = TArray(TStruct("a" -> TInt32(), "b" -> TString()))
    val input = "hello"
    val fb = FunctionBuilder.functionBuilder[Region, String, Long]
    val srvb = new StagedRegionValueBuilder(fb, rt)

    val struct = { ssb: StagedRegionValueBuilder =>
      Code(
        ssb.start(),
        ssb.addInt(srvb.arrayIdx + 1),
        ssb.advance(),
        ssb.addString(fb.getArg[String](2))
      )
    }

    fb.emit(
      Code(
        srvb.start(2),
        Code.whileLoop(srvb.arrayIdx < 2,
          Code(
            srvb.addBaseStruct(rt.elementType.asInstanceOf[TStruct], struct),
            srvb.advance()
          )
        ),
        srvb.end()
      )
    )


    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "array of struct")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    val rvb = new RegionValueBuilder(region2)
    rvb.start(rt)
    rvb.startArray(2)
    for (i <- 1 to 2) {
      rvb.startStruct()
      rvb.addInt(i)
      rvb.addString(input)
      rvb.endStruct()
    }
    rvb.endArray()
    rv2.setOffset(rvb.end())

    if (showRVInfo) {
      printRegion(region2, "array of struct")
      println(rv2.pretty(rt))
    }

    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(new UnsafeIndexedSeq(rt.physicalType, rv.region, rv.offset).sameElements(
      new UnsafeIndexedSeq(rt.physicalType, rv2.region, rv2.offset)))
  }

  @Test
  def testStructWithArray() {
    val rt = TStruct("a" -> TString(), "b" -> TArray(TInt32()))
    val input = "hello"
    val fb = FunctionBuilder.functionBuilder[Region, String, Long]
    val codeInput = fb.getArg[String](2)
    val srvb = new StagedRegionValueBuilder(fb, rt)

    val array = { sab: StagedRegionValueBuilder =>
      Code(
        sab.start(2),
        Code.whileLoop(sab.arrayIdx < 2,
          Code(
            sab.addInt(sab.arrayIdx + 1),
            sab.advance()
          )
        )
      )
    }

    fb.emit(
      Code(
        srvb.start(),
        srvb.addString(codeInput),
        srvb.advance(),
        srvb.addArray(TArray(TInt32()), array),
        srvb.end()
      )
    )

    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "struct with array")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    val rvb = new RegionValueBuilder(region2)

    rvb.start(rt)
    rvb.startStruct()
    rvb.addString(input)
    rvb.startArray(2)
    for (i <- 1 to 2) {
      rvb.addInt(i)
    }
    rvb.endArray()
    rvb.endStruct()

    rv2.setOffset(rvb.end())

    if (showRVInfo) {
      printRegion(region2, "struct with array")
      println(rv2.pretty(rt))
    }

    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(new UnsafeRow(rt.physicalType, rv.region, rv.offset) ==
      new UnsafeRow(rt.physicalType, rv2.region, rv2.offset))
  }

  @Test
  def testMissingArray() {
    val rt = TArray(TInt32())
    val input = 3
    val fb = FunctionBuilder.functionBuilder[Region, Int, Long]
    val codeInput = fb.getArg[Int](2)
    val srvb = new StagedRegionValueBuilder(fb, rt)

    fb.emit(
      Code(
        srvb.start(2),
        srvb.addInt(codeInput),
        srvb.advance(),
        srvb.setMissing(),
        srvb.advance(),
        srvb.end()
      )
    )

    val region = Region()
    val rv = RegionValue(region)
    rv.setOffset(fb.result()()(region, input))

    if (showRVInfo) {
      printRegion(region, "missing array")
      println(rv.pretty(rt))
    }

    val region2 = Region()
    val rv2 = RegionValue(region2)
    rv2.setOffset(ScalaToRegionValue(region2, TArray(TInt32()), FastIndexedSeq(input, null)))

    if (showRVInfo) {
      printRegion(region2, "missing array")
      println(rv2.pretty(rt))
    }

    assert(rv.pretty(rt) == rv2.pretty(rt))
    assert(new UnsafeIndexedSeq(rt.physicalType, rv.region, rv.offset).sameElements(
      new UnsafeIndexedSeq(rt.physicalType, rv2.region, rv2.offset)))
  }

  def printRegion(region: Region, string: String) {
    println(region.prettyBits())
  }

  @Test
  def testAddPrimitive() {
    val t = TStruct("a" -> TInt32(), "b" -> TBoolean(), "c" -> TFloat64())
    val fb = FunctionBuilder.functionBuilder[Region, Int, Boolean, Double, Long]
    val srvb = new StagedRegionValueBuilder(fb, t)

    fb.emit(
      Code(
        srvb.start(),
        srvb.addIRIntermediate(TInt32())(fb.getArg[Int](2)),
        srvb.advance(),
        srvb.addIRIntermediate(TBoolean())(fb.getArg[Boolean](3)),
        srvb.advance(),
        srvb.addIRIntermediate(TFloat64())(fb.getArg[Double](4)),
        srvb.advance(),
        srvb.end()
      )
    )

    val region = Region()
    val f = fb.result()()
    def run(i: Int, b: Boolean, d: Double): (Int, Boolean, Double) = {
      val off = f(region, i, b, d)
      (region.loadInt(t.loadField(region, off, 0)),
        region.loadBoolean(t.loadField(region, off, 1)),
        region.loadDouble(t.loadField(region, off, 2)))
    }

    assert(run(3, true, 42.0) == (3, true, 42.0))
    assert(run(42, false, -1.0) == (42, false, -1.0))
  }
}
