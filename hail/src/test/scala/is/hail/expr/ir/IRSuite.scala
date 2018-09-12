package is.hail.expr.ir

import is.hail.SparkSuite
import is.hail.expr.types._
import is.hail.TestUtils._
import is.hail.annotations.BroadcastRow
import is.hail.asm4s.Code
import is.hail.expr.{IRParserEnvironment, Parser}
import is.hail.expr.ir.IRSuite.TestFunctions
import is.hail.expr.ir.functions.{IRFunctionRegistry, RegistryFunctions, SeededIRFunction}
import is.hail.io.vcf.LoadVCF
import is.hail.table.{Ascending, Descending, SortField, Table}
import is.hail.utils._
import is.hail.variant.MatrixTable
import org.apache.spark.sql.Row
import org.testng.annotations.{DataProvider, Test}

object IRSuite { outer =>
  var globalCounter: Int = 0

  def incr(): Unit = {
    globalCounter += 1
  }

  object TestFunctions extends RegistryFunctions {

    def registerSeededWithMissingness(mname: String, aTypes: Array[Type], rType: Type)(impl: (EmitMethodBuilder, Long, Array[EmitTriplet]) => EmitTriplet) {
      IRFunctionRegistry.addIRFunction(new SeededIRFunction {
        val isDeterministic: Boolean = false

        override val name: String = mname

        override val argTypes: Seq[Type] = aTypes

        override val returnType: Type = rType

        def applySeeded(seed: Long, mb: EmitMethodBuilder, args: EmitTriplet*): EmitTriplet =
          impl(mb, seed, args.toArray)
      })
    }

    def registerSeededWithMissingness(mname: String, mt1: Type, rType: Type)(impl: (EmitMethodBuilder, Long, EmitTriplet) => EmitTriplet): Unit =
      registerSeededWithMissingness(mname, Array(mt1), rType) { case (mb, seed, Array(a1)) => impl(mb, seed, a1) }

    def registerAll() {
      registerSeededWithMissingness("incr_s", TBoolean(), TBoolean()) { (mb, _, l) =>
        EmitTriplet(Code(Code.invokeScalaObject[Unit](outer.getClass, "incr"), l.setup),
          l.m,
          l.v)
      }

      registerSeededWithMissingness("incr_m", TBoolean(), TBoolean()) { (mb, _, l) =>
        EmitTriplet(l.setup,
          Code(Code.invokeScalaObject[Unit](outer.getClass, "incr"), l.m),
          l.v)
      }

      registerSeededWithMissingness("incr_v", TBoolean(), TBoolean()) { (mb, _, l) =>
        EmitTriplet(l.setup,
          l.m,
          Code(Code.invokeScalaObject[Unit](outer.getClass, "incr"), l.v))
      }
    }
  }
}

class IRSuite extends SparkSuite {
  @Test def testI32() {
    assertEvalsTo(I32(5), 5)
  }

  @Test def testI64() {
    assertEvalsTo(I64(5), 5L)
  }

  @Test def testF32() {
    assertEvalsTo(F32(3.14f), 3.14f)
  }

  @Test def testF64() {
    assertEvalsTo(F64(3.14), 3.14)
  }

  @Test def testStr() {
    assertEvalsTo(Str("Hail"), "Hail")
  }

  @Test def testTrue() {
    assertEvalsTo(True(), true)
  }

  @Test def testFalse() {
    assertEvalsTo(False(), false)
  }

  // FIXME Void() doesn't work becuase we can't handle a void type in a tuple

  @Test def testCast() {
    assertEvalsTo(Cast(I32(5), TInt32()), 5)
    assertEvalsTo(Cast(I32(5), TInt64()), 5L)
    assertEvalsTo(Cast(I32(5), TFloat32()), 5.0f)
    assertEvalsTo(Cast(I32(5), TFloat64()), 5.0)

    assertEvalsTo(Cast(I64(5), TInt32()), 5)
    assertEvalsTo(Cast(I64(0xf29fb5c9af12107dL), TInt32()), 0xaf12107d) // truncate
    assertEvalsTo(Cast(I64(5), TInt64()), 5L)
    assertEvalsTo(Cast(I64(5), TFloat32()), 5.0f)
    assertEvalsTo(Cast(I64(5), TFloat64()), 5.0)

    assertEvalsTo(Cast(F32(3.14f), TInt32()), 3)
    assertEvalsTo(Cast(F32(3.99f), TInt32()), 3) // truncate
    assertEvalsTo(Cast(F32(3.14f), TInt64()), 3L)
    assertEvalsTo(Cast(F32(3.14f), TFloat32()), 3.14f)
    assertEvalsTo(Cast(F32(3.14f), TFloat64()), 3.14)

    assertEvalsTo(Cast(F64(3.14), TInt32()), 3)
    assertEvalsTo(Cast(F64(3.99), TInt32()), 3) // truncate
    assertEvalsTo(Cast(F64(3.14), TInt64()), 3L)
    assertEvalsTo(Cast(F64(3.14), TFloat32()), 3.14f)
    assertEvalsTo(Cast(F64(3.14), TFloat64()), 3.14)
  }

  @Test def testNA() {
    assertEvalsTo(NA(TInt32()), null)
  }

  @Test def testIsNA() {
    assertEvalsTo(IsNA(NA(TInt32())), true)
    assertEvalsTo(IsNA(I32(5)), false)
  }

  @Test def testIf() {
    assertEvalsTo(If(True(), I32(5), I32(7)), 5)
    assertEvalsTo(If(False(), I32(5), I32(7)), 7)
    assertEvalsTo(If(NA(TBoolean()), I32(5), I32(7)), null)
    assertEvalsTo(If(True(), NA(TInt32()), I32(7)), null)
  }

  @Test def testIfWithDifferentRequiredness() {
    val t = TStruct(true, "foo" -> TStruct("bar" -> TArray(TInt32Required, required = true)))
    val value = Row(Row(FastIndexedSeq(1, 2, 3)))
    assertEvalsTo(
      If(
        In(0, TBoolean()),
        In(1, t),
        MakeStruct(Seq("foo" -> MakeStruct(Seq("bar" -> ArrayRange(I32(0), I32(1), I32(1))))))),
      FastIndexedSeq((true, TBoolean()), (value, t)),
      value
    )
  }

  @Test def testLet() {
    assertEvalsTo(Let("v", I32(5), Ref("v", TInt32())), 5)
    assertEvalsTo(Let("v", NA(TInt32()), Ref("v", TInt32())), null)
    assertEvalsTo(Let("v", I32(5), NA(TInt32())), null)
  }

  @Test def testMakeArray() {
    assertEvalsTo(MakeArray(FastSeq(I32(5), NA(TInt32()), I32(-3)), TArray(TInt32())), FastIndexedSeq(5, null, -3))
    assertEvalsTo(MakeArray(FastSeq(), TArray(TInt32())), FastIndexedSeq())
  }

  @Test def testMakeStruct() {
    assertEvalsTo(MakeStruct(FastSeq()), Row())
    assertEvalsTo(MakeStruct(FastSeq("a" -> NA(TInt32()), "b" -> 4, "c" -> 0.5)), Row(null, 4, 0.5))
    //making sure wide structs get emitted without failure
    assertEvalsTo(GetField(MakeStruct((0 until 20000).map(i => s"foo$i" -> I32(1))), "foo1"), 1)
  }

  @Test def testMakeTuple() {
    assertEvalsTo(MakeTuple(FastSeq()), Row())
    assertEvalsTo(MakeTuple(FastSeq(NA(TInt32()), 4, 0.5)), Row(null, 4, 0.5))
    //making sure wide structs get emitted without failure
    assertEvalsTo(GetTupleElement(MakeTuple((0 until 20000).map(I32)), 1), 1)
  }

  @Test def testArrayRef() {
    assertEvalsTo(ArrayRef(MakeArray(FastIndexedSeq(I32(5), NA(TInt32())), TArray(TInt32())), I32(0)), 5)
    assertEvalsTo(ArrayRef(MakeArray(FastIndexedSeq(I32(5), NA(TInt32())), TArray(TInt32())), I32(1)), null)
    assertEvalsTo(ArrayRef(MakeArray(FastIndexedSeq(I32(5), NA(TInt32())), TArray(TInt32())), NA(TInt32())), null)

    assertFatal(ArrayRef(MakeArray(FastIndexedSeq(I32(5)), TArray(TInt32())), I32(2)), "array index out of bounds")
  }

  @Test def testArrayLen() {
    assertEvalsTo(ArrayLen(NA(TArray(TInt32()))), null)
    assertEvalsTo(ArrayLen(MakeArray(FastIndexedSeq(), TArray(TInt32()))), 0)
    assertEvalsTo(ArrayLen(MakeArray(FastIndexedSeq(I32(5), NA(TInt32())), TArray(TInt32()))), 2)
  }

  @Test def testArraySort() {
    assertEvalsTo(ArraySort(NA(TArray(TInt32())), True()), null)

    val a = MakeArray(FastIndexedSeq(I32(-7), I32(2), NA(TInt32()), I32(2)), TArray(TInt32()))
    assertEvalsTo(ArraySort(a, True()),
      FastIndexedSeq(-7, 2, 2, null))
    assertEvalsTo(ArraySort(a, False()),
      FastIndexedSeq(2, 2, -7, null))
  }

  @Test def testToSet() {
    assertEvalsTo(ToSet(NA(TArray(TInt32()))), null)

    val a = MakeArray(FastIndexedSeq(I32(-7), I32(2), NA(TInt32()), I32(2)), TArray(TInt32()))
    assertEvalsTo(ToSet(a), Set(-7, 2, null))
  }

  @Test def testToArrayFromSet() {
    val t = TSet(TInt32())
    assertEvalsTo(ToArray(NA(t)), null)
    assertEvalsTo(ToArray(In(0, t)),
      FastIndexedSeq((Set(-7, 2, null), t)),
      FastIndexedSeq(-7, 2, null))
  }

  @Test def testToArrayFromDict() {
    val t = TDict(TInt32(), TString())
    assertEvalsTo(ToArray(NA(t)), null)

    val d = Map(1 -> "a", 2 -> null, (null, "c"))
    assertEvalsTo(ToArray(In(0, t)),
      // wtf you can't do null -> ...
      FastIndexedSeq((d, t)),
      FastIndexedSeq(Row(1, "a"), Row(2, null), Row(null, "c")))
  }

  @Test def testToArrayFromArray() {
    val t = TArray(TInt32())
    assertEvalsTo(ToArray(NA(t)), null)
    assertEvalsTo(ToArray(In(0, t)),
      FastIndexedSeq((FastIndexedSeq(-7, 2, null, 2), t)),
      FastIndexedSeq(-7, 2, null, 2))
  }

  @Test def testSetContains() {
    val t = TSet(TInt32())
    assertEvalsTo(invoke("contains", NA(t), I32(2)), null)

    assertEvalsTo(invoke("contains", In(0, t), NA(TInt32())),
      FastIndexedSeq((Set(-7, 2, null), t)),
      true)
    assertEvalsTo(invoke("contains", In(0, t), I32(2)),
      FastIndexedSeq((Set(-7, 2, null), t)),
      true)
    assertEvalsTo(invoke("contains", In(0, t), I32(0)),
      FastIndexedSeq((Set(-7, 2, null), t)),
      false)
    assertEvalsTo(invoke("contains", In(0, t), I32(7)),
      FastIndexedSeq((Set(-7, 2), t)),
      false)
  }

  @Test def testDictContains() {
    val t = TDict(TInt32(), TString())
    assertEvalsTo(invoke("contains", NA(t), I32(2)), null)

    val d = Map(1 -> "a", 2 -> null, (null, "c"))
    assertEvalsTo(invoke("contains", In(0, t), NA(TInt32())),
      FastIndexedSeq((d, t)),
      true)
    assertEvalsTo(invoke("contains", In(0, t), I32(2)),
      FastIndexedSeq((d, t)),
      true)
    assertEvalsTo(invoke("contains", In(0, t), I32(0)),
      FastIndexedSeq((d, t)),
      false)
    assertEvalsTo(invoke("contains", In(0, t), I32(3)),
      FastIndexedSeq((Map(1 -> "a", 2 -> null), t)),
      false)
  }

  @Test def testArrayMap() {
    val naa = NA(TArray(TInt32()))
    val a = MakeArray(Seq(I32(3), NA(TInt32()), I32(7)), TArray(TInt32()))

    assertEvalsTo(ArrayMap(naa, "a", I32(5)), null)

    assertEvalsTo(ArrayMap(a, "a", ApplyBinaryPrimOp(Add(), Ref("a", TInt32()), I32(1))), FastIndexedSeq(4, null, 8))

    assertEvalsTo(Let("a", I32(5),
      ArrayMap(a, "a", Ref("a", TInt32()))),
      FastIndexedSeq(3, null, 7))
  }

  @Test def testArrayFilter() {
    val naa = NA(TArray(TInt32()))
    val a = MakeArray(Seq(I32(3), NA(TInt32()), I32(7)), TArray(TInt32()))

    assertEvalsTo(ArrayFilter(naa, "x", True()), null)

    assertEvalsTo(ArrayFilter(a, "x", NA(TBoolean())), FastIndexedSeq())
    assertEvalsTo(ArrayFilter(a, "x", False()), FastIndexedSeq())
    assertEvalsTo(ArrayFilter(a, "x", True()), FastIndexedSeq(3, null, 7))

    assertEvalsTo(ArrayFilter(a, "x",
      IsNA(Ref("x", TInt32()))), FastIndexedSeq(null))
    assertEvalsTo(ArrayFilter(a, "x",
      ApplyUnaryPrimOp(Bang(), IsNA(Ref("x", TInt32())))), FastIndexedSeq(3, 7))

    assertEvalsTo(ArrayFilter(a, "x",
      ApplyComparisonOp(LT(TInt32()), Ref("x", TInt32()), I32(6))), FastIndexedSeq(3))
  }

  @Test def testArrayFlatMap() {
    val ta = TArray(TInt32())
    val taa = TArray(ta)
    val naa = NA(taa)
    val naaa = MakeArray(FastIndexedSeq(NA(ta), NA(ta)), taa)
    val a = MakeArray(FastIndexedSeq(
      MakeArray(FastIndexedSeq(I32(7), NA(TInt32())), ta),
      NA(ta),
      MakeArray(FastIndexedSeq(I32(2)), ta)),
      taa)

    assertEvalsTo(ArrayFlatMap(naa, "a", MakeArray(FastIndexedSeq(I32(5)), ta)), null)

    assertEvalsTo(ArrayFlatMap(naaa, "a", Ref("a", ta)), FastIndexedSeq())

    assertEvalsTo(ArrayFlatMap(a, "a", Ref("a", ta)), FastIndexedSeq(7, null, 2))

    assertEvalsTo(ArrayFlatMap(ArrayRange(I32(0), I32(3), I32(1)), "i", ArrayRef(a, Ref("i", TInt32()))), FastIndexedSeq(7, null, 2))

    assertEvalsTo(Let("a", I32(5),
      ArrayFlatMap(a, "a", Ref("a", ta))),
      FastIndexedSeq(7, null, 2))
  }

  @Test def testArrayFold() {
    def fold(array: IR, zero: IR, f: (IR, IR) => IR): IR =
      ArrayFold(array, zero, "_accum", "_elt", f(Ref("_accum", coerce[TArray](array.typ).elementType), Ref("_elt", zero.typ)))

    assertEvalsTo(fold(ArrayRange(1, 2, 1), NA(TBoolean()), (accum, elt) => IsNA(accum)), true)
    assertEvalsTo(fold(TestUtils.IRArray(1, 2, 3), 0, (accum, elt) => accum + elt), 6)
    assertEvalsTo(fold(TestUtils.IRArray(1, 2, 3), NA(TInt32()), (accum, elt) => accum + elt), null)
    assertEvalsTo(fold(TestUtils.IRArray(1, null, 3), NA(TInt32()), (accum, elt) => accum + elt), null)
    assertEvalsTo(fold(TestUtils.IRArray(1, null, 3), 0, (accum, elt) => accum + elt), null)
  }

  @Test def testArrayScan() {
    def scan(array: IR, zero: IR, f: (IR, IR) => IR): IR =
      ArrayScan(array, zero, "_accum", "_elt", f(Ref("_accum", coerce[TArray](array.typ).elementType), Ref("_elt", zero.typ)))

    assertEvalsTo(scan(ArrayRange(1, 4, 1), NA(TBoolean()), (accum, elt) => IsNA(accum)), FastIndexedSeq(null, true, false, false))
    assertEvalsTo(scan(TestUtils.IRArray(1, 2, 3), 0, (accum, elt) => accum + elt), FastIndexedSeq(0, 1, 3, 6))
    assertEvalsTo(scan(TestUtils.IRArray(1, 2, 3), NA(TInt32()), (accum, elt) => accum + elt), FastIndexedSeq(null, null, null, null))
    assertEvalsTo(scan(TestUtils.IRArray(1, null, 3), NA(TInt32()), (accum, elt) => accum + elt), FastIndexedSeq(null, null, null, null))
    assertEvalsTo(scan(TestUtils.IRArray(1, null, 3), 0, (accum, elt) => accum + elt), FastIndexedSeq(0, 1, null, null))
  }

  @Test def testDie() {
    assertFatal(Die("mumblefoo", TFloat64()), "mble")
  }

  @Test def testArrayRange() {
    assertEvalsTo(ArrayRange(I32(0), I32(5), NA(TInt32())), null)
    assertEvalsTo(ArrayRange(I32(0), NA(TInt32()), I32(1)), null)
    assertEvalsTo(ArrayRange(NA(TInt32()), I32(5), I32(1)), null)

    assertFatal(ArrayRange(I32(0), I32(5), I32(0)), "step size")
  }

  @Test def testInsertFields() {
    val s = TStruct("a" -> TInt64(), "b" -> TString())

    assertEvalsTo(
      InsertFields(
        NA(s),
        Seq()),
      null)

    assertEvalsTo(
      InsertFields(
        NA(s),
        Seq("a" -> I64(5))),
      Row(5L, null))

    assertEvalsTo(
      InsertFields(
        NA(s),
        Seq("c" -> F64(3.2))),
      Row(null, null, 3.2))

    assertEvalsTo(
      InsertFields(
        NA(s),
        Seq("c" -> NA(TFloat64()))),
      Row(null, null, null))

    assertEvalsTo(
      InsertFields(
        MakeStruct(Seq("a" -> NA(TInt64()), "b" -> Str("abc"))),
        Seq()),
      Row(null, "abc"))

    assertEvalsTo(
      InsertFields(
        MakeStruct(Seq("a" -> NA(TInt64()), "b" -> Str("abc"))),
        Seq("a" -> I64(5))),
      Row(5L, "abc"))

    assertEvalsTo(
      InsertFields(
        MakeStruct(Seq("a" -> NA(TInt64()), "b" -> Str("abc"))),
        Seq("c" -> F64(3.2))),
      Row(null, "abc", 3.2))
  }

  @Test def testSelectFields() {
    assertEvalsTo(
      SelectFields(
        NA(TStruct("foo" -> TInt32(), "bar" -> TFloat64())),
        FastSeq("foo")),
      null)

    assertEvalsTo(
      SelectFields(
        MakeStruct(FastSeq("foo" -> 6, "bar" -> 0.0)),
        FastSeq("foo")),
      Row(6))

    assertEvalsTo(
      SelectFields(
        MakeStruct(FastSeq("a" -> 6, "b" -> 0.0, "c" -> 3L)),
        FastSeq("b", "a")),
      Row(0.0, 6))

  }

  @Test def testTableCount() {
    assertEvalsTo(TableCount(TableRange(0, 4)), 0L)
    assertEvalsTo(TableCount(TableRange(7, 4)), 7L)
  }

  @Test def testGroupByKey() {
    def tuple(k: String, v: Int): IR = MakeTuple(Seq(Str(k), I32(v)))

    def groupby(tuples: IR*): IR = GroupByKey(MakeArray(tuples, TArray(TTuple(TString(), TInt32()))))

    val collection1 = groupby(tuple("foo", 0), tuple("bar", 4), tuple("foo", -1), tuple("bar", 0), tuple("foo", 10), tuple("", 0))

    assertEvalsTo(collection1, Map("" -> FastIndexedSeq(0), "bar" -> FastIndexedSeq(4, 0), "foo" -> FastIndexedSeq(0, -1, 10)))
  }

  @DataProvider(name = "compareDifferentTypes")
  def compareDifferentTypesData(): Array[Array[Any]] = Array(
    Array(FastIndexedSeq(0.0, 0.0), TArray(+TFloat64()), TArray(TFloat64())),
    Array(Set(0, 1), TSet(+TInt32()), TSet(TInt32())),
    Array(Map(0L -> 5, 3L -> 20), TDict(+TInt64(), TInt32()), TDict(TInt64(), +TInt32())),
    Array(Interval(1, 2, includesStart = false, includesEnd = true), TInterval(+TInt32()), TInterval(TInt32())),
    Array(Row("foo", 0.0), TStruct("a" -> +TString(), "b" -> +TFloat64()), TStruct("a" -> TString(), "b" -> TFloat64())),
    Array(Row("foo", 0.0), TTuple(TString(), +TFloat64()), TTuple(+TString(), +TFloat64())),
    Array(Row(FastIndexedSeq("foo"), 0.0), TTuple(+TArray(TString()), +TFloat64()), TTuple(TArray(+TString()), +TFloat64()))
  )

  @Test(dataProvider = "compareDifferentTypes")
  def testComparisonOpDifferentTypes(a: Any, t1: Type, t2: Type) {
    assertEvalsTo(ApplyComparisonOp(EQ(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), true)
    assertEvalsTo(ApplyComparisonOp(LT(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), false)
    assertEvalsTo(ApplyComparisonOp(GT(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), false)
    assertEvalsTo(ApplyComparisonOp(LTEQ(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), true)
    assertEvalsTo(ApplyComparisonOp(GTEQ(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), true)
    assertEvalsTo(ApplyComparisonOp(NEQ(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), false)
    assertEvalsTo(ApplyComparisonOp(EQWithNA(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), true)
    assertEvalsTo(ApplyComparisonOp(NEQWithNA(t1, t2), In(0, t1), In(1, t2)), IndexedSeq(a -> t1, a -> t2), false)
  }

  @DataProvider(name = "valueIRs")
  def valueIRs(): Array[Array[IR]] = {
    val b = True()
    val c = Ref("c", TBoolean())
    val i = I32(5)
    val j = I32(7)
    val str = Str("Hail")
    val a = Ref("a", TArray(TInt32()))
    val aa = Ref("aa", TArray(TArray(TInt32())))
    val da = Ref("da", TArray(TTuple(TInt32(), TString())))
    val v = Ref("v", TInt32())
    val s = Ref("s", TStruct("x" -> TInt32(), "y" -> TInt64(), "z" -> TFloat64()))
    val t = Ref("t", TTuple(TInt32(), TInt64(), TFloat64()))

    val call = Ref("call", TCall())

    val collectSig = AggSignature(Collect(), Seq(), None, Seq(TInt32()))

    val callStatsSig = AggSignature(CallStats(), Seq(), Some(Seq(TInt32())), Seq(TCall()))

    val histSig = AggSignature(Histogram(), Seq(TFloat64(), TFloat64(), TInt32()), None, Seq(TFloat64()))

    val takeBySig = AggSignature(TakeBy(), Seq(TInt32()), None, Seq(TFloat64(), TInt32()))

    val keyedKeyedCollectSig = AggSignature(Keyed(Keyed(Collect())), Seq(), None, Seq(TInt32(), TString(), TBoolean()))

    val irs = Array(
      i, I64(5), F32(3.14f), F64(3.14), str, True(), False(), Void(),
      Cast(i, TFloat64()),
      NA(TInt32()), IsNA(i),
      If(b, i, j),
      Let("v", i, v),
      Ref("x", TInt32()),
      ApplyBinaryPrimOp(Add(), i, j),
      ApplyUnaryPrimOp(Negate(), i),
      ApplyComparisonOp(EQ(TInt32()), i, j),
      MakeArray(FastSeq(i, NA(TInt32()), I32(-3)), TArray(TInt32())),
      ArrayRef(a, i),
      ArrayLen(a),
      ArrayRange(I32(0), I32(5), I32(1)),
      ArraySort(a, b),
      ToSet(a),
      ToDict(da),
      ToArray(a),
      LowerBoundOnOrderedCollection(a, i, onKey = true),
      GroupByKey(da),
      ArrayMap(a, "v", v),
      ArrayFilter(a, "v", b),
      ArrayFlatMap(aa, "v", v),
      ArrayFold(a, I32(0), "x", "v", v),
      ArrayScan(a, I32(0), "x", "v", v),
      ArrayFor(a, "v", Void()),
      ApplyAggOp(I32(0), FastIndexedSeq.empty, None, collectSig),
      ApplyAggOp(F64(-2.11), FastIndexedSeq(F64(-5.0), F64(5.0), I32(100)), None, histSig),
      ApplyAggOp(call, FastIndexedSeq.empty, Some(FastIndexedSeq(I32(2))), callStatsSig),
      ApplyAggOp(F64(-2.11), FastIndexedSeq(I32(10)), None, takeBySig),
      ApplyAggOp(Str("foo"), FastIndexedSeq.empty, None, keyedKeyedCollectSig),
      InitOp(I32(0), FastIndexedSeq(I32(2)), callStatsSig),
      SeqOp(I32(0), FastIndexedSeq(i), collectSig),
      SeqOp(I32(0), FastIndexedSeq(F64(-2.11), I32(17)), takeBySig),
      SeqOp(I32(0), FastIndexedSeq(I32(5), Str("hello"), True(), Str("foo")), keyedKeyedCollectSig),
      Begin(IndexedSeq(Void())),
      MakeStruct(Seq("x" -> i)),
      SelectFields(s, Seq("x", "z")),
      InsertFields(s, Seq("x" -> i)),
      GetField(s, "x"),
      MakeTuple(Seq(i, b)),
      GetTupleElement(t, 1),
      StringSlice(str, I32(1), I32(2)),
      StringLength(str),
      In(2, TFloat64()),
      Die("mumblefoo", TFloat64()),
      invoke("&&", b, c), // ApplySpecial
      invoke("toFloat64", i), // Apply
      invoke("isDefined", s), // ApplyIR
      Uniroot("x", F64(3.14), F64(-5.0), F64(5.0)),
      Literal(TStruct("x" -> TInt32()), Row(1), "__broadcast1")
    )
    irs.map(x => Array(x))
  }

  @DataProvider(name = "tableIRs")
  def tableIRs(): Array[Array[TableIR]] = {
    try {
      val ht = Table.read(hc, "src/test/resources/backward_compatability/1.0.0/table/0.ht")
      val mt = MatrixTable.read(hc, "src/test/resources/backward_compatability/1.0.0/matrix_table/0.hmt")

      val read = ht.tir.asInstanceOf[TableRead]
      val mtRead = mt.ast.asInstanceOf[MatrixRead]
      val b = True()

      val xs: Array[TableIR] = Array(
        TableUnkey(read),
        TableDistinct(read),
        TableKeyBy(read, Array("m", "d")),
        TableFilter(read, b),
        read,
        MatrixColsTable(mtRead),
        TableAggregateByKey(read,
          MakeStruct(FastIndexedSeq(
            "a" -> I32(5)))),
        TableKeyByAndAggregate(read,
          NA(TStruct()), NA(TStruct()), Some(1), 2),
        TableJoin(read,
          TableRange(100, 10), "inner", 1),
        TableLeftJoinRightDistinct(read, TableRange(100, 10), "root"),
        MatrixEntriesTable(mtRead),
        MatrixRowsTable(mtRead),
        TableRepartition(read, 10, false),
        TableHead(read, 10),
        TableParallelize(
          MakeArray(FastSeq(
            MakeStruct(FastSeq("a" -> NA(TInt32()))),
            MakeStruct(FastSeq("a" -> I32(1)))
          ), TArray(TStruct("a" -> TInt32()))), None),
        TableMapRows(TableUnkey(read),
          MakeStruct(FastIndexedSeq(
            "a" -> GetField(Ref("row", read.typ.rowType), "f32"),
            "b" -> F64(-2.11))),
          None),
        TableMapGlobals(read,
          MakeStruct(FastIndexedSeq(
            "foo" -> NA(TArray(TInt32()))))),
        TableRange(100, 10),
        TableUnion(
          FastIndexedSeq(TableRange(100, 10), TableRange(50, 10))),
        TableExplode(read, "mset"),
        TableUnkey(read),
        TableOrderBy(TableUnkey(read), FastIndexedSeq(SortField("m", Ascending), SortField("m", Descending))),
        LocalizeEntries(mtRead, " # entries")
      )
      xs.map(x => Array(x))
    } catch {
      case t: Throwable =>
        println(t)
        println(t.printStackTrace())
        throw t
    }
  }

  @DataProvider(name = "matrixIRs")
  def matrixIRs(): Array[Array[MatrixIR]] = {
    try {
      val tableRead = Table.read(hc, "src/test/resources/backward_compatability/1.0.0/table/0.ht")
        .tir.asInstanceOf[TableRead]
      val read = MatrixTable.read(hc, "src/test/resources/backward_compatability/1.0.0/matrix_table/0.hmt")
        .ast.asInstanceOf[MatrixRead]
      val range = MatrixTable.range(hc, 3, 7, None)
        .ast.asInstanceOf[MatrixRead]
      val vcf = hc.importVCF("src/test/resources/sample.vcf")
        .ast.asInstanceOf[MatrixRead]
      val bgen = hc.importBgens(FastIndexedSeq("src/test/resources/example.8bits.bgen"))
        .ast.asInstanceOf[MatrixRead]
      val range1 = MatrixTable.range(hc, 20, 2, Some(3))
        .ast.asInstanceOf[MatrixRead]
      val range2 = MatrixTable.range(hc, 20, 2, Some(4))
        .ast.asInstanceOf[MatrixRead]

      val b = True()

      val newCol = MakeStruct(FastIndexedSeq(
        "col_idx" -> GetField(Ref("sa", read.typ.colType), "col_idx"),
        "new_f32" -> ApplyBinaryPrimOp(Add(),
          GetField(Ref("sa", read.typ.colType), "col_f32"),
          F32(-5.2f))))
      val newRow = MakeStruct(FastIndexedSeq(
        "row_idx" -> GetField(Ref("va", read.typ.rowType), "row_idx"),
        "new_f32" -> ApplyBinaryPrimOp(Add(),
          GetField(Ref("va", read.typ.rowType), "row_f32"),
          F32(-5.2f))))

      val xs = Array[MatrixIR](
        read,
        MatrixFilterRows(read, b),
        MatrixFilterCols(read, b),
        MatrixFilterEntries(read, b),
        MatrixChooseCols(read, Array(0, 0, 0)),
        MatrixMapCols(read, newCol, None),
        MatrixMapRows(read, newRow, None),
        MatrixMapEntries(read, MakeStruct(FastIndexedSeq(
          "global_f32" -> ApplyBinaryPrimOp(Add(),
            GetField(Ref("global", read.typ.globalType), "global_f32"),
            F32(-5.2f))))),
        MatrixCollectColsByKey(read),
        MatrixAggregateColsByKey(read, newCol),
        MatrixAggregateRowsByKey(read, newRow),
        range,
        vcf,
        bgen,
        TableToMatrixTable(
          tableRead,
          Array("astruct", "aset"),
          Array("d", "ml"),
          Array("mc"),
          Array("t"),
          None),
        MatrixExplodeRows(read, FastIndexedSeq("row_mset")),
        MatrixUnionRows(FastIndexedSeq(range1, range2)),
        MatrixExplodeCols(read, FastIndexedSeq("col_mset")),
        UnlocalizeEntries(
          LocalizeEntries(read, "all of the entries"),
          MatrixColsTable(read),
          "all of the entries"),
        MatrixAnnotateColsTable(read, tableRead, "uid_123"),
        MatrixAnnotateRowsTable(read, tableRead, "uid_123", Some(FastIndexedSeq(I32(1))))
      )

      xs.map(x => Array(x))
    } catch {
      case t: Throwable =>
        println(t)
        println(t.printStackTrace())
        throw t
    }
  }

  @Test(dataProvider = "valueIRs")
  def testValueIRParser(x: IR) {
    val s = Pretty(x)
    val x2 = Parser.parse_value_ir(s, IRParserEnvironment())
    assert(x2 == x)
  }

  @Test(dataProvider = "tableIRs")
  def testTableIRParser(x: TableIR) {
    val s = Pretty(x)
    val x2 = Parser.parse_table_ir(s, IRParserEnvironment())
    assert(x2 == x)
  }

  @Test(dataProvider = "matrixIRs")
  def testMatrixIRParser(x: MatrixIR) {
    val s = Pretty(x)
    val x2 = Parser.parse_matrix_ir(s, IRParserEnvironment())
    assert(x2 == x)
  }

  @Test def testCachedIR() {
    val cached = Literal(TSet(TInt32()), Set(1), "__unique_id")
    val s = s"(JavaIR __uid1)"
    val x2 = Parser.parse_value_ir(s, IRParserEnvironment(ref_map = Map.empty, ir_map = Map("__uid1" -> cached)))
    assert(x2 eq cached)
  }

  @Test def testCachedTableIR() {
    val cached = TableRange(1, 1)
    val s = s"(JavaTable __uid1)"
    val x2 = Parser.parse_table_ir(s, IRParserEnvironment(ref_map = Map.empty, ir_map = Map("__uid1" -> cached)))
    assert(x2 eq cached)
  }

  @Test def testCachedMatrixIR() {
    val cached = MatrixTable.range(hc, 3, 7, None).ast
    val s = s"(JavaMatrix __uid1)"
    val x2 = Parser.parse_matrix_ir(s, IRParserEnvironment(ref_map = Map.empty, ir_map = Map("__uid1" -> cached)))
    assert(x2 eq cached)

  }

  @Test def testEvaluations() {
    TestFunctions.registerAll()
    def test(x: IR, i: java.lang.Boolean, expectedEvaluations: Int) {
      val env = Env.empty[(Any, Type)]
      val args = IndexedSeq((i, TBoolean()))

      IRSuite.globalCounter = 0
      Interpret[Any](x, env, args, None, optimize = false)
      assert(IRSuite.globalCounter == expectedEvaluations)

      IRSuite.globalCounter = 0
      Interpret[Any](x, env, args, None)
      assert(IRSuite.globalCounter == expectedEvaluations)

      IRSuite.globalCounter = 0
      eval(x, env, args, None)
      assert(IRSuite.globalCounter == expectedEvaluations)
    }

    def i = In(0, TBoolean())

    def st = ApplySeeded("incr_s", FastSeq(True()), 0L)
    def sf = ApplySeeded("incr_s", FastSeq(True()), 0L)
    def sm = ApplySeeded("incr_s", FastSeq(NA(TBoolean())), 0L)

    def mt = ApplySeeded("incr_m", FastSeq(True()), 0L)
    def mf = ApplySeeded("incr_m", FastSeq(True()), 0L)
    def mm = ApplySeeded("incr_m", FastSeq(NA(TBoolean())), 0L)

    def vt = ApplySeeded("incr_v", FastSeq(True()), 0L)
    def vf = ApplySeeded("incr_v", FastSeq(True()), 0L)
    def vm = ApplySeeded("incr_v", FastSeq(NA(TBoolean())), 0L)

    // baseline
    test(st, true, 1); test(sf, true, 1); test(sm, true, 1)
    test(mt, true, 1); test(mf, true, 1); test(mm, true, 1)
    test(vt, true, 1); test(vf, true, 1); test(vm, true, 0)

    // if
    // condition
    test(If(st, i, True()), true, 1)
    test(If(sf, i, True()), true, 1)
    test(If(sm, i, True()), true, 1)

    test(If(mt, i, True()), true, 1)
    test(If(mf, i, True()), true, 1)
    test(If(mm, i, True()), true, 1)

    test(If(vt, i, True()), true, 1)
    test(If(vf, i, True()), true, 1)
    test(If(vm, i, True()), true, 0)

    // consequent
    test(If(i, st, True()), true, 1)
    test(If(i, sf, True()), true, 1)
    test(If(i, sm, True()), true, 1)

    test(If(i, mt, True()), true, 1)
    test(If(i, mf, True()), true, 1)
    test(If(i, mm, True()), true, 1)

    test(If(i, vt, True()), true, 1)
    test(If(i, vf, True()), true, 1)
    test(If(i, vm, True()), true, 0)

    // alternate
    test(If(i, True(), st), false, 1)
    test(If(i, True(), sf), false, 1)
    test(If(i, True(), sm), false, 1)

    test(If(i, True(), mt), false, 1)
    test(If(i, True(), mf), false, 1)
    test(If(i, True(), mm), false, 1)

    test(If(i, True(), vt), false, 1)
    test(If(i, True(), vf), false, 1)
    test(If(i, True(), vm), false, 0)
  }
}
