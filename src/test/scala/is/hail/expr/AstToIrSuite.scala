package is.hail.expr

import is.hail.expr.ir._
import is.hail.expr.types._
import org.testng.annotations.Test
import org.scalatest.testng.TestNGSuite

class ASTToIRSuite extends TestNGSuite {
  private def toIR[T](s: String): Option[IR] = {
    val ast = Parser.parseToAST(s, EvalContext(Map(
      "aggregable" -> (0, TAggregable(TInt32(),
        Map("agg" -> (0, TInt32()),
          "something" -> (1, TInt32())))))))
    ast.toIR(Some("aggregable"))
  }

  @Test
  def constants() {
    for {(in, out) <- Array(
      "3" -> I32(3),
      Int.MaxValue.toString -> I32(Int.MaxValue),
      "3.0" -> F64(3.0),
      "true" -> True(),
      "false" -> False(),
      "{}" -> MakeStruct(Seq()),
      "{a : 1}" -> MakeStruct(Seq(("a", I32(1)))),
      "{a: 1, b: 2}" -> MakeStruct(Seq(
        ("a", I32(1)), ("b", I32(2)))),
      "[1, 2]" -> MakeArray(Seq(I32(1), I32(2)), TArray(TInt32())),
      "[42.0]" -> MakeArray(Seq(F64(42.0)), TArray(TFloat64()))
    )
    } {
      assert(toIR(in).contains(out),
        s"expected '$in' to parse and convert into $out, but got ${ toIR(in) }")
    }
  }

  @Test
  def getField() {
    for {(in, out) <- Array(
      "{a: 1, b: 2}.a" ->
        GetField(
          MakeStruct(Seq(
            ("a", I32(1)), ("b", I32(2)))),
          "a",
          TInt32()),
      "{a: 1, b: 2}.b" ->
        GetField(
          MakeStruct(Seq(
            ("a", I32(1)), ("b", I32(2)))),
          "b",
          TInt32())
    )
    } {
      assert(toIR(in).contains(out),
        s"expected '$in' to parse and convert into $out, but got ${ toIR(in) }")
    }
  }

  @Test
  def let() {
    for {(in, out) <- Array(
      "let a = 0 and b = 3 in b" ->
        ir.Let("a", I32(0), ir.Let("b", I32(3), Ref("b", TInt32()), TInt32()), TInt32()),
      "let a = 0 and b = a in b" ->
        ir.Let("a", I32(0), ir.Let("b", Ref("a", TInt32()), Ref("b", TInt32()), TInt32()), TInt32()),
      "let i = 7 in i" ->
        ir.Let("i", I32(7), Ref("i", TInt32()), TInt32()),
      "let a = let b = 3 in b in a" ->
        ir.Let("a", ir.Let("b", I32(3), Ref("b", TInt32()), TInt32()), Ref("a", TInt32()), TInt32())
    )
    } {
      val r = toIR(in)
      Infer(r.get)
      Infer(out)
      assert(r.contains(out),
        s"expected '$in' to parse and convert into $out, but got ${ toIR(in) }")
    }
  }

  @Test
  def primOps() { for { (in, out) <- Array(
    "-1" -> ApplyUnaryPrimOp(Negate(), I32(1), TInt32()),
    "!true" -> ApplyUnaryPrimOp(Bang(), True(), TBoolean()),
    "1 / 2" -> ApplyBinaryPrimOp(FloatingPointDivide(), I32(1), I32(2), TFloat32()),
    "1.0 / 2.0" -> ApplyBinaryPrimOp(FloatingPointDivide(), F64(1.0), F64(2.0), TFloat64()),
    "1.0 < 2.0" -> ApplyBinaryPrimOp(LT(), F64(1.0), F64(2.0), TBoolean()),
    "1.0 <= 2.0" -> ApplyBinaryPrimOp(LTEQ(), F64(1.0), F64(2.0), TBoolean()),
    "1.0 >= 2.0" -> ApplyBinaryPrimOp(GTEQ(), F64(1.0), F64(2.0), TBoolean()),
    "1.0 > 2.0" -> ApplyBinaryPrimOp(GT(), F64(1.0), F64(2.0), TBoolean()),
    "1.0 == 2.0" -> ApplyBinaryPrimOp(EQ(), F64(1.0), F64(2.0), TBoolean()),
    "1.0 != 2.0" -> ApplyBinaryPrimOp(NEQ(), F64(1.0), F64(2.0), TBoolean()),
    "0 // 0 + 1" -> ApplyBinaryPrimOp(
      Add(),
      ApplyBinaryPrimOp(RoundToNegInfDivide(), I32(0), I32(0), TInt32()),
      I32(1),
      TInt32()),
    "0 // 0 * 1" -> ApplyBinaryPrimOp(
      Multiply(),
      ApplyBinaryPrimOp(RoundToNegInfDivide(), I32(0), I32(0), TInt32()),
      I32(1),
      TInt32())
  )
  } {
    assert(toIR(in).contains(out),
      s"expected '$in' to parse and convert into $out, but got ${toIR(in)}")
  }
  }

  @Test
  def aggs() { for { (in, out) <- Array(
    "aggregable.sum()" ->
      ApplyAggOp(AggIn(), Sum(), Seq()),
    "aggregable.map(x => x * 5).sum()" ->
      ApplyAggOp(
        AggMap(AggIn(), "x", ApplyBinaryPrimOp(Multiply(), Ref("x"), I32(5))),
        Sum(), Seq(), TInt32()),
    "aggregable.map(x => x * something).sum()" ->
      ApplyAggOp(
        AggMap(AggIn(), "x", ApplyBinaryPrimOp(Multiply(), Ref("x"), Ref("something"))),
        Sum(), Seq()),
    "aggregable.filter(x => x > 2).sum()" ->
      ApplyAggOp(
        AggFilter(AggIn(), "x", ApplyBinaryPrimOp(GT(), Ref("x"), I32(2))),
        Sum(), Seq()),
    "aggregable.flatMap(x => [x * 5]).sum()" ->
      ApplyAggOp(
        AggFlatMap(AggIn(), "x",
          MakeArray(Seq(ApplyBinaryPrimOp(Multiply(), Ref("x") ,I32(5))))),
        Sum(), Seq())
  )
  } {
    val tAgg = TAggregable(TInt32())
    tAgg.symTab = Map("something" -> (0, TInt32(): Type))
    Infer(out, Some(tAgg))
    val converted = toIR(in)
    converted.foreach(Infer(_, Some(tAgg)))
    assert(converted.contains(out),
      s"expected '$in' to parse and convert into $out, but got ${toIR(in)}")
  }
  }
}
