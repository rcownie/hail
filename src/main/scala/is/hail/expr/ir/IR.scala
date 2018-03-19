package is.hail.expr.ir

import is.hail.expr.types._
import is.hail.expr.BaseIR
import is.hail.expr.ir.functions.IRFunction

sealed trait IR extends BaseIR {
  def typ: Type

  override def children: IndexedSeq[BaseIR] =
    Children(this)

  override def copy(newChildren: IndexedSeq[BaseIR]): BaseIR =
    Copy(this, newChildren)
}

object Literal {
  def apply(x: Any, t: Type): IR = {
    if (x == null)
      return NA(t)
    t match {
      case _: TInt32 => I32(x.asInstanceOf[Int])
      case _: TInt64 => I64(x.asInstanceOf[Long])
      case _: TFloat32 => F32(x.asInstanceOf[Float])
      case _: TFloat64 => F64(x.asInstanceOf[Double])
      case _: TBoolean => if (x.asInstanceOf[Boolean]) True() else False()
      case _ => throw new RuntimeException("Unsupported literal type")
    }
  }
}

final case class I32(x: Int) extends IR { val typ = TInt32() }
final case class I64(x: Long) extends IR { val typ = TInt64() }
final case class F32(x: Float) extends IR { val typ = TFloat32() }
final case class F64(x: Double) extends IR { val typ = TFloat64() }
final case class True() extends IR { val typ = TBoolean() }
final case class False() extends IR { val typ = TBoolean() }
final case class StringConst(x: String) extends IR { val typ = TString() }

final case class Cast(v: IR, typ: Type) extends IR

final case class NA(typ: Type) extends IR
final case class MapNA(name: String, value: IR, body: IR, var typ: Type = null) extends IR
final case class IsNA(value: IR) extends IR { val typ = TBoolean() }

final case class If(cond: IR, cnsq: IR, altr: IR, var typ: Type = null) extends IR

final case class Let(name: String, value: IR, body: IR, var typ: Type = null) extends IR
final case class Ref(name: String, var typ: Type = null) extends IR

final case class ApplyBinaryPrimOp(op: BinaryOp, l: IR, r: IR, var typ: Type = null) extends IR
final case class ApplyUnaryPrimOp(op: UnaryOp, x: IR, var typ: Type = null) extends IR

final case class MakeArray(args: Seq[IR], var typ: TArray = null) extends IR
final case class MakeArrayN(len: IR, elementType: Type) extends IR { def typ: TArray = TArray(elementType) }
final case class ArrayRef(a: IR, i: IR, var typ: Type = null) extends IR
final case class ArrayMissingnessRef(a: IR, i: IR) extends IR { val typ: Type = TBoolean() }
final case class ArrayLen(a: IR) extends IR { val typ = TInt32() }
final case class ArrayRange(start: IR, stop: IR, step: IR) extends IR { val typ: TArray = TArray(TInt32()) }
final case class ArrayMap(a: IR, name: String, body: IR, var elementTyp: Type = null) extends IR { def typ: TArray = TArray(elementTyp) }
final case class ArrayFilter(a: IR, name: String, cond: IR) extends IR { def typ: TArray = coerce[TArray](a.typ) }
final case class ArrayFold(a: IR, zero: IR, accumName: String, valueName: String, body: IR, var typ: Type = null) extends IR

final case class AggIn(var typ: TAggregable = null) extends IR
final case class AggMap(a: IR, name: String, body: IR, var typ: TAggregable = null) extends IR
final case class AggFilter(a: IR, name: String, body: IR, var typ: TAggregable = null) extends IR
final case class AggFlatMap(a: IR, name: String, body: IR, var typ: TAggregable = null) extends IR
final case class ApplyAggOp(a: IR, op: AggOp, args: Seq[IR], var typ: Type = null) extends IR {
  def inputType: Type = coerce[TAggregable](a.typ).elementType
}

final case class MakeStruct(fields: Seq[(String, IR)], var typ: TStruct = null) extends IR

final case class InsertFields(old: IR, fields: Seq[(String, IR)], var typ: TStruct = null) extends IR

final case class GetField(o: IR, name: String, var typ: Type = null) extends IR
final case class GetFieldMissingness(o: IR, name: String) extends IR { val typ: Type = TBoolean() }

final case class MakeTuple(types: Seq[IR], var typ: TTuple = null) extends IR
final case class GetTupleElement(o: IR, idx: Int, var typ: Type = null) extends IR

final case class In(i: Int, var typ: Type) extends IR
final case class InMissingness(i: Int) extends IR { val typ: Type = TBoolean() }
// FIXME: should be type any
final case class Die(message: String) extends IR { val typ = TVoid }

final case class ApplyFunction(implementation: IRFunction, args: Seq[IR]) extends IR { val typ = implementation.returnType }
