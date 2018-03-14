package is.hail.expr.ir

import is.hail.asm4s._
import is.hail.expr.types._
import is.hail.utils._

object BinaryOp {
  private val returnType: ((BinaryOp, Type, Type)) => Option[Type] = lift {
    case (FloatingPointDivide(), _: TInt32, _: TInt32) => TFloat32()
    case (FloatingPointDivide(), _: TInt64, _: TInt64) => TFloat32()
    case (FloatingPointDivide(), _: TFloat32, _: TFloat32) => TFloat32()
    case (FloatingPointDivide(), _: TFloat64, _: TFloat64) => TFloat64()
    case (Add() | Subtract() | Multiply() | RoundToNegInfDivide(), _: TInt32, _: TInt32) => TInt32()
    case (Add() | Subtract() | Multiply() | RoundToNegInfDivide(), _: TInt64, _: TInt64) => TInt64()
    case (Add() | Subtract() | Multiply() | RoundToNegInfDivide(), _: TFloat32, _: TFloat32) => TFloat32()
    case (Add() | Subtract() | Multiply() | RoundToNegInfDivide(), _: TFloat64, _: TFloat64) => TFloat64()
    case (GT() | GTEQ() | LTEQ() | LT(), _: TInt32, _: TInt32) => TBoolean()
    case (GT() | GTEQ() | LTEQ() | LT(), _: TInt64, _: TInt64) => TBoolean()
    case (GT() | GTEQ() | LTEQ() | LT(), _: TFloat32, _: TFloat32) => TBoolean()
    case (GT() | GTEQ() | LTEQ() | LT(), _: TFloat64, _: TFloat64) => TBoolean()
    case (GT() | GTEQ() | LTEQ() | LT(), _: TString,  _: TString) => TBoolean()
    case (EQ() | NEQ(), _: TInt32, _: TInt32) => TBoolean()
    case (EQ() | NEQ(), _: TInt64, _: TInt64) => TBoolean()
    case (EQ() | NEQ(), _: TFloat32, _: TFloat32) => TBoolean()
    case (EQ() | NEQ(), _: TFloat64, _: TFloat64) => TBoolean()
    case (EQ() | NEQ(), _: TString,  _: TString) => TBoolean()
    case (EQ() | NEQ(), _: TBoolean, _: TBoolean) => TBoolean()
    case (DoubleAmpersand() | DoublePipe(), _: TBoolean, _: TBoolean) => TBoolean()
  }

  def returnTypeOption(op: BinaryOp, l: Type, r: Type): Option[Type] =
    returnType(op, l, r)

  def getReturnType(op: BinaryOp, l: Type, r: Type): Type =
    returnType(op, l, r).getOrElse(incompatible(l, r, op))

  private def incompatible[T](lt: Type, rt: Type, op: BinaryOp): T =
    throw new RuntimeException(s"Cannot apply $op to $lt and $rt")

  def emit(op: BinaryOp, lt: Type, rt: Type, l: Code[_], r: Code[_]): Code[_] = (lt, rt) match {
    case (_: TInt32, _: TInt32) =>
      val ll = coerce[Int](l)
      val rr = coerce[Int](r)
      op match {
        case Add() => ll + rr
        case Subtract() => ll - rr
        case Multiply() => ll * rr
        case FloatingPointDivide() => ll.toF / rr.toF
        case RoundToNegInfDivide() => Code.invokeStatic[Math, Int, Int, Int]("floorDiv", ll, rr)
        case GT() => ll > rr
        case GTEQ() => ll >= rr
        case LTEQ() => ll <= rr
        case LT() => ll < rr
        case EQ() => ll.ceq(rr)
        case NEQ() => ll.cne(rr)
        case _ => incompatible(lt, rt, op)
      }
    case (_: TInt64, _: TInt64) =>
      val ll = coerce[Long](l)
      val rr = coerce[Long](r)
      op match {
        case Add() => ll + rr
        case Subtract() => ll - rr
        case Multiply() => ll * rr
        case FloatingPointDivide() => ll.toF / rr.toF
        case RoundToNegInfDivide() => Code.invokeStatic[Math, Long, Long, Long]("floorDiv", ll, rr)
        case GT() => ll > rr
        case GTEQ() => ll >= rr
        case LTEQ() => ll <= rr
        case LT() => ll < rr
        case EQ() => ll.ceq(rr)
        case NEQ() => ll.cne(rr)
        case _ => incompatible(lt, rt, op)
      }
    case (_: TFloat32, _: TFloat32) =>
      val ll = coerce[Float](l)
      val rr = coerce[Float](r)
      op match {
        case Add() => ll + rr
        case Subtract() => ll - rr
        case Multiply() => ll * rr
        case FloatingPointDivide() => ll / rr
        case RoundToNegInfDivide() => Code.invokeStatic[Math, Float, Float]("floor", ll / rr)
        case GT() => ll > rr
        case GTEQ() => ll >= rr
        case LTEQ() => ll <= rr
        case LT() => ll < rr
        case EQ() => ll.ceq(rr)
        case NEQ() => ll.cne(rr)
        case _ => incompatible(lt, rt, op)
      }
    case (_: TFloat64, _: TFloat64) =>
      val ll = coerce[Double](l)
      val rr = coerce[Double](r)
      op match {
        case Add() => ll + rr
        case Subtract() => ll - rr
        case Multiply() => ll * rr
        case FloatingPointDivide() => ll / rr
        case RoundToNegInfDivide() => Code.invokeStatic[Math, Double, Double]("floor", ll / rr)
        case GT() => ll > rr
        case GTEQ() => ll >= rr
        case LTEQ() => ll <= rr
        case LT() => ll < rr
        case EQ() => ll.ceq(rr)
        case NEQ() => ll.cne(rr)
        case _ => incompatible(lt, rt, op)
      }
    case (_: TBoolean, _: TBoolean) =>
      val ll = coerce[Boolean](l)
      val rr = coerce[Boolean](r)
      op match {
        case DoubleAmpersand() => ll && rr
        case DoublePipe() => ll || rr
        case EQ() => ll.toI.ceq(rr.toI)
        case NEQ() => ll.toI ^ rr.toI
        case _ => incompatible(lt, rt, op)
      }
    case (_: TString, _: TString) =>
      // The operands may be either Code[String] or Code[LocalRef[Array[Byte]]] -
      // so we may need to build a String
      info(s"DEBUG: l is ${l}")
      info(s"DEBUG: r is ${r}")
      l match {
        case _: Code[Long] => info("DEBUG: l is Code[Long]")
        case _ =>
      }
      r match {
        case _: Code[Long] => info("DEBUG: r is Code[Long]")
        case _ =>
      }
      val ll = coerce[String](l)
      val rr = coerce[String](r)
      info(s"DEBUG: ll is ${ll}")
      info(s"DEBUG: rr is ${rr}")
      op match {
        case EQ() =>
          ll.invoke[Object, Boolean]("equals", Code.checkcast[Object](rr))
        case NEQ() =>
          ! Code.checkcast[String](ll).invoke[Object, Boolean]("equals", Code.checkcast[Object](rr))
        case _ => incompatible(lt, rt, op)
      }

    case _ => incompatible(lt, rt, op)
  }

  val fromString: PartialFunction[String, BinaryOp] = {
    case "+" => Add()
    case "-" => Subtract()
    case "*" => Multiply()
    case "/" => FloatingPointDivide()
    case "//" => RoundToNegInfDivide()
    case ">" => GT()
    case ">=" => GTEQ()
    case "<=" => LTEQ()
    case "<" => LT()
    case "==" => EQ()
    case "!=" => NEQ()
    case "&&" => DoubleAmpersand()
    case "||" => DoublePipe()
  }
}

sealed trait BinaryOp { }
case class Add() extends BinaryOp { }
case class Subtract() extends BinaryOp { }
case class Multiply() extends BinaryOp { }
case class FloatingPointDivide() extends BinaryOp { }
case class RoundToNegInfDivide() extends BinaryOp { }
case class GT() extends BinaryOp { }
case class GTEQ() extends BinaryOp { }
case class LTEQ() extends BinaryOp { }
case class LT() extends BinaryOp { }
case class EQ() extends BinaryOp { }
case class NEQ() extends BinaryOp { }
case class DoubleAmpersand() extends BinaryOp { }
case class DoublePipe() extends BinaryOp { }

