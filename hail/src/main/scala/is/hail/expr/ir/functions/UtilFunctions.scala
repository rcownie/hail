package is.hail.expr.ir.functions

import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.ir._
import is.hail.expr.types._
import is.hail.utils._
import is.hail.expr.types.coerce
import org.apache.spark.sql.Row

object UtilFunctions extends RegistryFunctions {

  def parseBoolean(s: String): Boolean = s.toBoolean

  def min(a: IR, b: IR): IR = If(ApplyComparisonOp(LT(a.typ), a, b), a, b)
  def max(a: IR, b: IR): IR = If(ApplyComparisonOp(GT(a.typ), a, b), a, b)

  def format(f: String, args: Row): String =
    String.format(f, args.toSeq.map(_.asInstanceOf[java.lang.Object]): _*)

  def registerAll() {
    val thisClass = getClass

    registerCode("triangle", TInt32(), TInt32()) { (_, n: Code[Int]) => (n * (n + 1)) / 2 }

    registerIR("isDefined", tv("T")) { a => ApplyUnaryPrimOp(Bang(), IsNA(a)) }
    registerIR("isMissing", tv("T")) { a => IsNA(a) }

    registerIR("[]", tv("T", _.isInstanceOf[TTuple]), TInt32()) { (a, i) => GetTupleElement(a, i.asInstanceOf[I32].x) }

    registerIR("range", TInt32(), TInt32(), TInt32())(ArrayRange)

    registerIR("range", TInt32(), TInt32())(ArrayRange(_, _, I32(1)))

    registerIR("range", TInt32())(ArrayRange(I32(0), _, I32(1)))

    registerIR("annotate", tv("T", _.isInstanceOf[TStruct]), tv("U", _.isInstanceOf[TStruct])) { (s, s2) =>
      s2 match {
        case s2: MakeStruct => InsertFields(s, s2.fields)
        case s2 =>
          val s2typ = coerce[TStruct](s2.typ)
          val s2id = genUID()
          Let(s2id, s2, InsertFields(s, s2typ.fieldNames.map { n => n -> GetField(Ref(s2id, s2typ), n) }))
      }
    }

    registerCode("toInt32", TBoolean(), TInt32()) { (_, x: Code[Boolean]) => x.toI }
    registerCode("toInt64", TBoolean(), TInt64()) { (_, x: Code[Boolean]) => x.toI.toL }
    registerCode("toFloat32", TBoolean(), TFloat32()) { (_, x: Code[Boolean]) => x.toI.toF }
    registerCode("toFloat64", TBoolean(), TFloat64()) { (_, x: Code[Boolean]) => x.toI.toD }
    registerCode("toInt32", TString(), TInt32()) { (mb, x: Code[Long]) =>
      val s = asm4s.coerce[String](wrapArg(mb, TString())(x))
      Code.invokeStatic[java.lang.Integer, String, Int]("parseInt", s)
    }
    registerCode("toInt64", TString(), TInt64()) { (mb, x: Code[Long]) =>
      val s = asm4s.coerce[String](wrapArg(mb, TString())(x))
      Code.invokeStatic[java.lang.Long, String, Long]("parseLong", s)
    }
    registerCode("toFloat32", TString(), TFloat32()) { (mb, x: Code[Long]) =>
      val s = asm4s.coerce[String](wrapArg(mb, TString())(x))
      Code.invokeStatic[java.lang.Float, String, Float]("parseFloat", s)
    }
    registerCode("toFloat64", TString(), TFloat64()) { (mb, x: Code[Long]) =>
      val s = asm4s.coerce[String](wrapArg(mb, TString())(x))
      Code.invokeStatic[java.lang.Double, String, Double]("parseDouble", s)
    }
    registerCode("toBoolean", TString(), TBoolean()) { (mb, x: Code[Long]) =>
      val s = asm4s.coerce[String](wrapArg(mb, TString())(x))
      Code.invokeScalaObject[String, Boolean](thisClass, "parseBoolean", s)
    }

    registerIR("min", tv("T"), tv("T"))(min)
    registerIR("max", tv("T"), tv("T"))(max)

    registerCode("format", TString(), tv("T", _.isInstanceOf[TTuple]), TString()) { (mb, format: Code[Long], args: Code[Long]) =>
      val typ = tv("T").subst()
      getRegion(mb).appendString(Code.invokeScalaObject[String, Row, String](thisClass, "format",
        asm4s.coerce[String](wrapArg(mb, TString())(format)),
        Code.checkcast[Row](asm4s.coerce[java.lang.Object](wrapArg(mb, typ)(args)))))
    }

    registerCodeWithMissingness("&&", TBoolean(), TBoolean(), TBoolean()) { (mb, l, r) =>
      val lm = Code(l.setup, l.m)
      val rm = Code(r.setup, r.m)

      val lv = l.value[Boolean]
      val rv = r.value[Boolean]

      val m = mb.newLocal[Boolean]
      val v = mb.newLocal[Boolean]
      val setup = Code(m := lm, v := !m && lv)
      val missing = m.mux(rm || rv, v && (rm || Code(v := rv, false)))
      val value = v

      EmitTriplet(setup, missing, value)
    }

    registerCodeWithMissingness("||", TBoolean(), TBoolean(), TBoolean()) { (mb, l, r) =>
      val lm = Code(l.setup, l.m)
      val rm = Code(r.setup, r.m)

      val lv = l.value[Boolean]
      val rv = r.value[Boolean]

      val m = mb.newLocal[Boolean]
      val v = mb.newLocal[Boolean]
      val setup = Code(m := lm, v := m || lv)
      val missing = m.mux(rm || !rv, !v && (rm || Code(v := rv, false)))
      val value = v

      EmitTriplet(setup, missing, value)
    }
  }
}