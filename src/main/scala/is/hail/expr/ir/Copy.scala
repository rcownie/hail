package is.hail.expr.ir

import is.hail.expr.BaseIR

object Copy {
  def apply(x: IR, newChildren: IndexedSeq[BaseIR]): BaseIR = {
    val children = newChildren.asInstanceOf[IndexedSeq[IR]]
    lazy val same = {
      assert(children.isEmpty)
      x
    }
    x match {
      case I32(_) => same
      case I64(_) => same
      case F32(_) => same
      case F64(_) => same
      case True() => same
      case False() => same
      case Cast(_, typ) =>
        val IndexedSeq(v) = children
        Cast(v, typ)
      case NA(_) => same
      case MapNA(name, _, _, typ) =>
        val IndexedSeq(value, body) = children
        MapNA(name, value, body, typ)
      case IsNA(value) =>
        val IndexedSeq(value) = children
        IsNA(value)
      case If(_, _, _, typ) =>
        val IndexedSeq(cond, cnsq, altr) = children
        If(cond, cnsq, altr, typ)
      case Let(name, _, _, typ) =>
        val IndexedSeq(value, body) = children
        Let(name, value, body, typ)
      case Ref(_, _) =>
        same
      case ApplyBinaryPrimOp(op, _, _, typ) =>
        val IndexedSeq(l, r) = children
        ApplyBinaryPrimOp(op, l, r, typ)
      case ApplyUnaryPrimOp(op, _, typ) =>
        val IndexedSeq(x) = children
        ApplyUnaryPrimOp(op, x, typ)
      case MakeArray(args, typ) =>
        assert(args.length == children.length)
        MakeArray(children, typ)
      case MakeArrayN(_, elementType) =>
        val IndexedSeq(len) = children
        MakeArrayN(len, elementType)
      case ArrayRef(_, _, typ) =>
        val IndexedSeq(a, i) = children
        ArrayRef(a, i, typ)
      case ArrayMissingnessRef(_, _) =>
        val IndexedSeq(a, i) = children
        ArrayMissingnessRef(a, i)
      case ArrayLen(_) =>
        val IndexedSeq(a) = children
        ArrayLen(a)
      case ArrayRange(_, _, _) =>
        val IndexedSeq(start, stop, step) = children
        ArrayRange(start, stop, step)
      case ArrayMap(_, name, _, elementTyp) =>
        val IndexedSeq(a, body) = children
        ArrayMap(a, name, body, elementTyp)
      case ArrayFilter(_, name, _) =>
        val IndexedSeq(a, cond) = children
        ArrayFilter(a, name, cond)
      case ArrayFold(_, _, accumName, valueName, _, typ) =>
        val IndexedSeq(a, zero, body) = children
        ArrayFold(a, zero, accumName, valueName, body, typ)
      case MakeStruct(fields, _) =>
        assert(fields.length == children.length)
        MakeStruct(fields.zip(children).map { case ((n, _), a) => (n, a) })
      case InsertFields(_, fields, typ) =>
        assert(children.length == fields.length + 1)
        InsertFields(children.head, fields.zip(children.tail).map { case ((n, _), a) => (n, a) })
      case GetField(_, name, typ) =>
        val IndexedSeq(o) = children
        GetField(o, name, typ)
      case GetFieldMissingness(_, name) =>
        val IndexedSeq(o) = children
        GetFieldMissingness(o, name)
      case AggIn(_) =>
        same
      case AggMap(_, name, _, typ) =>
        val IndexedSeq(a, body) = children
        AggMap(a, name, body, typ)
      case AggFilter(_, name, _, typ) =>
        val IndexedSeq(a, body) = children
        AggFilter(a, name, body, typ)
      case AggFlatMap(_, name, _, typ) =>
        val IndexedSeq(a, body) = children
        AggFlatMap(a, name, body, typ)
      case ApplyAggOp(_, op, args, typ) =>
        val a +: args = children
        ApplyAggOp(a, op, args, typ)
      case MakeTuple(_, typ) =>
        MakeTuple(children, typ)
      case GetTupleElement(_, idx, typ) =>
        val IndexedSeq(o) = children
        GetTupleElement(o, idx, typ)
      case In(_, _) =>
        same
      case InMissingness(_) =>
        same
      case Die(message) =>
        same
      case ApplyFunction(impl, args) =>
        ApplyFunction(impl, children)
    }
  }
}
