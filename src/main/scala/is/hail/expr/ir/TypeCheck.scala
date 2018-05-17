package is.hail.expr.ir

import is.hail.expr.types._

object TypeCheck {
  def apply(ir: IR, aggEnv: Option[Env[Type]] = None) {
    apply(ir, new Env[Type](), aggEnv)
  }

  def apply(ir: IR, env: Env[Type], aggEnv: Option[Env[Type]]) {
    def check(ir: IR, env: Env[Type] = env, aggEnv: Option[Env[Type]] = aggEnv) {
      apply(ir, env, aggEnv)
    }

    ir match {
      case I32(x) =>
      case I64(x) =>
      case F32(x) =>
      case F64(x) =>
      case True() =>
      case False() =>
      case Str(x) =>
      case Void() =>

      case Cast(v, typ) =>
        check(v)
        assert(Casts.valid(v.typ, typ))

      case NA(t) =>
        assert(t != null)
      case IsNA(v) =>
        check(v)

      case x@If(cond, cnsq, altr) =>
        check(cond)
        check(cnsq)
        check(altr)
        assert(cond.typ.isOfType(TBoolean()))
        assert(cnsq.typ == altr.typ, s"${ cnsq.typ }, ${ altr.typ }, $cond")
        assert(x.typ == cnsq.typ)

      case x@Let(name, value, body) =>
        check(value)
        check(body, env = env.bind(name, value.typ))
        assert(x.typ == body.typ)
      case x@Ref(name, _) =>
        val expected = env.lookup(x)
        assert(x.typ == expected, s"$name ${ x.typ.parsableString() } ${ expected.parsableString() }")
      case x@ApplyBinaryPrimOp(op, l, r) =>
        check(l)
        check(r)
        assert(x.typ == BinaryOp.getReturnType(op, l.typ, r.typ))
      case x@ApplyUnaryPrimOp(op, v) =>
        check(v)
        assert(x.typ == UnaryOp.getReturnType(op, v.typ))
      case x@MakeArray(args, typ) =>
        if (args.length == 0)
          assert(typ != null)
        else {
          args.foreach(check(_))
          val t = args.head.typ
          args.map(_.typ).zipWithIndex.tail.foreach { case (x, i) => assert(x.isOfType(t), s"at position $i type mismatch: $t $x") }
          assert(x.typ == TArray(t))
        }
      case x@ArrayRef(a, i) =>
        check(a)
        check(i)
        assert(i.typ.isOfType(TInt32()))
        assert(x.typ == -coerce[TArray](a.typ).elementType)
      case ArrayLen(a) =>
        check(a)
        assert(a.typ.isInstanceOf[TArray])
      case x@ArrayRange(a, b, c) =>
        check(a)
        check(b)
        check(c)
        assert(a.typ.isOfType(TInt32()))
        assert(b.typ.isOfType(TInt32()))
        assert(c.typ.isOfType(TInt32()))
      case x@ArraySort(a) =>
        check(a)
        assert(a.typ.isInstanceOf[TArray])
      case x@ToSet(a) =>
        check(a)
        assert(a.typ.isInstanceOf[TArray])
      case x@ToDict(a) =>
        check(a)
        assert(a.typ.isInstanceOf[TArray])
        assert(coerce[TBaseStruct](coerce[TArray](a.typ).elementType).size == 2)
      case x@ToArray(a) =>
        check(a)
        assert(a.typ.isInstanceOf[TContainer])
      case x@SetContains(set, elem) =>
        check(set)
        check(elem)
        assert(set.typ.isInstanceOf[TSet])
        assert(-coerce[TSet](set.typ).elementType == elem.typ)
      case x@DictContains(dict, key) =>
        check(dict)
        check(key)
        assert(dict.typ.isInstanceOf[TDict])
        assert(-coerce[TDict](dict.typ).keyType == key.typ)
      case x@DictGet(dict, key) =>
        check(dict)
        check(key)
        assert(dict.typ.isInstanceOf[TDict])
        assert(-coerce[TDict](dict.typ).keyType == key.typ)
      case x@ArrayMap(a, name, body) =>
        check(a)
        val tarray = coerce[TArray](a.typ)
        check(body, env = env.bind(name, -tarray.elementType))
        assert(x.elementTyp == body.typ)
      case x@ArrayFilter(a, name, cond) =>
        check(a)
        val tarray = coerce[TArray](a.typ)
        check(cond, env = env.bind(name, tarray.elementType))
        assert(cond.typ.isOfType(TBoolean()))
      case x@ArrayFlatMap(a, name, body) =>
        check(a)
        val tarray = coerce[TArray](a.typ)
        check(body, env = env.bind(name, tarray.elementType))
        assert(body.typ.isInstanceOf[TArray])
      case x@ArrayFold(a, zero, accumName, valueName, body) =>
        check(a)
        val tarray = coerce[TArray](a.typ)
        check(zero)
        check(body, env = env.bind(accumName -> zero.typ, valueName -> -tarray.elementType))
        assert(body.typ == zero.typ)
        assert(x.typ == zero.typ)
      case x@ArrayFor(a, valueName, body) =>
        check(a)
        val tarray = coerce[TArray](a.typ)
        check(body, env = env.bind(valueName -> -tarray.elementType))
        assert(body.typ == TVoid)
      case x@InitOp(i, args, _) =>
        args.foreach(check(_))
        check(i)
        assert(i.typ.isInstanceOf[TInt32])
      case x@SeqOp(a, i, aggSig) =>
        check(a)
        check(i)
        assert(a.typ == aggSig.inputType)
        assert(i.typ.isInstanceOf[TInt32])
      case x@Begin(xs) =>
        xs.foreach { x =>
          check(x)
          assert(x.typ == TVoid)
        }
      case x@ApplyAggOp(a, constructorArgs, initOpArgs, aggSig) =>
        check(a, env = aggEnv.get)
        constructorArgs.foreach(check(_))
        initOpArgs.foreach(_.foreach(check(_)))
        assert(a.typ == TVoid)
        assert(x.typ == AggOp.getType(aggSig))
      case x@MakeStruct(fields) =>
        fields.foreach { case (name, a) => check(a) }
        assert(x.typ == TStruct(fields.map { case (name, a) =>
          (name, a.typ)
        }: _*))
      case x@SelectFields(old, fields) =>
        check(old)
        assert{
          val oldfields = coerce[TStruct](old.typ).fieldNames.toSet
          fields.forall { id => oldfields.contains(id) }
        }
      case x@InsertFields(old, fields) =>
        check(old)
        fields.foreach { case (name, a) => check(a) }
        assert(x.typ == fields.foldLeft(old.typ) { case (t, (name, a)) =>
          t match {
            case t2: TStruct =>
              t2.selfField(name) match {
                case Some(f2) => t2.updateKey(name, f2.index, a.typ)
                case None => t2.appendKey(name, a.typ)
              }
            case _ => TStruct(name -> a.typ)
          }
        }.asInstanceOf[TStruct])
      case x@GetField(o, name) =>
        check(o)
        val t = coerce[TStruct](o.typ)
        assert(t.index(name).nonEmpty, s"$name not in $t")
        assert(x.typ == -t.field(name).typ)
      case x@MakeTuple(types) =>
        types.foreach { a => check(a) }
        assert(x.typ == TTuple(types.map(_.typ): _*))
      case x@GetTupleElement(o, idx) =>
        check(o)
        val t = coerce[TTuple](o.typ)
        assert(idx >= 0 && idx < t.size)
        assert(x.typ == -t.types(idx))
      case In(i, typ) =>
        assert(typ != null)
      case Die(msg) =>
      case x@ApplyIR(fn, args, conversion) =>
        check(x.explicitNode)
      case x@Apply(fn, args) =>
        val impl = x.implementation
        args.foreach(check(_))
        assert(x.implementation.unify(args.map(_.typ)))
      case x@ApplySpecial(fn, args) =>
        val impl = x.implementation
        args.foreach(check(_))
        assert(x.implementation.unify(args.map(_.typ)))
      case MatrixWrite(_, _) =>
      case x@TableAggregate(child, query) =>
        check(query,
          env = child.typ.aggEnv,
          aggEnv = Some(child.typ.tAgg.symTab.foldLeft(Env.empty[Type]) { case (env, (n, (i, t))) =>
              env.bind(n, t)
          }))
        assert(x.typ == query.typ)
      case TableWrite(_, _, _, _) =>
      case TableExport(_, _, _, _, _) =>
      case TableCount(_) =>
    }
  }
}
