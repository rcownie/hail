package is.hail.expr.ir

import is.hail.asm4s._
import is.hail.annotations._
import is.hail.annotations.aggregators._
import is.hail.expr.types._
import is.hail.utils._

import scala.collection.mutable
import scala.language.existentials
import scala.language.postfixOps

object Emit {
  type E = Env[(TypeInfo[_], Code[Boolean], Code[_])]

  type F = (Code[Boolean], Code[_]) => Code[Unit]

  private[ir] def toCode(ir: IR, fb: EmitFunctionBuilder[_], nSpecialArguments: Int): EmitTriplet = {
    emit(ir, fb, Env.empty, nSpecialArguments)
  }

  def apply(ir: IR, fb: EmitFunctionBuilder[_]) {
    apply(ir, fb, 1)
  }

  def apply(ir: IR, fb: EmitFunctionBuilder[_], nSpecialArguments: Int) {
    val triplet = emit(ir, fb, Env.empty, nSpecialArguments)
    typeToTypeInfo(ir.typ) match {
      case ti: TypeInfo[t] =>
        fb.emit(Code(triplet.setup, triplet.m.mux(
          Code._throw(Code.newInstance[RuntimeException, String]("cannot return empty")),
          coerce[t](triplet.v))))
    }
  }

  private def emit(
    ir: IR,
    fb: EmitFunctionBuilder[_],
    env: E,
    nSpecialArguments: Int): EmitTriplet = {
    TypeCheck(ir)
    new Emit(fb.apply_method, nSpecialArguments).emit(ir, env)
  }
}

case class EmitTriplet(setup: Code[Unit], m: Code[Boolean], v: Code[_]) {
  def value[T]: Code[T] = coerce[T](v)
}

case class EmitArrayTriplet(setup: Code[Unit], m: Option[Code[Boolean]], addElements: Code[Unit])

case class ArrayIteratorTriplet(calcLength: Code[Unit], length: Option[Code[Int]], arrayEmitter: Emit.F => EmitArrayTriplet) {
  def wrapContinuation(contMap: (Emit.F, Code[Boolean], Code[_]) => Code[Unit]): ArrayIteratorTriplet =
    copy(calcLength = calcLength, length = length, arrayEmitter = { cont: Emit.F => arrayEmitter(contMap(cont, _, _)) })
}

private class Emit(
  mb: EmitMethodBuilder,
  nSpecialArguments: Int) {

  private val maxBytecodeSizeTarget: Int = 4096
  private val opSize: Int = 20

  def getChunkBounds(sizes: Seq[Int]): Array[Int] = {
    var total = 0
    val ab = new ArrayBuilder[Int]()
    ab += 0
    sizes.zipWithIndex.foreach { case (size, i) =>
      if (total == 0 || total + size <= maxBytecodeSizeTarget)
        total += size
      else {
        ab += i
        total = 0
      }
    }
    ab += sizes.size
    ab.result()
  }

  val methods: mutable.Map[String, Seq[(Seq[Type], EmitMethodBuilder)]] = mutable.Map().withDefaultValue(FastSeq())

  import Emit.E
  import Emit.F

  private def wrapToMethod(irs: Seq[IR], env: E)(useValues: (EmitMethodBuilder, Type, EmitTriplet) => Code[Unit]): Code[Unit] = {
    def wrapCodeChunks(items: Seq[_], isIR: Boolean = false): Code[Unit] = {
      val sizes: Seq[Int] = if (isIR) irs.map(_.size * opSize) else Array.fill(items.size)(5)
      val chunkBounds = getChunkBounds(sizes)
      chunkBounds match {
        case Array(start, end) =>
          if (isIR) {
            val c = for (ir: IR <- items.asInstanceOf[Seq[IR]]) yield
              useValues(mb, ir.typ, emit(ir, env))
            coerce[Unit](Code(c: _*))
          } else
            coerce[Unit](Code(items.asInstanceOf[Seq[Code[Unit]]]: _*))
        case _ =>
          val chunks = chunkBounds.zip(chunkBounds.tail).map { case (start, end) =>
            val newMB = mb.fb.newMethod(mb.parameterTypeInfo, typeInfo[Unit])
            val c = {
              if (isIR) {
                val emitWrapper = new Emit(newMB, nSpecialArguments)
                for (ir: IR <- items.asInstanceOf[Seq[IR]].slice(start, end)) yield
                  useValues(newMB, ir.typ, emitWrapper.emit(ir, env))
              } else
                items.asInstanceOf[Seq[Code[Unit]]].slice(start, end)
            }
            newMB.emit(Code(c: _*))
            val args = mb.parameterTypeInfo.zipWithIndex.map { case (ti, i) => mb.getArg(i + 1)(ti).load() }
            coerce[Unit](newMB.invoke(args: _*))
          }
          wrapCodeChunks(chunks)
      }
    }

    wrapCodeChunks(irs, true)
  }

  /**
    * Invariants of the Returned Triplet
    * ----------------------------------
    *
    * The elements of the triplet are called (precompute, missingness, value)
    *
    *  1. evaluate each returned Code[_] at most once
    *  2. evaluate precompute *on all static code-paths* leading to missingness or value
    *  3. guard the the evaluation of value by missingness
    *
    *  Triplets returning values cannot have side-effects.  For void triplets, precompute
    *  contains the side effect, missingness is false, and value is {@code Code._empty}.
    *
    * JVM gotcha:
    * a variable must be initialized on all static code-paths prior to its use (ergo defaultValue)
    *
    * Argument Convention
    * -------------------
    *
    * {@code In(i)} occupies two argument slots, one for the value and one for a
    * missing bit. The value for {@code  In(0)} is passed as argument
    * {@code  nSpecialArguments + 1}. The missingness bit is the subsequent
    * argument. In general, the value for {@code  In(i)} appears at
    * {@code  nSpecialArguments + 1 + 2 * i}.
    *
    * There must always be at least one special argument: a {@code  Region} in
    * which the IR can allocate memory.
    *
    * Aggregating expressions must have at least two special arguments. As with
    * all expressions, the first argument must be a {@code  Region}. The second
    * argument is the {@code  Array[RegionValueAggregator]} that is used by {@code initOp} and
    * {@code SeqOp} to implement the aggregation. Note that the special arguments
    * do not appear in pairs, i.e., they may not be missing.
    *
    * When compiling an aggregation expression, {@code AggIn} refers to the first
    * argument {@code In(0)} whose type must be of type
    * {@code tAggIn.elementType}.  {@code tAggIn.symTab} is not used by Emit.
    *
    **/
  private def emit(ir: IR, env: E): EmitTriplet = {

    def emit(ir: IR, env: E = env): EmitTriplet =
      this.emit(ir, env)

    def emitArrayIterator(ir: IR, env: E = env) = this.emitArrayIterator(ir, env)

    val region = mb.getArg[Region](1).load()
    lazy val aggregator = {
      assert(nSpecialArguments >= 2)
      mb.getArg[Array[RegionValueAggregator]](2)
    }

    ir match {
      case I32(x) =>
        present(const(x))
      case I64(x) =>
        present(const(x))
      case F32(x) =>
        present(const(x))
      case F64(x) =>
        present(const(x))
      case Str(x) =>
        present(region.appendString(const(x)))
      case True() =>
        present(const(true))
      case False() =>
        present(const(false))
      case Void() =>
        EmitTriplet(Code._empty, const(false), Code._empty)

      case Cast(v, typ) =>
        val codeV = emit(v)
        val cast = Casts.get(v.typ, typ)
        EmitTriplet(codeV.setup, codeV.m, cast(codeV.v))

      case NA(typ) =>
        EmitTriplet(Code._empty, const(true), defaultValue(typ))
      case IsNA(v) =>
        val codeV = emit(v)
        EmitTriplet(codeV.setup, const(false), codeV.m)

      case If(cond, cnsq, altr) =>
        if (cnsq.typ == TVoid) {
          val codeCond = emit(cond)
          val codeCnsq = emit(cnsq)
          val codeAltr = emit(altr)
          EmitTriplet(
            Code(
              codeCond.setup,
              codeCond.m.mux(
                Code._empty,
                coerce[Boolean](codeCond.v).mux(
                  codeCnsq.setup,
                  codeAltr.setup))),
            const(false),
            Code._empty)
        } else {
          val typ = ir.typ
          val codeCond = emit(cond)
          val xvcond = mb.newLocal[Boolean]()
          val out = coerce[Any](mb.newLocal()(typeToTypeInfo(typ)))
          val mout = mb.newLocal[Boolean]()
          val codeCnsq = emit(cnsq)
          val codeAltr = emit(altr)
          val setup = Code(
            codeCond.setup,
            codeCond.m.mux(
              Code(mout := true, out := defaultValue(typ)),
              Code(
                xvcond := coerce[Boolean](codeCond.v),
                coerce[Boolean](xvcond).mux(
                  Code(codeCnsq.setup, mout := codeCnsq.m, out := codeCnsq.v),
                  Code(codeAltr.setup, mout := codeAltr.m, out := codeAltr.v)))))

          EmitTriplet(setup, mout, out)
        }

      case Let(name, value, body) =>
        val typ = ir.typ
        val vti = typeToTypeInfo(value.typ)
        val mx = mb.newField[Boolean]()
        val x = coerce[Any](mb.newField(name)(vti))
        val codeV = emit(value)
        val bodyenv = env.bind(name -> (vti, mx.load(), x.load()))
        val codeBody = emit(body, env = bodyenv)
        val setup = Code(
          codeV.setup,
          mx := codeV.m,
          x := mx.mux(defaultValue(value.typ), codeV.v),
          codeBody.setup)

        EmitTriplet(setup, codeBody.m, codeBody.v)
      case Ref(name, typ) =>
        val ti = typeToTypeInfo(typ)
        val (t, m, v) = env.lookup(name)
        assert(t == ti, s"$name type annotation, $typ, $t doesn't match typeinfo: $ti")
        EmitTriplet(Code._empty, m, v)

      case ApplyBinaryPrimOp(op, l, r) =>
        val typ = ir.typ
        val codeL = emit(l)
        val codeR = emit(r)
        EmitTriplet(Code(codeL.setup, codeR.setup),
          codeL.m || codeR.m,
          BinaryOp.emit(op, l.typ, r.typ, codeL.v, codeR.v))
      case ApplyUnaryPrimOp(op, x) =>
        val typ = ir.typ
        val v = emit(x)
        EmitTriplet(v.setup, v.m, UnaryOp.emit(op, x.typ, v.v))

      case MakeArray(args, typ) =>
        val srvb = new StagedRegionValueBuilder(mb, typ)
        val addElement = srvb.addIRIntermediate(typ.elementType)
        val addElts = { (newMB: EmitMethodBuilder, t: Type, v: EmitTriplet) =>
          Code(
            v.setup,
            v.m.mux(srvb.setMissing(), addElement(v.v)),
            srvb.advance())
        }
        present(Code(srvb.start(args.size, init = true), wrapToMethod(args, env)(addElts), srvb.offset))
      case x@ArrayRef(a, i) =>
        val typ = x.typ
        val ti = typeToTypeInfo(typ)
        val tarray = coerce[TArray](a.typ)
        val ati = coerce[Long](typeToTypeInfo(tarray))
        val codeA = emit(a)
        val codeI = emit(i)
        val xma = mb.newLocal[Boolean]()
        val xa = mb.newLocal()(ati)
        val xi = mb.newLocal[Int]
        val len = mb.newLocal[Int]
        val xmi = mb.newLocal[Boolean]()
        val xmv = mb.newLocal[Boolean]()
        val setup = Code(
          codeA.setup,
          xma := codeA.m,
          xa := coerce[Long](xma.mux(defaultValue(tarray), codeA.v)),
          codeI.setup,
          xmi := codeI.m,
          xi := coerce[Int](xmi.mux(defaultValue(TInt32()), codeI.v)),
          xmv := xma || xmi || !tarray.isElementDefined(region, xa, xi))

        EmitTriplet(setup, xmv, Code(
          len := tarray.loadLength(region, xa),
          (xi < len).mux(
            region.loadIRIntermediate(typ)(tarray.elementOffset(xa, len, xi)),
            Code._fatal(
              const("array index out of bounds: ")
                .invoke[String, String]("concat", xi.load().toS)
                .invoke[String, String]("concat", " / ")
                .invoke[String, String]("concat", len.load().toS)
            ))))
      case ArrayLen(a) =>
        val codeA = emit(a)
        EmitTriplet(codeA.setup, codeA.m, TContainer.loadLength(region, coerce[Long](codeA.v)))

      case _: ArraySort | _: ToSet | _: ToDict =>
        val a = ir.children(0).asInstanceOf[IR]
        val atyp = coerce[TArray](a.typ)

        val aout = emitArrayIterator(a)
        val vab = new StagedArrayBuilder(atyp.elementType, mb, 16)
        val sorter = new ArraySorter(mb, vab, keyOnly = ir.isInstanceOf[ToDict])

        val cont = { (m: Code[Boolean], v: Code[_]) =>
          m.mux(vab.addMissing(), vab.add(v))
        }

        val processArrayElts = aout.arrayEmitter(cont)
        EmitTriplet(processArrayElts.setup, processArrayElts.m.getOrElse(const(false)), Code(
          vab.clear,
          aout.calcLength,
          processArrayElts.addElements,
          sorter.sortIntoRegion(distinct = !ir.isInstanceOf[ArraySort])))

      case ToArray(a) =>
        emit(a)

      case x@(_: SetContains | _: DictContains) =>
        val (set, elem, bs) = (x: @unchecked) match {
          case SetContains(s, e) =>
            val bs = new BinarySearch(mb, coerce[TSet](s.typ), keyOnly = false)
            (s, e, bs)
          case DictContains(dict, key) =>
            val bs = new BinarySearch(mb, coerce[TDict](dict.typ), keyOnly = true)
            (dict, key, bs)
        }
        val s = emit(set)
        val e = emit(elem)

        val smx = mb.newLocal[Boolean]
        val svx = mb.newLocal[Long]
        val emx = mb.newLocal[Boolean]
        val evx = mb.newLocal()(typeToTypeInfo(elem.typ))
        val isIn = bs.isIndexEqual(svx, bs.getClosestIndex(svx, emx, evx), emx, evx)
        EmitTriplet(
          Code(s.setup, e.setup),
          Code(smx := s.m, smx),
          Code(Code(svx := s.value[Long],
            emx := e.m,
            evx.storeAny(emx.mux(defaultValue(elem.typ), e.v)),
            isIn)))

      case x@DictGet(dict, key) =>
        val dtype = coerce[TDict](dict.typ)
        val d = emit(dict)
        val k = emit(key)
        val bs = new BinarySearch(mb, dtype, keyOnly = true)
        val dmx = mb.newLocal[Boolean]
        val dvx = mb.newLocal[Long]
        val kmx = mb.newLocal[Boolean]
        val kvx = mb.newLocal()(typeToTypeInfo(key.typ))
        val i = mb.newLocal[Int]
        val pair = mb.newLocal[Long]
        val mx = mb.newLocal[Boolean]
        val isEq = mb.newLocal[Boolean]
        val elt = coerce[TBaseStruct](dtype.elementType)
        val keyIn = Code(
          dvx := d.value[Long],
          kmx := k.m,
          kmx.mux(Code._empty, kvx.storeAny(k.v)),
          i := bs.getClosestIndex(dvx, kmx, kvx),
          isEq := bs.isIndexEqual(dvx, i, kmx, kvx),
          isEq)
        val isValueMissing = Code(
          pair := dtype.loadElement(region, dvx, i),
          elt.isFieldMissing(region, pair, 1))
        val getValue =
          region.loadIRIntermediate(x.typ)(elt.fieldOffset(pair, 1))

        val setupDefaults = Code(
          dmx := d.m,
          dvx := 0L,
          kmx := false,
          kvx.storeAny(defaultValue(key.typ)),
          i := -1,
          pair := 0L)

        EmitTriplet(
          Code(d.setup, k.setup),
          Code(setupDefaults, mx := (dmx || !keyIn || isValueMissing), mx),
          getValue)

      case _: ArrayMap | _: ArrayFilter | _: ArrayRange | _: ArrayFlatMap =>
        val elt = coerce[TArray](ir.typ).elementType
        val srvb = new StagedRegionValueBuilder(mb, ir.typ)

        val aout = emitArrayIterator(ir)
        aout.length match {
          case Some(len) =>
            val cont = { (m: Code[Boolean], v: Code[_]) =>
              coerce[Unit](
                Code(
                  m.mux(
                    srvb.setMissing(),
                    srvb.addIRIntermediate(elt)(v)),
                  srvb.advance()))
            }
            val processAElts = aout.arrayEmitter(cont)
            EmitTriplet(processAElts.setup, processAElts.m.getOrElse(const(false)), Code(
              aout.calcLength,
              srvb.start(len, init = true),
              processAElts.addElements,
              srvb.offset
            ))

          case None =>
            val len = mb.newLocal[Int]
            val i = mb.newLocal[Int]
            val vab = new StagedArrayBuilder(elt, mb, 16)

            val cont = { (m: Code[Boolean], v: Code[_]) =>
              m.mux(vab.addMissing(), vab.add(v))
            }

            val processArrayElts = aout.arrayEmitter(cont)
            EmitTriplet(processArrayElts.setup, processArrayElts.m.getOrElse(const(false)), Code(
              vab.clear,
              aout.calcLength,
              processArrayElts.addElements,
              len := vab.size,
              srvb.start(len, init = true),
              i := 0,
              Code.whileLoop(i < len,
                vab.isMissing(i).mux(
                  srvb.setMissing(),
                  srvb.addIRIntermediate(elt)(vab(i))),
                i := i + 1,
                srvb.advance()),
              srvb.offset
            ))
        }

      case ArrayFold(a, zero, name1, name2, body) =>
        val typ = ir.typ
        val tarray = coerce[TArray](a.typ)
        val tti = typeToTypeInfo(typ)
        val eti = typeToTypeInfo(tarray.elementType)
        val xmv = mb.newField[Boolean](name2+"_missing")
        val xvv = coerce[Any](mb.newField(name2)(eti))
        val xmout = mb.newField[Boolean](name1+"_missing")
        val xvout = coerce[Any](mb.newField(name1)(tti))
        val i = mb.newLocal[Int]("af_i")
        val len = mb.newLocal[Int]("af_len")
        val bodyenv = env.bind(
          name1 -> (tti, xmout.load(), xvout.load()),
          name2 -> (eti, xmv.load(), xvv.load()))

        val codeZ = emit(zero)
        val codeB = emit(body, env = bodyenv)

        val aBase = emitArrayIterator(a)

        val cont = { (m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := v,
            codeB.setup,
            xmout := codeB.m,
            xvout := xmout.mux(defaultValue(typ), codeB.v))
        }

        val processAElts = aBase.arrayEmitter(cont)
        val ma = processAElts.m.getOrElse(const(false))

        EmitTriplet(Code(
          processAElts.setup,
          xmout := true,
          xvout := defaultValue(typ),
          xmout := ma.mux(
            ma,
            Code(
              codeZ.setup,
              xmout := codeZ.m,
              xvout := xmout.mux(defaultValue(typ), codeZ.v),
              aBase.calcLength,
              processAElts.addElements,
              xmout)),
          xvout := xmout.mux(defaultValue(typ), xvout)
        ), xmout, xvout)

      case ArrayFor(a, valueName, body) =>
        val tarray = coerce[TArray](a.typ)
        val eti = typeToTypeInfo(tarray.elementType)
        val xmv = mb.newField[Boolean]()
        val xvv = coerce[Any](mb.newField(valueName)(eti))
        val bodyenv = env.bind(
          valueName -> (eti, xmv.load(), xvv.load()))
        val codeB = emit(body, env = bodyenv)
        val aBase = emitArrayIterator(a)
        val cont = { (m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := v,
            codeB.setup)
        }

        val processAElts = aBase.arrayEmitter(cont)
        val ma = processAElts.m.getOrElse(const(false))
        EmitTriplet(
          Code(
            processAElts.setup,
            ma.mux(
              Code._empty,
              Code(aBase.calcLength, processAElts.addElements))),
          const(false),
          Code._empty)

      case InitOp(i, args, aggSig) =>
        val nArgs = args.length
        val argsm = Array.fill[ClassFieldRef[Boolean]](nArgs)(mb.newField[Boolean]())
        val argsv = (0 until nArgs).map(i => mb.newField(typeToTypeInfo(args(i).typ))).toArray

        val codeI = emit(i)
        val codeA = args.map(ir => emit(ir))

        val argsSetup = Code((0 until nArgs).map { i =>
          val a = codeA(i)
          Code(
            argsm(i) := a.m,
            argsv(i).storeAny(argsm(i).mux(
              defaultValue(args(i).typ),
              a.v
            ))
          )
        }.toArray: _*)

        val agg = AggOp.get(aggSig)
        EmitTriplet(
          Code(codeI.setup,
            Code(codeA.map(_.setup): _*),
            argsSetup,
            codeI.m.mux(
              Code._empty,
              agg.initOp(
                aggregator(coerce[Int](codeI.v)),
                argsv.map(Code(_)),
                argsm.map(Code(_).asInstanceOf[Code[Boolean]])))),
          const(false),
          Code._empty)

      case x@SeqOp(a, i, aggSig) =>
        val codeA = emit(a)
        val codeI = emit(i)
        val agg = AggOp.get(aggSig)
        EmitTriplet(
          Code(codeI.setup, codeA.setup,
            codeI.m.mux(
              Code._empty,
               agg.seqOp(aggregator(coerce[Int](codeI.v)), codeA.v, codeA.m))),
          const(false),
          Code._empty)

      case Begin(xs) =>
        EmitTriplet(
          wrapToMethod(xs, env) { case (_, t, code) =>
            code.setup
          },
          const(false),
          Code._empty)

      case x@MakeStruct(fields) =>
        val srvb = new StagedRegionValueBuilder(mb, x.typ)
        val addFields = { (newMB: EmitMethodBuilder, t: Type, v: EmitTriplet) =>
          Code(
            v.setup,
            v.m.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(v.v)),
            srvb.advance())
        }
        present(Code(srvb.start(init = true), wrapToMethod(fields.map(_._2), env)(addFields), srvb.offset))

      case x@SelectFields(ir, fields) =>
        val old = emit(ir)
        val oldt = coerce[TStruct](ir.typ)
        val oldv = mb.newLocal[Long]
        val srvb = new StagedRegionValueBuilder(mb, x.typ)

        val addFields =
          Code(srvb.start(),
            Code(fields.map { name =>
                val i = oldt.fieldIdx(name)
                val t = oldt.types(i)
                val fieldMissing = oldt.isFieldMissing(region, oldv, i)
                val fieldValue = region.loadIRIntermediate(t)(oldt.fieldOffset(oldv, i))
                Code(
                  fieldMissing.mux(
                    srvb.setMissing(),
                    srvb.addIRIntermediate(t)(fieldValue)),
                  srvb.advance())
              }: _*))

        EmitTriplet(
          old.setup,
          old.m,
          Code(
            oldv := old.value[Long],
            addFields,
            srvb.offset))


      case x@InsertFields(old, fields) =>
        old.typ match {
          case oldtype: TStruct =>
            val codeOld = emit(old)
            val xo = mb.newLocal[Long]
            val xmo = mb.newLocal[Boolean]()
            val updateInit = Map(fields.filter { case (name, _) => oldtype.hasField(name) }
              .map { case (name, v) => name -> (v.typ, emit(v)) }: _*)
            val appendInit = fields.filter { case (name, _) => !oldtype.hasField(name) }
              .map { case (_, v) => (v.typ, emit(v)) }
            val srvb = new StagedRegionValueBuilder(mb, x.typ)
            present(Code(
              srvb.start(init = true),
              Code(
                codeOld.setup,
                xo := coerce[Long](codeOld.v),
                xmo := codeOld.m,
                Code(oldtype.fields.map { f =>
                  updateInit.get(f.name) match {
                    case Some((t, EmitTriplet(dov, mv, vv))) =>
                      Code(
                        dov,
                        mv.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(vv)),
                        srvb.advance())
                    case None =>
                      Code(
                        (xmo || oldtype.isFieldMissing(region, xo, f.index)).mux(
                          srvb.setMissing(),
                          srvb.addIRIntermediate(f.typ)(region.loadIRIntermediate(f.typ)(oldtype.fieldOffset(xo, f.index)))
                        ),
                        srvb.advance())
                  }
                }: _*)),
              Code(appendInit.map { case (t, EmitTriplet(setup, mv, vv)) =>
                Code(
                  setup,
                  mv.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(vv)),
                  srvb.advance())
              }: _*),
              srvb.offset))
          case _ =>
            val newIR = MakeStruct(fields)
            emit(newIR)
        }

      case GetField(o, name) =>
        val t = coerce[TStruct](o.typ)
        val fieldIdx = t.fieldIdx(name)
        val codeO = emit(o)
        val xmo = mb.newLocal[Boolean]()
        val xo = mb.newLocal[Long]
        val setup = Code(
          codeO.setup,
          xmo := codeO.m,
          xo := coerce[Long](xmo.mux(defaultValue(t), codeO.v)))
        EmitTriplet(setup,
          xmo || !t.isFieldDefined(region, xo, fieldIdx),
          region.loadIRIntermediate(t.types(fieldIdx))(t.fieldOffset(xo, fieldIdx)))

      case x@MakeTuple(fields) =>
        val srvb = new StagedRegionValueBuilder(mb, x.typ)
        val addFields = { (newMB: EmitMethodBuilder, t: Type, v: EmitTriplet) =>
          Code(
            v.setup,
            v.m.mux(srvb.setMissing(), srvb.addIRIntermediate(t)(v.v)),
            srvb.advance())
        }
        present(Code(srvb.start(init = true), wrapToMethod(fields, env)(addFields), srvb.offset))

      case GetTupleElement(o, idx) =>
        val t = coerce[TTuple](o.typ)
        val codeO = emit(o)
        val xmo = mb.newLocal[Boolean]()
        val xo = mb.newLocal[Long]
        val setup = Code(
          codeO.setup,
          xmo := codeO.m,
          xo := coerce[Long](xmo.mux(defaultValue(t), codeO.v)))
        EmitTriplet(setup,
          xmo || !t.isFieldDefined(region, xo, idx),
          region.loadIRIntermediate(t.types(idx))(t.fieldOffset(xo, idx)))

      case In(i, typ) =>
        EmitTriplet(Code._empty,
          mb.getArg[Boolean](normalArgumentPosition(i) + 1),
          mb.getArg(normalArgumentPosition(i))(typeToTypeInfo(typ)))
      case Die(m) =>
        present(Code._throw(Code.newInstance[RuntimeException, String](m)))
      case ir@ApplyIR(fn, args, conversion) =>
        emit(ir.explicitNode)
      case ir@Apply(fn, args) =>
        val impl = ir.implementation
        val unified = impl.unify(args.map(_.typ))
        assert(unified)

        val meth =
          methods(fn).filter { case (argt, _) => argt.zip(args.map(_.typ)).forall { case (t1, t2) => t1 isOfType t2 } } match {
            case Seq(f) =>
              f._2
            case Seq() =>
              val methodbuilder = impl.getAsMethod(mb.fb, args.map(_.typ): _*)
              methods.update(fn, methods(fn) :+ (args.map(_.typ), methodbuilder))
              methodbuilder
          }
        val codeArgs = args.map(emit(_))
        val vars = args.map { a => coerce[Any](mb.newLocal()(typeToTypeInfo(a.typ))) }
        val ins = vars.zip(codeArgs.map(_.v)).map { case (l, i) => l := i }
        val setup = coerce[Unit](Code(codeArgs.map(_.setup): _*))
        val missing = if (codeArgs.isEmpty) const(false) else codeArgs.map(_.m).reduce(_ || _)
        val value = Code(ins :+ meth.invoke(mb.getArg[Region](1).load() +: vars.map { a => a.load() }: _*): _*)
        EmitTriplet(setup, missing, value)
      case x@ApplySpecial(_, args) =>
        x.implementation.argTypes.foreach(_.clear())
        val unified = x.implementation.unify(args.map(_.typ))
        assert(unified)
        x.implementation.apply(mb, args.map(emit(_)): _*)
    }
  }

  private def emitArrayIterator(ir: IR, env: E): ArrayIteratorTriplet = {

    def emit(ir: IR, env: E = env) = this.emit(ir, env)

    def emitArrayIterator(ir: IR, env: E = env) = this.emitArrayIterator(ir, env)

    val region = mb.getArg[Region](1).load()

    ir match {
      case x@ArrayRange(startir, stopir, stepir) =>
        val codeStart = emit(startir)
        val codeStop = emit(stopir)
        val codeStep = emit(stepir)

        val start = mb.newLocal[Int]("ar_start")
        val stop = mb.newLocal[Int]("ar_stop")
        val step = mb.newLocal[Int]("ar_step")

        val i = mb.newLocal[Int]("ar_i")
        val len = mb.newLocal[Int]("ar_len")
        val llen = mb.newLocal[Long]("ar_llen")

        val calcLength = Code(
          start := coerce[Int](codeStart.v),
          stop := coerce[Int](codeStop.v),
          step := coerce[Int](codeStep.v),
          step.ceq(0).mux(
            Code._fatal("Array range cannot have step size 0."),
            Code._empty[Unit]),
          llen := start.ceq(stop).mux(0L,
            (step < 0).mux(
              (start.toL - stop.toL - 1L) / (-step).toL + 1L,
              (stop.toL - start.toL - 1L) / step.toL + 1L)),
          (llen > const(Int.MaxValue.toLong)).mux(
            Code._fatal("Array range cannot have more than MAXINT elements."),
            len := (llen < 0L).mux(0L, llen).toI)
        )

        ArrayIteratorTriplet(calcLength, Some(len.load()), { continuation: F =>
          EmitArrayTriplet(
            coerce[Unit](Code(codeStart.setup, codeStop.setup, codeStep.setup)),
            Some(codeStart.m || codeStop.m || codeStep.m),
            Code(
              i := 0,
              Code.whileLoop(i < len,
                continuation(false, start),
                i := i + 1,
                start := start + step)))
        })
      case x@ArrayFilter(a, name, condition) =>
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(x.typ.elementType))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val condenv = env.bind(name -> (elementTypeInfoA, xmv.load(), xvv.load()))
        val codeCond = emit(condition, condenv)

        val filterCont = { (cont: F, m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xvv := xmv.mux(
              defaultValue(x.typ.elementType),
              v),
            Code(
              codeCond.setup,
              (codeCond.m || !coerce[Boolean](codeCond.v)).mux(
                Code._empty,
                cont(xmv, xvv))))
        }
        emitArrayIterator(a).copy(length = None).wrapContinuation(filterCont)

      case x@ArrayFlatMap(a, name, body) =>
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(coerce[TArray](a.typ).elementType))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val bodyenv = env.bind(name -> (elementTypeInfoA, xmv.load(), xvv.load()))
        val bodyIt = emitArrayIterator(body, bodyenv)

        val bodyCont = { (cont: F, m: Code[Boolean], v: Code[_]) =>
          val bodyArray = bodyIt.arrayEmitter(cont)
          Code(
            xmv := m,
            xmv.mux(
              Code._empty,
              Code(
                xvv := v,
                bodyArray.setup,
                bodyArray.m.map(_.mux(
                  Code._empty,
                  Code(bodyIt.calcLength, bodyArray.addElements))).getOrElse(Code(bodyIt.calcLength, bodyArray.addElements)))))
        }

        emitArrayIterator(a).copy(length = None).wrapContinuation(bodyCont)

      case x@ArrayMap(a, name, body) =>
        val elt = coerce[TArray](a.typ).elementType
        val elementTypeInfoA = coerce[Any](typeToTypeInfo(elt))
        val xmv = mb.newField[Boolean]()
        val xvv = mb.newField(name)(elementTypeInfoA)
        val bodyenv = env.bind(name -> (elementTypeInfoA, xmv.load(), xvv.load()))
        val codeB = emit(body, bodyenv)
        val mapCont = { (continuation: F, m: Code[Boolean], v: Code[_]) =>
          Code(
            xmv := m,
            xmv.mux(
              xvv := defaultValue(elt),
              xvv := v),
            codeB.setup,
            continuation(codeB.m, codeB.v))
        }
        emitArrayIterator(a).wrapContinuation(mapCont)

      case MakeArray(args, _) =>
        val f = { cont: F =>
          EmitArrayTriplet(Code._empty[Unit], None, coerce[Unit](Code(
            for (elt <- args) yield {
              val codeElt = emit(elt)
              Code(codeElt.setup, cont(codeElt.m, codeElt.v))
            })))
        }
        ArrayIteratorTriplet(Code._empty, Some(const(args.length)), f)

      case If(cond, cnsq, altr) =>
        val codeCond = emit(cond)
        val xmcond = mb.newLocal[Boolean]()
        val xvcond = mb.newLocal[Boolean]()
        val mout = mb.newLocal[Boolean]()
        val aCnsq = emitArrayIterator(cnsq)
        val aAltr = emitArrayIterator(altr)

        val f = { cont: F =>
          val addCnsq = aCnsq.arrayEmitter(cont)
          val addAltr = aAltr.arrayEmitter(cont)
          val setup = Code(
            codeCond.setup,
            xmcond := codeCond.m,
            xmcond.mux(
              Code(
                mout := true,
                xvcond := coerce[Boolean](defaultValue(cond.typ))
              ),
              Code(
                mout := false,
                xvcond := coerce[Boolean](codeCond.v),
                addCnsq.setup, addAltr.setup)))
          val missing: Code[Boolean] = if (addCnsq.m.isEmpty && addAltr.m.isEmpty)
            mout
          else
            Code(
              xmcond.mux(Code._empty,
                mout := xvcond.mux(addCnsq.m.getOrElse(false), addAltr.m.getOrElse(false))),
              mout)
          val add = xvcond.mux(addCnsq.addElements, addAltr.addElements)
          EmitArrayTriplet(setup, Some(missing), add)
        }

        val lenCalc = xvcond.mux(aCnsq.calcLength, aAltr.calcLength)
        val optLen = aCnsq.length.flatMap(l1 => aAltr.length.map(xvcond.mux(l1, _)))
        ArrayIteratorTriplet(lenCalc, optLen, f)

      case _ =>
        val t: TArray = coerce[TArray](ir.typ)
        val i = mb.newLocal[Int]("i")
        val len = mb.newLocal[Int]("len")
        val aoff = mb.newLocal[Long]("aoff")
        val codeV = emit(ir, env)
        val calcLength = Code(
          aoff := coerce[Long](codeV.v),
          len := t.loadLength(region, aoff))
        ArrayIteratorTriplet(calcLength, Some(len.load()), { continuation: F =>
          EmitArrayTriplet(codeV.setup, Some(codeV.m), Code(
            i := 0,
            Code.whileLoop(i < len,
              continuation(t.isElementMissing(region, aoff, i),
                region.loadIRIntermediate(t.elementType)(t.elementOffsetInRegion(region, aoff, i))),
              i := i + 1)))
        })
    }
  }

  private def present(x: Code[_]): EmitTriplet =
    EmitTriplet(Code._empty, const(false), x)

  private def normalArgumentPosition(idx: Int): Int = {
    1 + nSpecialArguments + idx * 2
  }
}
