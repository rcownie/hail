package is.hail.expr

import is.hail.asm4s
import is.hail.asm4s._
import is.hail.expr.types._

package object ir {
  def typeToTypeInfo(t: Type): TypeInfo[_] = t.fundamentalType match {
    case _: TInt32 => typeInfo[Int]
    case _: TInt64 => typeInfo[Long]
    case _: TFloat32 => typeInfo[Float]
    case _: TFloat64 => typeInfo[Double]
    case _: TBoolean => typeInfo[Boolean]
    case _: TBinary => typeInfo[Long]
    case _: TArray => typeInfo[Long]
    case _: TBaseStruct => typeInfo[Long]
    case _ => throw new RuntimeException(s"unsupported type found, $t")
  }

  def defaultValue(t: Type): Code[_] = typeToTypeInfo(t) match {
    case BooleanInfo => false
    case IntInfo => 0
    case LongInfo => 0L
    case FloatInfo => 0.0f
    case DoubleInfo => 0.0
    case ti => throw new RuntimeException(s"unsupported type found: $t whose type info is $ti")
  }

  // FIXME add InsertStruct IR node
  def insertStruct(s: ir.IR, typ: TStruct, name: String, v: ir.IR): ir.IR = {
    assert(typ.hasField(name))
    ir.MakeStruct(typ.fields.zipWithIndex.map { case (f, i) =>
      (f.name,
        if (f.name == name)
          v
        else
          ir.GetField(s, f.name))
    })
  }

  private[ir] def coerce[T](c: Code[_]): Code[T] = asm4s.coerce(c)

  private[ir] def coerce[T](lr: LocalRef[_]): LocalRef[T] = lr.asInstanceOf[LocalRef[T]]

  private[ir] def coerce[T](ti: TypeInfo[_]): TypeInfo[T] = ti.asInstanceOf[TypeInfo[T]]

  private[ir] def coerce[T <: Type](x: Type): T = {
    import is.hail.expr.types
    types.coerce[T](x)
  }
}
