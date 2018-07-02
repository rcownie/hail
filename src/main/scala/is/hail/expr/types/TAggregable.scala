package is.hail.expr.types

import is.hail.annotations._
import is.hail.expr._
import is.hail.expr.ir.EmitMethodBuilder

import scala.reflect.ClassTag

object TAggregable {
  def apply(elementType: Type, symTab: SymbolTable): TAggregable = {
    val agg = TAggregable(elementType)
    agg.symTab = symTab
    agg
  }
}

final case class TAggregable(elementType: Type, override val required: Boolean = false) extends TContainer {
  val elementByteSize: Long = UnsafeUtils.arrayElementSize(elementType)

  val contentsAlignment: Long = elementType.alignment.max(4)

  override val fundamentalType: TArray = TArray(elementType.fundamentalType, required)

  // FIXME does symTab belong here?
  // not used for equality
  var symTab: SymbolTable = _

  def bindings: Array[(String, Type)] =
    (if (symTab != null) symTab.map { case (n, (_, t)) => (n, t) }.toArray else Array())

  override def unify(concrete: Type): Boolean = {
    concrete match {
      case TAggregable(celementType, _) => elementType.unify(celementType)
      case _ => false
    }
  }

  // FIXME symTab == null
  override def subst() = TAggregable(elementType.subst())

  override def isRealizable = false

  def _typeCheck(a: Any): Boolean =
    throw new RuntimeException("TAggregable is not realizable")

  override def _toPretty: String = s"Aggregable[${ elementType.toString }]"

  override def scalaClassTag: ClassTag[_ <: AnyRef] = elementType.scalaClassTag

  val ordering: ExtendedOrdering = null

  def codeOrdering(mb: EmitMethodBuilder, other: Type): CodeOrdering = null
}
