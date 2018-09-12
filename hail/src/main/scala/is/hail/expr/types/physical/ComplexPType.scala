package is.hail.expr.types.physical

import is.hail.annotations.UnsafeOrdering

abstract class ComplexPType extends PType {
  val representation: PType

  override def byteSize: Long = representation.byteSize

  override def alignment: Long = representation.alignment

  override def unsafeOrdering(missingGreatest: Boolean): UnsafeOrdering = representation.unsafeOrdering(missingGreatest)

  override def fundamentalType: PType = representation.fundamentalType
}
