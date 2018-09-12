package is.hail.expr

import is.hail.expr.types._
import is.hail.utils._
import is.hail.variant.{Call, RGBase, Locus}

trait HailRep[T] { self =>
  def typ: Type
}

trait HailRepFunctions {

  implicit object boolHr extends HailRep[Boolean] {
    def typ = TBoolean()
  }

  implicit object int32Hr extends HailRep[Int] {
    def typ = TInt32()
  }

  implicit object int64Hr extends HailRep[Long] {
    def typ = TInt64()
  }

  implicit object float32Hr extends HailRep[Float] {
    def typ = TFloat32()
  }

  implicit object float64Hr extends HailRep[Double] {
    def typ = TFloat64()
  }

  implicit object boxedboolHr extends HailRep[java.lang.Boolean] {
    def typ = TBoolean()
  }

  implicit object boxedInt32Hr extends HailRep[java.lang.Integer] {
    def typ = TInt32()
  }

  implicit object boxedInt64Hr extends HailRep[java.lang.Long] {
    def typ = TInt64()
  }

  implicit object boxedFloat32Hr extends HailRep[java.lang.Float] {
    def typ = TFloat32()
  }

  implicit object boxedFloat64Hr extends HailRep[java.lang.Double] {
    def typ = TFloat64()
  }

  implicit object stringHr extends HailRep[String] {
    def typ = TString()
  }

  object callHr extends HailRep[Call] {
    def typ = TCall()
  }

  implicit class locusHr(rg: RGBase) extends HailRep[Locus] {
    def typ = TLocus(rg)
  }

  implicit def intervalHr[T](implicit hrt: HailRep[T]) = new HailRep[Interval] {
    def typ = TInterval(hrt.typ)
  }

  implicit def arrayHr[T](implicit hrt: HailRep[T]) = new HailRep[IndexedSeq[T]] {
    def typ = TArray(hrt.typ)
  }

  implicit def setHr[T](implicit hrt: HailRep[T]) = new HailRep[Set[T]] {
    def typ = TSet(hrt.typ)
  }

  implicit def dictHr[K, V](implicit hrt: HailRep[K], hrt2: HailRep[V]) = new HailRep[Map[K, V]] {
    def typ = TDict(hrt.typ, hrt2.typ)
  }

  implicit def unaryHr[T, U](implicit hrt: HailRep[T], hru: HailRep[U]) = new HailRep[(Any) => Any] {
    def typ = TFunction(FastSeq(hrt.typ), hru.typ)
  }
}
