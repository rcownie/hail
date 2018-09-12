package is.hail.expr.types

import is.hail.annotations._
import is.hail.asm4s.Code
import is.hail.check._
import is.hail.expr.types.physical.PLocus
import is.hail.utils._
import is.hail.variant._

import scala.reflect.ClassTag
import scala.reflect.classTag

object TLocus {
  def representation(required: Boolean = false): TStruct = {
    val rep = TStruct(
      "contig" -> +TString(),
      "position" -> +TInt32())
    rep.setRequired(required).asInstanceOf[TStruct]
  }

  def schemaFromRG(rg: Option[ReferenceGenome], required: Boolean = false): Type = rg match {
    case Some(ref) => TLocus(ref)
    case None => TLocus.representation(required)
  }
}

case class TLocus(rg: RGBase, override val required: Boolean = false) extends ComplexType {
  lazy val physicalType: PLocus = PLocus(rg, required)

  def _toPretty = s"Locus($rg)"

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("locus<")
    sb.append(prettyIdentifier(rg.name))
    sb.append('>')
  }
  def _typeCheck(a: Any): Boolean = a.isInstanceOf[Locus]

  override def genNonmissingValue: Gen[Annotation] = Locus.gen(rg.asInstanceOf[ReferenceGenome])

  override def scalaClassTag: ClassTag[Locus] = classTag[Locus]

  val ordering: ExtendedOrdering =
    ExtendedOrdering.extendToNull(rg.locusOrdering)

  val representation: TStruct = TLocus.representation(required)

  def locusOrdering: Ordering[Locus] = rg.locusOrdering

  override def unify(concrete: Type): Boolean = concrete match {
    case TLocus(crg, _) => rg.unify(crg)
    case _ => false
  }

  override def clear(): Unit = rg.clear()

  override def subst() = rg.subst().locusType
  
  def contig(region: Code[Region], off: Code[Long]): Code[Long] = representation.loadField(region, off, 0)
  
  def position(region: Code[Region], off: Code[Long]): Code[Int] = region.loadInt(representation.loadField(region, off, 1))
}
