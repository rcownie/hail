package is.hail.expr.ir.functions

import is.hail.asm4s.Code
import is.hail.expr.types._
import is.hail.variant.{Call, Call2}

object CallFunctions extends RegistryFunctions {

  def registerAll() {
    registerScalaFunction("downcode", TCall(), TInt32(), TCall())(Call.getClass, "downcode")
    registerScalaFunction("UnphasedDiploidGtIndexCall", TInt32(), TCall())(Call2.getClass, "fromUnphasedDiploidGtIndex")

    registerCode("==", TCall(), TCall(), TBoolean()) { (_, a: Code[Int], b: Code[Int]) => a.ceq(b) }
  }
}
