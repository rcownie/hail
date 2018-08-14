package is.hail.methods

import is.hail.rvd.{OrderedRVD, OrderedRVDType}
import is.hail.table.Table
import is.hail.utils.{Interval, IntervalTree}
import is.hail.variant.MatrixTable

import scala.collection.JavaConverters._

object MatrixFilterIntervals {
  def apply(mt: MatrixTable, jintervals: java.util.ArrayList[Interval], keep: Boolean): MatrixTable = {
    val intervals = IntervalTree(mt.rvd.typ.pkType.ordering, jintervals.asScala.toArray)
    mt.copy2(rvd = mt.rvd.filterIntervals(intervals, keep))
  }
}

object TableFilterIntervals {
  def apply(ht: Table, jintervals: java.util.ArrayList[Interval], keep: Boolean): Table = {
    assert(ht.key.isDefined)
    val orvd = ht.value.enforceOrderingRVD.asInstanceOf[OrderedRVD]
    val intervals = IntervalTree(orvd.typ.pkType.ordering, jintervals.asScala.toArray)
    ht.copy2(rvd = orvd.filterIntervals(intervals, keep))
  }
}
