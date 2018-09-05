package is.hail.sparkextras

import is.hail.annotations._
import is.hail.rvd.{OrderedRVD, OrderedRVDPartitioner, OrderedRVDType, RVDContext}
import is.hail.utils._
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class OrderedDependency[T](
    oldPartitionerBc: Broadcast[OrderedRVDPartitioner],
    newIntervalListBc: Broadcast[IndexedSeq[Interval]],
    rdd: RDD[T]
) extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): Seq[Int] =
    oldPartitionerBc.value.getPartitionRange(newIntervalListBc.value(partitionId))
}

object RepartitionedOrderedRDD2 {
  def apply(prev: OrderedRVD, newRangeBounds: IndexedSeq[Interval]): ContextRDD[RVDContext, RegionValue] =
    ContextRDD(new RepartitionedOrderedRDD2(prev, newRangeBounds))
}

/**
  * Repartition 'prev' to comply with 'newRangeBounds', using narrow dependencies.
  * Assumes new key type is a prefix of old key type, so no reordering is
  * needed.
  */
class RepartitionedOrderedRDD2 private (prev: OrderedRVD, newRangeBounds: IndexedSeq[Interval])
  extends RDD[ContextRDD.ElementType[RVDContext, RegionValue]](prev.crdd.sparkContext, Nil) { // Nil since we implement getDependencies

  val prevCRDD: ContextRDD[RVDContext, RegionValue] = prev.boundary.crdd
  val typ: OrderedRVDType = prev.typ
  val oldPartitionerBc: Broadcast[OrderedRVDPartitioner] = prev.partitioner.broadcast(prevCRDD.sparkContext)
  val newRangeBoundsBc: Broadcast[IndexedSeq[Interval]] = prevCRDD.sparkContext.broadcast(newRangeBounds)

  require(newRangeBounds.forall{i => typ.kType.relaxedTypeCheck(i.start) && typ.kType.relaxedTypeCheck(i.end)})

  def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](newRangeBoundsBc.value.length) { i =>
      RepartitionedOrderedRDD2Partition(
        i,
        dependency.getParents(i).toArray.map(prevCRDD.partitions),
        newRangeBoundsBc.value(i))
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[RVDContext => Iterator[RegionValue]] = {
    val ordPartition = partition.asInstanceOf[RepartitionedOrderedRDD2Partition]
    val pord = typ.kType.ordering.intervalEndpointOrdering
    val range = ordPartition.range
    val ur = new UnsafeRow(typ.rowType)
    val key = new KeyedRow(ur, typ.kFieldIdx)

    Iterator.single { (ctx: RVDContext) =>
      ordPartition.parents.iterator
        .flatMap { parentPartition =>
          prevCRDD.iterator(parentPartition, context).flatMap(_(ctx))
        }.dropWhile { rv =>
          ur.set(rv)
          pord.lt(key, range.left)
        }.filter { rv =>
          ur.set(rv)
          pord.lteq(key, range.right)
        }
    }
  }

  val dependency = new OrderedDependency(
    oldPartitionerBc,
    newRangeBoundsBc,
    prevCRDD.rdd)

  override def getDependencies: Seq[Dependency[_]] = FastSeq(dependency)
}

case class RepartitionedOrderedRDD2Partition(
    index: Int,
    parents: Array[Partition],
    range: Interval
) extends Partition
