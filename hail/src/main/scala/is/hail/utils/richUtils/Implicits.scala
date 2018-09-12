package is.hail.utils.richUtils

import java.io.InputStream

import breeze.linalg.DenseMatrix
import is.hail.annotations.{JoinedRegionValue, Region, RegionValue}
import is.hail.asm4s.Code
import is.hail.io.RichContextRDDRegionValue
import is.hail.rvd.RVDContext
import is.hail.io.{InputBuffer, RichContextRDDRegionValue}
import is.hail.sparkextras._
import is.hail.utils.{ArrayBuilder, HailIterator, JSONWriter, MultiArray2, Truncatable, WithContext}
import org.apache.hadoop
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.json4s.JValue

import scala.collection.{TraversableOnce, mutable}
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.matching.Regex

trait Implicits {
  implicit def toRichArray[T](a: Array[T]): RichArray[T] = new RichArray(a)

  implicit def toRichBoolean(b: Boolean): RichBoolean = new RichBoolean(b)

  implicit def toRichDenseMatrixDouble(m: DenseMatrix[Double]): RichDenseMatrixDouble = new RichDenseMatrixDouble(m)

  implicit def toRichEnumeration[T <: Enumeration](e: T): RichEnumeration[T] = new RichEnumeration(e)

  implicit def toRichHadoopConfiguration(hConf: hadoop.conf.Configuration): RichHadoopConfiguration =
    new RichHadoopConfiguration(hConf)

  implicit def toRichInt(i: Int): RichInt = new RichInt(i)

  implicit def toRichIndexedRowMatrix(irm: IndexedRowMatrix): RichIndexedRowMatrix = new RichIndexedRowMatrix(irm)

  implicit def toRichIntPairTraversableOnce[V](t: TraversableOnce[(Int, V)]): RichIntPairTraversableOnce[V] =
    new RichIntPairTraversableOnce[V](t)

  implicit def toRichIterable[T](i: Iterable[T]): RichIterable[T] = new RichIterable(i)

  implicit def toRichIterable[T](a: Array[T]): RichIterable[T] = new RichIterable(a)

  implicit def toRichContextIterator[T](it: Iterator[WithContext[T]]): RichContextIterator[T] = new RichContextIterator[T](it)

  implicit def toRichIterator[T](it: Iterator[T]): RichIterator[T] = new RichIterator[T](it)

  implicit def toRichRowIterator(it: Iterator[Row]): RichRowIterator = new RichRowIterator(it)

  implicit def toRichLong(l: Long): RichLong = new RichLong(l)

  implicit def toRichMap[K, V](m: Map[K, V]): RichMap[K, V] = new RichMap(m)

  implicit def toRichMultiArray2Long(ma: MultiArray2[Long]): RichMultiArray2Long = new RichMultiArray2Long(ma)

  implicit def toRichMultiArray2Int(ma: MultiArray2[Int]): RichMultiArray2Int = new RichMultiArray2Int(ma)

  implicit def toRichMultiArray2Double(ma: MultiArray2[Double]): RichMultiArray2Double = new RichMultiArray2Double(ma)

  implicit def toRichMutableMap[K, V](m: mutable.Map[K, V]): RichMutableMap[K, V] = new RichMutableMap(m)

  implicit def toRichOption[T](o: Option[T]): RichOption[T] = new RichOption[T](o)

  implicit def toRichOrderedArray[T: Ordering](a: Array[T]): RichOrderedArray[T] = new RichOrderedArray(a)

  implicit def toRichOrderedSeq[T: Ordering](s: Seq[T]): RichOrderedSeq[T] = new RichOrderedSeq[T](s)

  implicit def toRichPairRDD[K, V](r: RDD[(K, V)])(implicit kct: ClassTag[K],
    vct: ClassTag[V]): RichPairRDD[K, V] = new RichPairRDD(r)

  implicit def toRichRDD[T](r: RDD[T])(implicit tct: ClassTag[T]): RichRDD[T] = new RichRDD(r)

  implicit def toRichContextRDDRegionValue(r: ContextRDD[RVDContext, RegionValue]): RichContextRDDRegionValue = new RichContextRDDRegionValue(r)

  implicit def toRichRDDByteArray(r: RDD[Array[Byte]]): RichRDDByteArray = new RichRDDByteArray(r)

  implicit def toRichRegex(r: Regex): RichRegex = new RichRegex(r)

  implicit def toRichRow(r: Row): RichRow = new RichRow(r)

  implicit def toRichSC(sc: SparkContext): RichSparkContext = new RichSparkContext(sc)

  implicit def toRichString(str: String): RichString = new RichString(str)

  implicit def toRichStringBuilder(sb: mutable.StringBuilder): RichStringBuilder = new RichStringBuilder(sb)

  implicit def toRichStorageLevel(sl: StorageLevel): RichStorageLevel = new RichStorageLevel(sl)

  implicit def toTruncatable(s: String): Truncatable = s.truncatable()

  implicit def toTruncatable[T](it: Iterable[T]): Truncatable = it.truncatable()

  implicit def toTruncatable(arr: Array[_]): Truncatable = toTruncatable(arr: Iterable[_])

  implicit def toHailIteratorDouble(it: HailIterator[Int]): HailIterator[Double] = new HailIterator[Double] {
    override def next(): Double = it.next().toDouble
    override def hasNext: Boolean = it.hasNext
  }

  implicit def toRichInputStream(in: InputStream): RichInputStream = new RichInputStream(in)

  implicit def toRichJoinedRegionValue(jrv: JoinedRegionValue): RichJoinedRegionValue =
    new RichJoinedRegionValue(jrv)

  implicit def toRichCodeRegion(r: Code[Region]): RichCodeRegion = new RichCodeRegion(r)

  implicit def toRichPartialKleisliOptionFunction[A, B](x: PartialFunction[A, Option[B]]): RichPartialKleisliOptionFunction[A, B] = new RichPartialKleisliOptionFunction(x)

  implicit def toContextPairRDDFunctions[C <: AutoCloseable, K: ClassTag, V: ClassTag](x: ContextRDD[C, (K, V)]): ContextPairRDDFunctions[C, K, V] = new ContextPairRDDFunctions(x)

  implicit def toRichContextRDD[T: ClassTag](x: ContextRDD[RVDContext, T]): RichContextRDD[T] = new RichContextRDD(x)

  implicit def toRichCodeInputBuffer(in: Code[InputBuffer]): RichCodeInputBuffer = new RichCodeInputBuffer(in)
}
