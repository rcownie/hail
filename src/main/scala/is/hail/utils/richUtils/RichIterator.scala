package is.hail.utils.richUtils

import java.io.PrintWriter

import is.hail.annotations.{Region, RegionValue, RegionValueBuilder}
import is.hail.expr.types.TStruct

import scala.collection.JavaConverters._
import scala.io.Source
import is.hail.utils.{FlipbookIterator, StagingIterator, StateMachine}
import org.apache.spark.sql.Row

class RichIterator[T](val it: Iterator[T]) extends AnyVal {
  def toStagingIterator: StagingIterator[T] = {
    val bit = it.buffered
    StagingIterator(
      new StateMachine[T] {
        def value: T = bit.head
        def isValid = bit.hasNext
        def advance() { bit.next() }
      }
    )
  }

  def toFlipbookIterator: FlipbookIterator[T] = toStagingIterator

  def foreachBetween(f: (T) => Unit)(g: => Unit) {
    if (it.hasNext) {
      f(it.next())
      while (it.hasNext) {
        g
        f(it.next())
      }
    }
  }

  def pipe(pb: ProcessBuilder,
    printHeader: (String => Unit) => Unit,
    printElement: (String => Unit, T) => Unit,
    printFooter: (String => Unit) => Unit): (Iterator[String], Process) = {

    val command = pb.command().asScala.mkString(" ")

    val proc = pb.start()

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + command) {
      override def run() {
        val out = new PrintWriter(proc.getOutputStream)

        printHeader(out.println)
        it.foreach(x => printElement(out.println, x))
        printFooter(out.println)
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    (Source.fromInputStream(proc.getInputStream).getLines(), proc)
  }

  def trueGroupedIterator(groupSize: Int): Iterator[Iterator[T]] =
    new Iterator[Iterator[T]] {
      var prev: Iterator[T] = null

      def hasNext: Boolean = it.hasNext

      def next(): Iterator[T] = {
        if (!hasNext)
          throw new NoSuchElementException("next on empty iterator")

        // the previous element must must be fully consumed or the next block will start in the wrong place
        assert(prev == null || !prev.hasNext)
        prev = new Iterator[T] {
          var i = 0

          def hasNext: Boolean = it.hasNext && i < groupSize

          def next(): T = {
            if (!hasNext)
              throw new NoSuchElementException("next on empty iterator")
            i += 1
            it.next()
          }
        }

        prev
      }
    }
}

class RichRowIterator(val it: Iterator[Row]) extends AnyVal {
  def toRegionValueIterator(rowTyp: TStruct): Iterator[RegionValue] = {
    val region = Region()
    val rvb = new RegionValueBuilder(region)
    val rv = RegionValue(region)
    it.map { row =>
      region.clear()
      rvb.start(rowTyp)
      rvb.addAnnotation(rowTyp, row)
      rv.setOffset(rvb.end())
      rv
    }
  }
}
