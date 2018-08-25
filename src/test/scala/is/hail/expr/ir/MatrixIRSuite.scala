package is.hail.expr.ir

import is.hail.SparkSuite
import is.hail.expr.ir.TestUtils._
import is.hail.expr.types._
import is.hail.table.Table
import is.hail.utils._
import is.hail.TestUtils._
import is.hail.variant.MatrixTable
import org.apache.spark.sql.Row
import org.testng.annotations.{DataProvider, Test}

class MatrixIRSuite extends SparkSuite {

  def rangeMatrix: MatrixIR = MatrixTable.range(hc, 20, 20, Some(4)).ast

  def getRows(mir: MatrixIR): Array[Row] =
    MatrixRowsTable(mir).execute(hc).rdd.collect()

  def getCols(mir: MatrixIR): Array[Row] =
    MatrixColsTable(mir).execute(hc).rdd.collect()

  @Test def testScanCountBehavesLikeIndexOnRows() {
    val mt = rangeMatrix
    val oldRow = Ref("va", mt.typ.rvRowType)

    val newRow = InsertFields(oldRow, Seq("idx" -> IRScanCount))

    val newMatrix = MatrixMapRows(mt, newRow, None)
    val rows = getRows(newMatrix)
    assert(rows.forall { case Row(row_idx, idx) => row_idx == idx })
  }

  @Test def testScanCollectBehavesLikeRangeOnRows() {
    val mt = rangeMatrix
    val oldRow = Ref("va", mt.typ.rvRowType)

    val newRow = InsertFields(oldRow, Seq("range" -> IRScanCollect(GetField(oldRow, "row_idx"))))

    val newMatrix = MatrixMapRows(mt, newRow, None)
    val rows = getRows(newMatrix)
    assert(rows.forall { case Row(row_idx: Int, range: IndexedSeq[Int]) => range sameElements Array.range(0, row_idx) })
  }

  @Test def testScanCollectBehavesLikeRangeWithAggregationOnRows() {
    val mt = rangeMatrix
    val oldRow = Ref("va", mt.typ.rvRowType)

    val newRow = InsertFields(oldRow, Seq("n" -> IRAggCount, "range" -> IRScanCollect(GetField(oldRow, "row_idx").toL)))

    val newMatrix = MatrixMapRows(mt, newRow, None)
    val rows = getRows(newMatrix)
    assert(rows.forall { case Row(row_idx: Int, n: Long, range: IndexedSeq[Int]) => (n == 20) && (range sameElements Array.range(0, row_idx)) })
  }

  @Test def testScanCountBehavesLikeIndexOnCols() {
    val mt = rangeMatrix
    val oldCol = Ref("sa", mt.typ.colType)

    val newCol = InsertFields(oldCol, Seq("idx" -> IRScanCount))

    val newMatrix = MatrixMapCols(mt, newCol, None)
    val cols = getCols(newMatrix)
    assert(cols.forall { case Row(col_idx, idx) => col_idx == idx })
  }

  @Test def testScanCollectBehavesLikeRangeOnCols() {
    val mt = rangeMatrix
    val oldCol = Ref("sa", mt.typ.colType)

    val newCol = InsertFields(oldCol, Seq("range" -> IRScanCollect(GetField(oldCol, "col_idx"))))

    val newMatrix = MatrixMapCols(mt, newCol, None)
    val cols = getCols(newMatrix)
    assert(cols.forall { case Row(col_idx: Int, range: IndexedSeq[Int]) => range sameElements Array.range(0, col_idx) })
  }

  @Test def testScanCollectBehavesLikeRangeWithAggregationOnCols() {
    val mt = rangeMatrix
    val oldCol = Ref("sa", mt.typ.colType)

    val newCol = InsertFields(oldCol, Seq("n" -> IRAggCount, "range" -> IRScanCollect(GetField(oldCol, "col_idx").toL)))

    val newMatrix = MatrixMapCols(mt, newCol, None)
    val cols = getCols(newMatrix)
    assert(cols.forall { case Row(col_idx: Int, n: Long, range: IndexedSeq[Int]) => (n == 20) && (range sameElements Array.range(0, col_idx)) })
  }

  def rangeRowMatrix(start: Int, end: Int): MatrixIR = {
    val i = end - start
    val baseRange = MatrixTable.range(hc, i, 5, Some(math.min(4, i))).ast
    val row = Ref("va", baseRange.typ.rvRowType)
    MatrixMapRows(baseRange,
      InsertFields(
        row,
        FastIndexedSeq("row_idx" -> (GetField(row, "row_idx") + start))),
      Some(FastIndexedSeq("row_idx") ->
        FastIndexedSeq.empty))
  }

  @DataProvider(name = "unionRowsData")
  def unionRowsData(): Array[Array[Any]] = Array(
    Array(FastIndexedSeq(0 -> 0, 5 -> 7)),
    Array(FastIndexedSeq(0 -> 1, 5 -> 7)),
    Array(FastIndexedSeq(0 -> 6, 5 -> 7)),
    Array(FastIndexedSeq(2 -> 3, 0 -> 1, 5 -> 7)),
    Array(FastIndexedSeq(2 -> 4, 0 -> 3, 5 -> 7)),
    Array(FastIndexedSeq(3 -> 6, 0 -> 1, 5 -> 7)))

  @Test(dataProvider = "unionRowsData")
  def testMatrixUnionRows(ranges: IndexedSeq[(Int, Int)]) {
    val expectedOrdering = ranges.flatMap { case (start, end) =>
      Array.range(start, end)
    }.sorted

    val unioned = MatrixUnionRows(ranges.map { case (start, end) =>
      rangeRowMatrix(start, end)
    })
    val actualOrdering = getRows(unioned).map { case Row(i: Int) => i }

    assert(actualOrdering sameElements expectedOrdering)
  }

  @DataProvider(name = "explodeRowsData")
  def explodeRowsData(): Array[Array[Any]] = Array(
    Array(FastIndexedSeq("empty"), FastIndexedSeq()),
    Array(FastIndexedSeq("null"), null),
    Array(FastIndexedSeq("set"), FastIndexedSeq(1, 3)),
    Array(FastIndexedSeq("one"), FastIndexedSeq(3)),
    Array(FastIndexedSeq("na"), FastIndexedSeq(null)),
    Array(FastIndexedSeq("x", "y"), FastIndexedSeq(3)),
    Array(FastIndexedSeq("foo", "bar"), FastIndexedSeq(1, 3)),
    Array(FastIndexedSeq("a", "b", "c"), FastIndexedSeq()))

  @Test(dataProvider = "explodeRowsData")
  def testMatrixExplode(path: IndexedSeq[String], collection: IndexedSeq[Integer]) {
    val tarray = TArray(TInt32())
    val range = MatrixTable.range(hc, 5, 2, None).ast

    val field = path.init.foldRight(path.last -> toIRArray(collection))(_ -> IRStruct(_))
    val annotated = MatrixMapRows(range, InsertFields(Ref("va", range.typ.rvRowType), FastIndexedSeq(field)), None)

    val q = annotated.typ.rowType.query(path: _*)
    val exploded = getRows(MatrixExplodeRows(annotated, path.toIndexedSeq)).map(q(_).asInstanceOf[Integer])

    val expected = if (collection == null) Array[Integer]() else Array.fill(5)(collection).flatten
    assert(exploded sameElements expected)
  }

  // these two items are helper for UnlocalizedEntries testing,
  def makeLocalizedTable(data: Array[Array[Any]]): Table = {
    val rowRdd = sc.parallelize(data.map(Row.fromSeq(_)))
    val rowSig = TStruct(
      "idx" -> TInt32(),
      "animal" -> TString(),
      "__entries" -> TArray(TStruct("ent1" -> TString(), "ent2" -> TFloat64()))
    )
    val keyNames = IndexedSeq("idx")
    Table(hc, rowRdd, rowSig, Some(keyNames))
  }
  def getLocalizedCols: Table = {
    val cdata = Array(
      Array(1, "atag"),
      Array(2, "btag")
    )
    val colRdd = sc.parallelize(cdata.map(Row.fromSeq(_)))
    val colSig = TStruct("idx" -> TInt32(), "tag" -> TString())
    val keyNames = IndexedSeq("idx")
    Table(hc, colRdd, colSig, Some(keyNames))
  }

  @Test def testUnlocalizeEntries() {
    val rdata = Array(
      Array(1, "fish", IndexedSeq(Row.fromSeq(Array("a", 1.0)), Row.fromSeq(Array("x", 2.0)))),
      Array(2, "cat", IndexedSeq(Row.fromSeq(Array("b", 0.0)), Row.fromSeq(Array("y", 0.1)))),
      Array(3, "dog", IndexedSeq(Row.fromSeq(Array("c", -1.0)), Row.fromSeq(Array("z", 30.0))))
    )
    val rowTab = makeLocalizedTable(rdata)
    val colTab = getLocalizedCols

    val mir = UnlocalizeEntries(rowTab.tir, colTab.tir, "__entries")
    // cols are same
    val mtCols = MatrixColsTable(mir).execute(hc).rdd.collect()
    val taCols = colTab.tir.execute(hc).rdd.collect()
    assert(mtCols sameElements taCols)

    // Rows are same
    val mtRows = MatrixRowsTable(mir).execute(hc).rdd.collect()
    val taRows = rowTab.tir.execute(hc).rdd
    assert(mtRows sameElements taRows.map(row => Row.fromSeq(row.toSeq.take(2))).collect())

    // Round trip
    val localRows = new MatrixTable(hc, mir).localizeEntries("__entries").tir.execute(hc).rdd.collect()
    assert(localRows sameElements taRows.collect())
  }

  @Test def testUnlocalizeEntriesErrors() {
    val rdata = Array(
      Array(1, "fish", IndexedSeq(Row.fromSeq(Array("x", 2.0)))),
      Array(2, "cat", IndexedSeq(Row.fromSeq(Array("b", 0.0)), Row.fromSeq(Array("y", 0.1)))),
      Array(3, "dog", IndexedSeq(Row.fromSeq(Array("c", -1.0)), Row.fromSeq(Array("z", 30.0))))
    )
    val rowTab = makeLocalizedTable(rdata)
    val colTab = getLocalizedCols
    val mir = UnlocalizeEntries(rowTab.tir, colTab.tir, "__entries")
    // All rows must have the same number of elements in the entry field as colTab has rows
    try {
      val mv = mir.execute(hc)
      mv.rvd.count // force evaluation of mapPartitionsPreservesPartitioning in UnlocalizeEntries.execute
      assert(false, "should have thrown an error, as the number of columns must match "
        + "the number of array entries")
    } catch {
      case e: org.apache.spark.SparkException => {
        assert(e.getCause.getClass.toString.contains("HailException"))
        assert(e.getCause.getMessage.contains("incorrect entry array length"))
      }
    }

    // The entry field must be an array
    try {
      val mir1 = UnlocalizeEntries(rowTab.tir, colTab.tir, "animal")
    } catch {
      case e: is.hail.utils.HailException => {}
    }

    val rdata2 = Array(
      Array(1, "fish", null),
      Array(2, "cat", IndexedSeq(Row.fromSeq(Array("b", 0.0)), Row.fromSeq(Array("y", 0.1)))),
      Array(3, "dog", IndexedSeq(Row.fromSeq(Array("c", -1.0)), Row.fromSeq(Array("z", 30.0))))
    )
    val rowTab2 = makeLocalizedTable(rdata2)
    val mir2 = UnlocalizeEntries(rowTab2.tir, colTab.tir, "__entries")

    try {
      val mv = mir2.execute(hc)
      mv.rvd.count // force evaluation of mapPartitionsPreservesPartitioning in UnlocalizeEntries.execute
      assert(false, "should have thrown an error, as no array should be missing")
    } catch {
      case e: org.apache.spark.SparkException => {
        assert(e.getCause.getClass.toString.contains("HailException"))
        assert(e.getCause.getMessage.contains("missing"))
      }
    }
  }

  @Test def testMatrixFiltersWorkWithRandomness() {
    val range = MatrixTable.range(hc, 20, 20, Some(4)).ast
    val rand = ApplySeeded("rand_bool", FastIndexedSeq(0.5), seed=0)

    val cols = MatrixFilterCols(range, rand).execute(hc).nCols
    val rows = MatrixFilterRows(range, rand).execute(hc).rvd.count()
    val entries = MatrixEntriesTable(MatrixFilterEntries(range, rand)).execute(hc).rvd.count()

    assert(cols < 20 && cols > 0)
    assert(rows < 20 && rows > 0)
    assert(entries < 400 && entries > 0)
  }
}
