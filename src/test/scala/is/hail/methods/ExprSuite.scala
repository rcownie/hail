package is.hail.methods

import is.hail.TestUtils._
import is.hail.annotations.Annotation
import is.hail.check.Gen
import is.hail.check.Prop._
import is.hail.check.Properties
import is.hail.expr._
import is.hail.expr.types._
import is.hail.utils.StringEscapeUtils._
import is.hail.utils._
import is.hail.variant._
import is.hail.{SparkSuite, TestUtils}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.Matchers._
import org.testng.annotations.Test

class ExprSuite extends SparkSuite {

  @Test def compileTest() {
    def run[T](s: String): Option[T] =
      Option(Parser.parseToAST(s, EvalContext()).run(EvalContext())().asInstanceOf[T])

    assert(run[Int]("3").contains(3))
    assert(run[Long](Int.MaxValue.toString).contains(Int.MaxValue))
    assert(run[Double]("3.0").contains(3.0))
    assert(run[Boolean]("true").contains(true))
    assert(run[Boolean]("false").contains(false))
    assert(run[String](""""foo"""").contains("foo"))

    assert(run[String]("'foo'").contains("foo"))
    assert(run[String]("'\"foo'").contains("\"foo"))
    assert(run[String]("'\\'foo'").contains("'foo"))

    assert(run[String]("""if (true) "foo" else "bar"""").contains("foo"))
    assert(run[String]("""if (false) "foo" else "bar"""").contains("bar"))
    assert(run[Int]("""if (true) 1 else 0""").contains(1))
    assert(run[Int]("""if (false) 1 else 0""").contains(0))

    assert(run[IndexedSeq[String]]("""["a"]""").contains(Array("a"): IndexedSeq[String]))
    assert(run[IndexedSeq[Int]]("[1][:0]").contains(Array[Int]() : IndexedSeq[Int]))
    assert(run[IndexedSeq[Int]]("[1]").contains(Array(1): IndexedSeq[Int]))
    assert(run[IndexedSeq[Int]]("[1,2]").contains(Array(1, 2): IndexedSeq[Int]))
    assert(run[IndexedSeq[Int]]("[1,2,3]").contains(Array(1, 2, 3): IndexedSeq[Int]))

    assert(run[Annotation]("Tuple()").contains(Annotation()))
    assert(run[Annotation]("Tuple(5)").contains(Annotation(5)))
    assert(run[Annotation]("Tuple(5, Tuple(3, 4))").contains(Annotation(5, Annotation(3, 4))))
    assert(run[Int]("let t = Tuple(5, Tuple(3, 4)) in t[1][1]").contains(4))
    assert(run[IndexedSeq[Annotation]]("[ Tuple((1), (\"hello\")), Tuple((1), (\"hello\")) ]").contains(Array(Annotation(1, "hello"), Annotation(1, "hello")): IndexedSeq[Annotation]))


    assert(run[Annotation]("{}").contains(Annotation()))
    assert(run[Annotation]("{a: 1}").contains(Annotation(1)))
    assert(run[Annotation]("{a: 1, b: 2}").contains(Annotation(1, 2)))
    assert(run[Int]("{a: 1, b: 2}.a").contains(1))
    assert(run[Int]("{a: 1, b: 2}.b").contains(2))
    assert(run[String]("""{a: "a", b: "b"}.b""").contains("b"))
    assert(run[Annotation]("""{a: {aa: "aa"}, b: "b"}.a""").contains(Annotation("aa")))
    assert(run[String]("""{a: {aa: "aa"}, b: "b"}.a.aa""").contains("aa"))

    assert(run[Int]("let a = 0 and b = 3 in b").contains(3))
    assert(run[Int]("let a = 0 and b = a in b").contains(0))
    assert(run[Int]("let i = 7 in i").contains(7))
    assert(run[Int]("let a = let b = 3 in b in a").contains(3))

    assert(run[Int](""""abc".length""").contains(3))
    assert(run[IndexedSeq[String]](""""a,b,c".split(",")""").contains(Array("a", "b", "c"): IndexedSeq[String]))
    assert(run[IndexedSeq[String]](""" "1:5-100".firstMatchIn("([^:]*)[:\\t](\\d+)[\\-\\t](\\d+)") """).contains(Array("1", "5", "100"): IndexedSeq[String]))
    assert(run[IndexedSeq[String]](""" "".firstMatchIn("([^:]*)[:\\t](\\d+)[\\-\\t](\\d+)") """).isEmpty)
    assert(run[Int]("(3.0).toInt32()").contains(3))
    assert(run[Double]("(3).toFloat64()").contains(3.0))
  }

  @Test def exprTest() {
    val symTab = Map("i" -> (0, TInt32()),
      "j" -> (1, TInt32()),
      "d" -> (2, TFloat64()),
      "d2" -> (3, TFloat64()),
      "s" -> (4, TString()),
      "s2" -> (5, TString()),
      "a" -> (6, TArray(TInt32())),
      "m" -> (7, TInt32()),
      "as" -> (8, TArray(TStruct(("a", TInt32()),
        ("b", TString())))),
      "gs" -> (9, TStruct(("noCall", TCall()),
        ("homRef", TCall()),
        ("het", TCall()),
        ("homVar", TCall()),
        ("hetNonRef35", TCall()))),
      "t" -> (10, TBoolean()),
      "f" -> (11, TBoolean()),
      "mb" -> (12, TBoolean()),
      "is" -> (13, TString()),
      "iset" -> (14, TSet(TInt32())),
      "genedict" -> (15, TDict(TString(), TInt32())),
      "structArray" -> (16, TArray(TStruct(
        ("f1", TInt32()),
        ("f2", TString()),
        ("f3", TInt32())))),
      "a2" -> (17, TArray(TString())),
      "nullarr" -> (18, TArray(TInt32())),
      "nullset" -> (19, TSet(TInt32())),
      "emptyarr" -> (20, TArray(TInt32())),
      "emptyset" -> (21, TSet(TInt32())),
      "calls" -> (22, TStruct(("noCall", TCall()),
        ("homRef", TCall()),
        ("het", TCall()),
        ("homVar", TCall())
      )))

    val ec = EvalContext(symTab)

    val a = ec.a
    a(0) = 5 // i
    a(1) = -7 // j
    a(2) = 3.14
    a(3) = 5.79e7
    a(4) = "12,34,56,78"
    a(5) = "this is a String, there are many like it, but this one is mine"
    a(6) = IndexedSeq(1, 2, null, 6, 3, 3, -1, 8)
    a(7) = null // m
    a(8) = Array[Any](Annotation(23, "foo"), null): IndexedSeq[Any]
    a(9) = Annotation(null, Call2(0, 0), Call2(0, 1), Call2(1, 1), Call2(3, 5))
    a(10) = true
    a(11) = false
    a(12) = null // mb
    a(13) = "-37" // is
    a(14) = Set(0, 1, 2)
    a(15) = Map("gene1" -> 2, "gene2" -> 10, "gene3" -> 14)
    a(16) = IndexedSeq(Annotation(1, "A", 2),
      Annotation(5, "B", 6),
      Annotation(10, "C", 10))
    a(17) = IndexedSeq("a", "d", null, "c", "e", null, "d", "c")
    a(18) = null
    a(19) = null
    a(20) = IndexedSeq[Int]()
    a(21) = Set[Int]()
    a(22) = Annotation(null, Call2(0, 0), Call2(0, 1), Call2(1, 1))

    assert(a.length == symTab.size)

    val rdd = sc.parallelize(Array(0), 1)

    val bindings = symTab.toSeq
      .sortBy { case (name, (i, _)) => i }
      .map { case (name, (_, typ)) => (name, typ) }
      .zip(a)
      .map { case ((name, typ), value) => (name, typ, value.asInstanceOf[AnyRef]) }

    def eval[T](s: String): Option[T] = {
      val compiledCode = Parser.parseToAST(s, ec).compile().run(bindings, ec)
      val compileResult = Option(compiledCode().asInstanceOf[T])
      rdd.map(_ => compileResult).collect().head // force serialization
    }

    def evalWithType[T](s: String): (Type, Option[T]) = {
      val (t, f) = Parser.parseExpr(s, ec)
      (t, Option(f()).map(_.asInstanceOf[T]))
    }

    assert(D_==(eval[Double]("gamma(5.0)").get, 24))
    assert(D_==(eval[Double]("gamma(0.5)").get, 1.7724538509055159)) // python: math.gamma(0.5)

    // uniroot (default) tolerance is ~1.22e-4
    assert(D_==(eval[Double]("uniroot(x => x*x + 3.0*x - 4.0, 0.0, 2.0)").get, 1, tolerance = 1e-4))
    assert(D_==(eval[Double]("uniroot(x => x*x + 3.0*x - 4.0, -5.0, -1.0)").get, -4, tolerance = 1e-4))

    assert(eval[Int]("is.toInt32()").contains(-37))

    assert(eval[Boolean]("!true").contains(false))
    assert(eval[Boolean]("!isMissing(i)").contains(true))

    assert(eval[Boolean]("-j").contains(7))

    assert(eval[Boolean]("gs.het.isHomRef()").contains(false))
    assert(eval[Boolean]("!gs.het.isHomRef()").contains(true))

    assert(eval[Boolean]("1.0 / 2.0 == 0.5").contains(true))

    assert(eval[Boolean]("0 % 1 == 0").contains(true))
    assert(eval[Boolean]("0 % -1 == 0").contains(true))
    assert(eval[Boolean]("7 % 3 == 1").contains(true))
    assert(eval[Boolean]("-7 % 3 == 2").contains(true))
    assert(eval[Boolean]("7 % -3 == -2").contains(true))
    assert(eval[Boolean]("-7 % -3 == -1").contains(true))
    assert(eval[Boolean]("-6 % 3 == 0").contains(true))
    assert(eval[Boolean]("6 % -3 == 0").contains(true))
    assert(eval[Boolean]("-6 % -3 == 0").contains(true))

    assert(eval[Boolean]("1.0 % 2.0 == 1.0").contains(true))
    assert(eval[Boolean]("-1.0 % 2.0 == 1.0").contains(true))
    assert(eval[Boolean]("1.0 % -2.0 == -1.0").contains(true))
    assert(eval[Boolean]("-1.0 % -2.0 == -1.0").contains(true))

    assert(eval[Boolean]("2.0 % 1.0 == 0.0").contains(true))
    assert(eval[Boolean]("-2.0 % 1.0 == 0.0").contains(true))
    assert(eval[Boolean]("2.0 % -1.0 == 0.0").contains(true))
    assert(eval[Boolean]("-2.0 % -1.0 == 0.0").contains(true))

    assert(eval[Boolean]("0 // 1 == 0").contains(true))
    assert(eval[Boolean]("0 // -1 == 0").contains(true))
    assert(eval[Boolean]("7 // 2 == 3").contains(true))
    assert(eval[Boolean]("-7 // -2 == 3").contains(true))
    assert(eval[Boolean]("-7 // 2 == -4").contains(true))
    assert(eval[Boolean]("7 // -2 == -4").contains(true))
    assert(eval[Boolean]("-6 // 2 == -3").contains(true))
    assert(eval[Boolean]("6 // -2 == -3").contains(true))

    assert(eval[Boolean]("1.0 // 2.0 == 0.0").contains(true))
    assert(eval[Boolean]("-1.0 // 2.0 == -1.0").contains(true))
    assert(eval[Boolean]("1.0 // -2.0 == -1.0").contains(true))
    assert(eval[Boolean]("-1.0 // -2.0 == 0.0").contains(true))

    assert(eval[Float]("0 / 0").forall(_.isNaN))
    assert(eval[Double]("0.0 / 0.0").forall(_.isNaN))
    assert(eval[Float]("0 / 0 + 1f").forall(_.isNaN))
    assert(eval[Double]("0.0 / 0.0 + 1d").forall(_.isNaN))
    assert(eval[Float]("0 / 0 * 1f").forall(_.isNaN))
    assert(eval[Double]("0.0 / 0.0 * 1d").forall(_.isNaN))
    assert(eval[Float]("1 / 0").contains(Float.PositiveInfinity))
    assert(eval[Double]("1.0 / 0.0").contains(Double.PositiveInfinity))
    assert(eval[Float]("-1 / 0").contains(Float.NegativeInfinity))
    assert(eval[Double]("-1.0 / 0.0").contains(Double.NegativeInfinity))
    // NB: the -0 is parsed as the zero integer, which is converted to +0.0
    assert(eval[Float]("1 / -0").contains(Float.PositiveInfinity))
    assert(eval[Double]("1.0 / -0.0").contains(Double.NegativeInfinity))
    assert(eval[Float]("(0/0) * (1/0)").forall(_.isNaN))
    assert(eval[Double]("(0.0/0.0) * (1.0/0.0)").forall(_.isNaN))
    for {x <- Array("-1.0/0.0", "-1.0", "0.0", "1.0", "1.0/0.0")} {
      assert(eval[Boolean](s"0.0/0.0 < $x").contains(false))
      assert(eval[Boolean](s"0.0/0.0 <= $x").contains(false))
      assert(eval[Boolean](s"0.0/0.0 > $x").contains(false))
      assert(eval[Boolean](s"0.0/0.0 >= $x").contains(false))
      assert(eval[Boolean](s"0.0/0.0 == $x").contains(false))
      assert(eval[Boolean](s"0.0/0.0 != $x").contains(true))
    }

    assert(eval[Boolean]("isMissing(gs.noCall)").contains(true))
    assert(eval[Boolean]("gs.noCall.ploidy").isEmpty)
    assert(eval[Boolean]("gs.noCall[0]").isEmpty)

    assert(eval[Boolean]("isMissing(gs.noCall[1])").contains(true))
    assert(eval[Boolean]("gs.noCall[1]").isEmpty)

    assert(eval[Int]("let a = i and b = j in a + b").contains(-2))
    assert(eval[Int]("let a = i and b = a + j in b").contains(-2))
    assert(eval[Int]("let i = j in i").contains(-7))
    assert(eval[Int]("let a = let b = j in b + 1 in a + 1").contains(-5))

    assert(eval[Boolean]("mb || true").contains(true))
    assert(eval[Boolean]("true || mb").contains(true))
    assert(eval[Boolean]("isMissing(false || mb)").contains(true))
    assert(eval[Boolean]("isMissing(mb || false)").contains(true))

    assert(eval[Int]("gs.homRef[0]").contains(0)
      && eval[Int]("gs.homRef[1]").contains(0))
    assert(eval[Int]("gs.het[0]").contains(0)
      && eval[Int]("gs.het[1]").contains(1))
    assert(eval[Int]("gs.homVar[0]").contains(1)
      && eval[Int]("gs.homVar[1]").contains(1))
    assert(eval[Int]("gs.hetNonRef35[0]").contains(3)
      && eval[Int]("gs.hetNonRef35[1]").contains(5))

    assert(eval[Boolean]("isMissing(i)").contains(false))
    assert(eval[Boolean]("isDefined(i)").contains(true))
    assert(eval[Boolean]("isDefined(i)").contains(true))
    assert(eval[Boolean]("i").nonEmpty)

    assert(eval[Boolean]("isMissing(m)").contains(true))
    assert(eval[Boolean]("isDefined(m)").contains(false))
    assert(eval[Boolean]("m").isEmpty)

    assert(eval[Boolean]("isMissing(a[1])").contains(false))
    assert(eval[Boolean]("isDefined(a[1])").contains(true))
    assert(eval[Boolean]("a[1]").nonEmpty)

    assert(eval[Boolean]("isMissing(a[2])").contains(true))
    assert(eval[Boolean]("isDefined(a[2])").contains(false))
    assert(eval[Boolean]("a[2]").isEmpty)

    assert(eval[Int]("a[0]").contains(1))
    assert(eval[Int]("a[1]").contains(2))
    assert(eval[Int]("a[2]").isEmpty)
    assert(eval[Int]("a[3]").contains(6))
    assert(eval[Int]("a[-1]").contains(8))
    assert(eval[Int]("a[-2]").contains(-1))
    for (i <- 0 until 8)
      assert(eval[Int](s"a[${ i - 8 }]") == eval[Int](s"a[$i]"))

    assert(eval[String]("s[0]").contains("1"))
    assert(eval[String]("s[1]").contains("2"))
    assert(eval[String]("s[2]").contains(","))
    assert(eval[String]("s[3]").contains("3"))
    assert(eval[String]("s[-1]").contains("8"))
    assert(eval[String]("s[-2]").contains("7"))
    for (i <- 0 until 11)
      assert(eval[String](s"s[${ i - 11 }]") == eval[String](s"s[$i]"))

    assert(eval[Boolean]("1 == 1.0").contains(true))

    assert(eval[Int]("as.length()").contains(2))
    assert(eval[Int]("as[0].a").contains(23))
    assert(eval[Boolean]("isMissing(as[1].b)").contains(true))
    assert(eval[Boolean]("as[1].b").isEmpty)

    assert(eval[Int]("i").contains(5))
    assert(eval[Int]("j").contains(-7))
    assert(eval[Int]("i.max(j)").contains(5))
    assert(eval[Int]("i.min(j)").contains(-7))
    assert(eval[Double]("d").exists(D_==(_, 3.14)))
    assert(eval[IndexedSeq[String]]("""s.split(",")""").contains(IndexedSeq("12", "34", "56", "78")))
    assert(eval[Int]("s2.length()").contains(62))

    assert(eval[Int]("""a.find(x => x < 0)""").contains(-1))

    assert(eval[IndexedSeq[_]]("""a.flatMap(x => [x])""").contains(IndexedSeq(1, 2, null, 6, 3, 3, -1, 8)))
    assert(eval[IndexedSeq[_]]("""a.flatMap(x => [x, x + 1])""").contains(IndexedSeq(1, 2, 2, 3, null, null, 6, 7, 3, 4, 3, 4, -1, 0, 8, 9)))

    assert(eval[IndexedSeq[_]]("""nullarr.flatMap(x => [x])""").isEmpty)
    assert(eval[IndexedSeq[_]]("""emptyarr.flatMap(x => [x])""").contains(IndexedSeq[Int]()))
    assert(eval[IndexedSeq[_]]("""emptyarr.flatMap(x => nullarr)""").contains(IndexedSeq[Int]()))
    assert(eval[IndexedSeq[_]]("""a.flatMap(x => nullarr)""").isEmpty)
    assert(eval[IndexedSeq[_]]("""[nullarr, [1], [2]].flatMap(x => x)""").isEmpty)
    assert(eval[IndexedSeq[_]]("""[[0], nullarr, [2]].flatMap(x => x)""").isEmpty)
    assert(eval[IndexedSeq[_]]("""[[0], [1], nullarr].flatMap(x => x)""").isEmpty)
    assert(eval[IndexedSeq[_]]("""a.append(5)""").contains(IndexedSeq(1, 2, null, 6, 3, 3, -1, 8, 5)))
    assert(eval[IndexedSeq[_]]("""a.extend([5, -3, 0])""").contains(IndexedSeq(1, 2, null, 6, 3, 3, -1, 8, 5, -3, 0)))

    assert(eval[Set[_]]("""iset.flatMap(x => [x].toSet())""").contains(Set(0, 1, 2)))
    assert(eval[Set[_]]("""iset.flatMap(x => [x, x + 1].toSet())""").contains(Set(0, 1, 2, 3)))
    assert(eval[Set[_]]("""iset.add(3)""").contains(Set(0, 1, 2, 3)))
    assert(eval[Set[_]]("""iset.remove(3)""").contains(Set(0, 1, 2)))
    assert(eval[Set[_]]("""iset.remove(0)""").contains(Set(1, 2)))
    assert(eval[Set[_]]("""iset.add(3).remove(0)""").contains(Set(1, 2, 3)))
    assert(eval[Set[_]]("""iset.union([2,3,4].toSet)""").contains(Set(0, 1, 2, 3, 4)))
    assert(eval[Set[_]]("""iset.intersection([2,3,4].toSet)""").contains(Set(2)))
    assert(eval[Set[_]]("""iset.difference([2,3,4].toSet)""").contains(Set(0, 1)))
    assert(eval[Boolean]("""iset.isSubset([2,3,4].toSet)""").contains(false))
    assert(eval[Boolean]("""iset.isSubset([0,1].toSet)""").contains(false))
    assert(eval[Boolean]("""iset.isSubset([0,1,2,3,4].toSet)""").contains(true))


    assert(eval[Set[_]]("""nullset.flatMap(x => [x].toSet())""").isEmpty)
    assert(eval[Set[_]]("""emptyset.flatMap(x => [x].toSet())""").contains(Set[Int]()))
    assert(eval[Set[_]]("""emptyset.flatMap(x => nullset)""").contains(Set[Int]()))
    assert(eval[Set[_]]("""iset.flatMap(x => nullset)""").isEmpty)
    assert(eval[Set[_]]("""[nullset, [1].toSet(), [2].toSet()].toSet().flatMap(x => x)""").isEmpty)
    assert(eval[Set[_]]("""[[0].toSet(), nullset, [2].toSet()].toSet().flatMap(x => x)""").isEmpty)
    assert(eval[Set[_]]("""[[0].toSet(), [1].toSet(), nullset].toSet().flatMap(x => x)""").isEmpty)

    assert(eval[Set[_]]("""[[0].toSet(), [1].toSet(), nullset].filter(s => isDefined(s)).toSet().flatMap(x => x)""").contains(Set(0, 1)))

    TestUtils.interceptFatal("""No function found.*flatMap""")(
      eval[Set[_]]("""iset.flatMap(0)"""))

    TestUtils.interceptFatal("""No function found.*flatMap""")(
      eval[Set[_]]("""iset.flatMap(x => x)"""))

    TestUtils.interceptFatal("""No function found.*flatMap""")(
      eval[Set[_]]("""iset.flatMap(x => [x])"""))

    TestUtils.interceptFatal("""No function found.*flatMap""")(
      eval[IndexedSeq[_]]("""a.flatMap(x => [x].toSet())"""))

    assert(eval[IndexedSeq[_]](""" [[1], [2, 3], [4, 5, 6]].flatten() """).contains(IndexedSeq(1, 2, 3, 4, 5, 6)))
    assert(eval[IndexedSeq[_]](""" [a, [1]].flatten() """).contains(IndexedSeq(1, 2, null, 6, 3, 3, -1, 8, 1)))

    assert(eval[Set[_]](""" [[1], [2]].flatten().toSet() """).contains(Set(1, 2)))

    assert(eval[IndexedSeq[_]](""" [nullarr].flatten() """).isEmpty)
    assert(eval[IndexedSeq[_]](""" [[0], nullarr].flatten() """).isEmpty)
    assert(eval[IndexedSeq[_]](""" [nullarr, [1]].flatten() """).isEmpty)

    assert(eval[IndexedSeq[_]](""" [[1], nullarr, [2, 3]].filter(a => isDefined(a)).flatten() """).contains(IndexedSeq(1, 2, 3)))

    assert(eval[Set[_]](""" [iset, [2, 3, 4].toSet()].toSet().flatten() """).contains(Set(0, 1, 2, 3, 4)))

    assert(eval[Set[_]](""" [nullset].toSet().flatten() """).isEmpty)
    assert(eval[Set[_]](""" [[0].toSet(), nullset].toSet().flatten() """).isEmpty)
    assert(eval[Set[_]](""" [nullset, [1].toSet()].toSet().flatten() """).isEmpty)

    TestUtils.interceptFatal("""No function found.*flatten""")(
      eval[Set[_]](""" [iset, [2, 3, 4].toSet()].flatten() """))

    TestUtils.interceptFatal("""No function found.*flatten""")(
      eval[Set[_]](""" [[1], [2, 3, 4]].toSet().flatten() """))

    TestUtils.interceptFatal("""No function found.*flatten""")(
      eval[Set[_]](""" [0].flatten() """))

    TestUtils.interceptFatal("""No function found.*flatten""")(
      eval[Set[_]](""" [[0]].flatten(0) """))


    assert(eval[IndexedSeq[_]]("""a.sort()""").contains(IndexedSeq(-1, 1, 2, 3, 3, 6, 8, null)))
    assert(eval[IndexedSeq[_]]("""a.sort(true)""").contains(IndexedSeq(-1, 1, 2, 3, 3, 6, 8, null)))
    assert(eval[IndexedSeq[_]]("""a.sort(false)""").contains(IndexedSeq(8, 6, 3, 3, 2, 1, -1, null)))

    assert(eval[IndexedSeq[_]]("""a2.sort()""").contains(IndexedSeq("a", "c", "c", "d", "d", "e", null, null)))
    assert(eval[IndexedSeq[_]]("""a2.sort(true)""").contains(IndexedSeq("a", "c", "c", "d", "d", "e", null, null)))
    assert(eval[IndexedSeq[_]]("""a2.sort(false)""").contains(IndexedSeq("e", "d", "d", "c", "c", "a", null, null)))

    assert(eval[IndexedSeq[_]]("""a.sortBy(x => x)""").contains(IndexedSeq(-1, 1, 2, 3, 3, 6, 8, null)))
    assert(eval[IndexedSeq[_]]("""a.sortBy(x => -x)""").contains(IndexedSeq(8, 6, 3, 3, 2, 1, -1, null)))
    eval[IndexedSeq[_]]("""a.sortBy(x => (x - 2) * (x + 1))""") should (be(Some(IndexedSeq(1, -1, 2, 3, 3, 6, 8, null))) or be(Some(IndexedSeq(1, 2, -1, 3, 3, 6, 8, null))))

    assert(eval[IndexedSeq[_]]("""a.sortBy(x => x, true)""").contains(IndexedSeq(-1, 1, 2, 3, 3, 6, 8, null)))
    assert(eval[IndexedSeq[_]]("""a.sortBy(x => x, false)""").contains(IndexedSeq(8, 6, 3, 3, 2, 1, -1, null)))

    assert(eval[IndexedSeq[_]]("""a2.sortBy(x => x)""").contains(IndexedSeq("a", "c", "c", "d", "d", "e", null, null)))
    assert(eval[IndexedSeq[_]]("""a2.sortBy(x => x, true)""").contains(IndexedSeq("a", "c", "c", "d", "d", "e", null, null)))
    assert(eval[IndexedSeq[_]]("""a2.sortBy(x => x, false)""").contains(IndexedSeq("e", "d", "d", "c", "c", "a", null, null)))

    assert(eval[String](""" "HELLO=" + j + ", asdasd" + 9""")
      .contains("HELLO=-7, asdasd9"))

    assert(eval[IndexedSeq[_]](""" a.filter(x => x < 4)   """)
      .contains(IndexedSeq(1, 2, 3, 3, -1)))

    assert(eval[IndexedSeq[_]](""" a.filter(x => x < 4).map(x => x * 100)   """)
      .contains(IndexedSeq(1, 2, 3, 3, -1).map(_ * 100)))

    assert(eval[Boolean](""" a.filter(x => x < 4).map(x => x * 100).exists(x => x == -100)   """)
      .contains(true))

    assert(eval[Int]("""a.min()""").contains(-1))
    assert(eval[Int]("""a.max()""").contains(8))
    assert(eval[Int]("""a.median()""").contains(3))

    assert(eval[Int]("""emptyarr.sum()""").contains(0))
    assert(eval[Int]("""emptyarr.mean()""").isEmpty)
    assert(eval[Int]("""emptyarr.min()""").isEmpty)
    assert(eval[Int]("""emptyarr.max()""").isEmpty)
    assert(eval[Int]("""emptyarr.median()""").isEmpty)

    assert(eval[Int]("""emptyset.sum()""").contains(0))
    assert(eval[Int]("""emptyset.mean()""").isEmpty)
    assert(eval[Int]("""emptyset.min()""").isEmpty)
    assert(eval[Int]("""emptyset.max()""").isEmpty)
    assert(eval[Int]("""emptyset.median()""").isEmpty)

    assert(eval[Double]("""a.mean()""").contains(22 / 7.0))
    assert(eval[Int]("""a.sum()""").contains(IndexedSeq(1, 2, 6, 3, 3, -1, 8).sum))
    assert(eval[String]("""str(i)""").contains("5"))
    assert(eval[String]("""let l = Locus("1", 1000) in json(l)""").contains("""{"contig":"1","position":1000}"""))
    assert(eval[String](""" "" + 5 + "5" """) == eval[String](""" "5" + 5 """))
    assert(eval[Int]("""iset.min()""").contains(0))
    assert(eval[Int]("""iset.max()""").contains(2))
    assert(eval[Int]("""iset.sum()""").contains(3))

    assert(eval[String](""" "\t\t\t" """).contains("\t\t\t"))
    assert(eval[String](""" "\"\"\"" """).contains("\"\"\""))
    assert(eval[String](""" "```" """).contains("```"))

    assert(eval[String](""" "\t" """) == eval[String]("\"\t\""))

    assert(eval[String](""" "a b c d".replace(" ", "_") """).contains("a_b_c_d"))
    assert(eval[String](" \"a\\tb\".replace(\"\\t\", \"_\") ").contains("a_b"))
    assert(eval[String](""" "a    b  c    d".replace("\\s+", "_") """).contains("a_b_c_d"))

    // quoting '` optional in strings
    assert(eval[String](""" "\"\'\`" """).contains(""""'`"""))
    assert(eval[String](""" "\"'`" """).contains(""""'`"""))

    // quoting "' optional in literal identifiers
    assert(eval[Int]("""let `\"\'\`` = 5 in `"'\``""").contains(5))

    assert(eval[String]("""NA: String""").isEmpty)
    assert(eval[String]("""NA: Int""").isEmpty)
    assert(eval[String]("""NA: Array[Int]""").isEmpty)

    assert(eval[IndexedSeq[Any]]("""[1, 2, 3, 4]""").contains(IndexedSeq(1, 2, 3, 4)))
    assert(eval[IndexedSeq[Any]]("""[1, 2, NA:Int, 6, 3, 3, -1, 8]""").contains(IndexedSeq(1, 2, null, 6, 3, 3, -1, 8)))


    assert(eval[IndexedSeq[Any]]("""[1, 2, 3.0, 4]""").contains(IndexedSeq(1, 2, 3.0, 4)))
    assert(eval[Double]("""[1, 2, 3.0, 4].max()""").contains(4.0))

    intercept[HailException](eval[IndexedSeq[Any]]("""[1,2, "hello"] """))
    intercept[HailException](eval[IndexedSeq[Any]]("""[] """))

    val (t, r) = evalWithType[Annotation](""" {field1: 1, field2: 2 } """)
    assert(r.contains(Annotation(1, 2)))
    assert(t == TStruct(("field1", TInt32()), ("field2", TInt32())))

    val (t2, r2) = evalWithType[Annotation](""" {field1: 1, asdasd: "Hello" } """)
    assert(r2.contains(Annotation(1, "Hello")))
    assert(t2 == TStruct(("field1", TInt32()), ("asdasd", TString())))

    assert(eval[IndexedSeq[_]](""" [0,1,2,3][0:2] """).contains(IndexedSeq(0, 1)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][2:100] """).contains(IndexedSeq(2, 3)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][2:] """).contains(IndexedSeq(2, 3)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][:2] """).contains(IndexedSeq(0, 1)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][:] """).contains(IndexedSeq(0, 1, 2, 3)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][-3:] """).contains(IndexedSeq(1, 2, 3)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][:-1] """).contains(IndexedSeq(0, 1, 2)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][0:-3] """).contains(IndexedSeq(0)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][-4:-2] """).contains(IndexedSeq(0, 1)))
    assert(eval[IndexedSeq[_]](""" [0,1,2,3][-2:-4] """).contains(IndexedSeq()))

    forAll(Gen.choose(1, 4), Gen.choose(1, 4)) { (i: Int, j: Int) =>
      eval[IndexedSeq[Int]](s""" [0,1,2,3][-$i:-$j] """) == eval[IndexedSeq[Int]](s""" [0,1,2,3][4-$i:4-$j] """)
    }.check()

    forAll(Gen.choose(5, 10), Gen.choose(5, 10)) { (i: Int, j: Int) =>
      eval[IndexedSeq[Int]](s""" [0,1,2,3][-$i:-$j] """).contains(IndexedSeq[Int]())
    }.check()

    forAll(Gen.choose(-5, 5)) { (i: Int) =>
      eval[IndexedSeq[Int]](s""" [0,1,2,3][$i:] """) == eval[IndexedSeq[Int]](s""" [0,1,2,3][$i:4] """)
    }.check()

    forAll(Gen.choose(-5, 5)) { (i: Int) =>
      eval[IndexedSeq[Int]](s""" [0,1,2,3][:$i] """) == eval[IndexedSeq[Int]](s""" [0,1,2,3][0:$i] """)
    }.check()

    assert(eval[String](""" "abcd"[0:2] """).contains("ab"))
    assert(eval[String](""" "abcd"[2:100] """).contains("cd"))
    assert(eval[String](""" "abcd"[2:] """).contains("cd"))
    assert(eval[String](""" "abcd"[:2] """).contains("ab"))
    assert(eval[String](""" "abcd"[:] """).contains("abcd"))
    assert(eval[String](""" ""[:] """).contains(""))
    assert(eval[String](""" "abcd"[-2:100] """).contains("cd"))
    assert(eval[String](""" "abcd"[-2:0] """).contains(""))
    assert(eval[String](""" "abcd"[0:100] """).contains("abcd"))
    assert(eval[String](""" "abcd"[4:] """).contains(""))
    assert(eval[String](""" "abcd"[3:] """).contains("d"))
    assert(eval[String](""" "abcd"[-4:-2] """).contains("ab"))
    assert(eval[String](""" "abcd"[-2:-4] """).contains(""))

    forAll(Gen.choose(1, 4), Gen.choose(1, 4)) { (i: Int, j: Int) =>
      eval[String](s""" "abcd"[-$i:-$j] """) == eval[String](s""" "abcd"[4-$i:4-$j] """)
    }.check()

    forAll(Gen.choose(5, 10), Gen.choose(5, 10)) { (i: Int, j: Int) =>
      eval[String](s""" "abcd"[-$i:-$j] """).contains("")
    }.check()

    forAll(Gen.choose(-5, 5)) { (i: Int) =>
      eval[String](s""" "abcd"[$i:] """) == eval[String](s""" "abcd"[$i:4] """)
    }.check()

    forAll(Gen.choose(-5, 5)) { (i: Int) =>
      eval[String](s""" "abcd"[:$i] """) == eval[String](s""" "abcd"[0:$i] """)
    }.check()

    assert(eval[Int](""" genedict["gene2"] """).contains(10))

    assert(eval[Map[_, _]](""" genedict.mapValues(x => x + 1) """).contains(Map("gene1" -> 3, "gene2" -> 11, "gene3" -> 15)))

    assert(eval[IndexedSeq[Int]]("""genedict.values""").contains(IndexedSeq(2, 10, 14)))
    assert(eval[IndexedSeq[String]]("""genedict.keys""").contains(IndexedSeq("gene1", "gene2", "gene3")))
    val ks = eval[Set[String]]("""genedict.keySet""")
    assert(ks.isDefined)
    ks.get should contain theSameElementsAs Seq("gene1", "gene2", "gene3")

    // caused exponential blowup previously
    assert(eval[Boolean](
      """
        |if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else if (false) false
        |else true
      """.stripMargin).contains(true))

    val ifConditionNotBooleanMessage = "condition must have type Boolean"
    TestUtils.interceptFatal(ifConditionNotBooleanMessage)(
      eval[Int]("""if (1) 1 else 0"""))
    TestUtils.interceptFatal(ifConditionNotBooleanMessage)(
      eval[Int]("""if ("a") 1 else 0"""))
    TestUtils.interceptFatal(ifConditionNotBooleanMessage)(
      eval[Int]("""if (0.1) 1 else 0"""))
    TestUtils.interceptFatal(ifConditionNotBooleanMessage)(
      eval[Int]("""if ([1]) 1 else 0"""))

    assert(eval[Annotation](""" select({a:1,b:2}, a) """).contains(Annotation(1)))
    assert(eval[Boolean](""" let x = {a:1, b:2, c:3, `\tweird\t`: 4} in select(x, a,b,`\tweird\t`) == drop(x, c) """).contains(true))

    assert(eval[Locus]("""Locus("1", 1)""").contains(Locus("1", 1)))
    assert(eval[Locus]("""Locus("1:1")""").contains(Locus("1", 1)))
    assert(eval[Boolean]("""let l = Locus("1", 1) in Locus(str(l)) == l""").contains(true))

    implicit val locusOrd = ReferenceGenome.defaultReference.locusOrdering
    assert(eval[Interval]("""Interval(Locus("1", 1), Locus("2", 2), true, false)""").contains(Interval(Locus("1", 1), Locus("2", 2), true, false)))
    assert(eval[Locus]("""Interval(Locus("1", 1), Locus("2", 2), true, false).start""").contains(Locus("1", 1)))
    assert(eval[Locus]("""Interval(Locus("1", 1), Locus("2", 2), true, false).end""").contains(Locus("2", 2)))
    assert(eval[Boolean]("""Interval(Locus("1", 1), Locus("1", 3), true, false).contains(Locus("1", 2))""").contains(true))
    assert(eval[Boolean]("""Interval(Locus("1", 1), Locus("1", 3), true, false).contains(Locus("2", 2))""").contains(false))

    assert(eval[Interval]("""LocusInterval(GRCh37)("1", 3, 5, true, false)""").contains(Interval(Locus("1", 3), Locus("1", 5), true, false)))
    assert(eval[Interval]("""LocusInterval(GRCh37)("[1:3-5]")""").contains(Interval(Locus("1", 3), Locus("1", 5), true, true)))

    assert(eval[Boolean]("Interval(-2, 9, true, false).contains(-3)").contains(false))
    assert(eval[Boolean]("Interval(-2, 9, true, false).contains(-2)").contains(true))
    assert(eval[Boolean]("Interval(-2, 9, true, false).contains(5)").contains(true))
    assert(eval[Boolean]("Interval(-2, 9, true, false).contains(9)").contains(false))

    // FIXME catch parse errors
    assert(eval(""" "\``\''" """) == eval(""" "``''" """))
    TestUtils.interceptFatal("""invalid escape character.*string.*\\a""")(eval[String](""" "this is bad \a" """))
    TestUtils.interceptFatal("""unterminated string literal""")(eval[String](""" "unclosed string \" """))
    TestUtils.interceptFatal("""invalid escape character.*backtick identifier.*\\i""")(eval[String](""" let `bad\identifier` = 0 in 0 """))
    TestUtils.interceptFatal("""unterminated backtick identifier""")(eval[String](""" let `bad\identifier = 0 in 0 """))

    assert(D_==(eval[Double]("log(56.toFloat64)").get, math.log(56)))
    assert(D_==(eval[Double]("exp(5.6)").get, math.exp(5.6)))
    assert(D_==(eval[Double]("log10(5.6)").get, math.log10(5.6)))
    assert(D_==(eval[Double]("log10(5.6)").get, eval[Double]("log(5.6, 10.0)").get))
    assert(D_==(eval[Double]("log(5.6, 3.2)").get, 1.481120576298196))
    assert(D_==(eval[Double]("sqrt(5.6)").get, math.sqrt(5.6)))

    assert(eval[IndexedSeq[_]]("""[1,2,3] + [2,3,4] """).contains(IndexedSeq(3, 5, 7)))
    assert(eval[IndexedSeq[_]]("""[1,2,3] - [2,3,4] """).contains(IndexedSeq(-1, -1, -1)))
    assert(eval[IndexedSeq[_]]("""[1,2,3] / [2,3,4] """).contains(IndexedSeq(.5, 2f / 3f, .75)))
    assert(eval[IndexedSeq[_]]("""1 / [2,3,4] """).contains(IndexedSeq(1f / 2f, 1f / 3f, 1.0 / 4.0)))
    assert(eval[IndexedSeq[_]]("""[2,3,4] / 2""").contains(IndexedSeq(1.0, 1.5, 2.0)))
    assert(eval[IndexedSeq[_]]("""[2.0,3.0,4.0] * 2.0""").contains(IndexedSeq(4.0, 6.0, 8.0)))
    assert(eval[IndexedSeq[_]]("""[2.0,NA: Float64,4.0] * 2.0""").contains(IndexedSeq(4.0, null, 8.0)))
    assert(eval[IndexedSeq[_]]("""[2,NA: Int,4] * [2,NA: Int,4]""").contains(IndexedSeq(4.0, null, 16.0)))
    assert(eval[IndexedSeq[_]]("""[2.0,NA: Float64,4.0] + 2.0""").contains(IndexedSeq(4.0, null, 6.0)))
    assert(eval[IndexedSeq[_]]("""[2,NA: Int,4] + [2,NA: Int,4]""").contains(IndexedSeq(4.0, null, 8.0)))
    assert(eval[IndexedSeq[_]]("""[2.0,NA: Float64,4.0] - 2.0""").contains(IndexedSeq(0.0, null, 2.0)))
    assert(eval[IndexedSeq[_]]("""[2,NA: Int,4] - [2,NA: Int,4]""").contains(IndexedSeq(0.0, null, 0.0)))
    assert(eval[IndexedSeq[_]]("""[2.0,NA: Float64,4.0] / 2.0""").contains(IndexedSeq(1.0, null, 2.0)))
    assert(eval[IndexedSeq[_]]("""[2,NA: Int,4] / [2,NA: Int,4]""").contains(IndexedSeq(1.0, null, 1.0)))

    // tests for issue #1204
    assert(eval[IndexedSeq[_]]("""([2,3,4] / 2) / 2f""").contains(IndexedSeq(0.5, 0.75, 1.0)))
    assert(eval[IndexedSeq[_]]("""([2L,3L,4L] / 2L) / 2f""").contains(IndexedSeq(0.5, 0.75, 1.0)))
    assert(eval[IndexedSeq[_]]("""([2.0,3.0,4.0] / 2.0) / 2.0""").contains(IndexedSeq(0.5, 0.75, 1.0)))

    TestUtils.interceptFatal("""Cannot apply operation \+ to arrays of unequal length""") {
      eval[IndexedSeq[Int]]("""[1] + [2,3,4] """)
    }

    interceptFatal("No function found") {
      eval[Double](""" log(Locus("22", 123)) """)
    }

    assert(eval[Int]("[0, 0.toInt64()][0].toInt32()").contains(0))
    assert(eval[Int]("[0, 0.toFloat32()][0].toInt32()").contains(0))
    assert(eval[Int]("[0, 0.toFloat64()][0].toInt32()").contains(0))
    assert(eval[Boolean]("[NA:Int] == [0]").contains(false))
    assert(eval[Boolean]("[NA:Int64] == [0.toInt64()]").contains(false))
    assert(eval[Boolean]("[NA:Float32] == [0.toFloat32()]").contains(false))
    assert(eval[Boolean]("[NA:Float] == [0.toFloat64()]").contains(false))

    assert(eval[IndexedSeq[Int]]("range(5)").contains(IndexedSeq(0, 1, 2, 3, 4)))
    assert(eval[IndexedSeq[Int]]("range(1)").contains(IndexedSeq(0)))
    assert(eval[IndexedSeq[Int]]("range(10, 14)").contains(IndexedSeq(10, 11, 12, 13)))
    assert(eval[IndexedSeq[Int]]("range(-2, 2)").contains(IndexedSeq(-2, -1, 0, 1)))

    assert(eval[IndexedSeq[Int]]("range(0)").contains(IndexedSeq()))
    assert(eval[IndexedSeq[Int]]("range(-2)").contains(IndexedSeq()))

    assert(eval[IndexedSeq[Int]]("range(10, 10)").contains(IndexedSeq()))
    assert(eval[IndexedSeq[Int]]("range(10, 0)").contains(IndexedSeq()))

    assert(eval[Boolean]("pcoin(2.0)").contains(true))
    assert(eval[Boolean]("pcoin(-1.0)").contains(false))

    assert(eval[Boolean]("runif(2.0, 3.0) > -1.0").contains(true))
    assert(eval[Boolean]("runif(2.0, 3.0) < 3.0").contains(true))

    assert(eval[Boolean]("rnorm(2.0, 4.0).abs() > -1.0").contains(true))

    assert(D_==(eval[Double]("pnorm(qnorm(0.5))").get, 0.5))
    assert(D_==(eval[Double]("qnorm(pnorm(0.5))").get, 0.5))
    assert(D_==(eval[Double]("qnorm(pnorm(-0.5))").get, -0.5))

    assert(D_==(eval[Double]("qchisqtail(pchisqtail(0.5,1.0),1.0)").get, 0.5))
    assert(D_==(eval[Double]("pchisqtail(qchisqtail(0.5,1.0),1.0)").get, 0.5))

    assert(eval[Boolean]("rpois(5.0) >= 0d").contains(true))
    assert(eval[Boolean]("rpois(5, 5.0).length == 5").contains(true))
    assert(D_==(eval[Double]("dpois(5.0, 5.0)").get, 0.1754674))
    assert(D_==(eval[Double]("dpois(5.0, 5.0, true)").get, -1.740302))
    assert(D_==(eval[Double]("ppois(5.0, 5.0)").get, 0.6159607))
    assert(D_==(eval[Double]("ppois(5.0, 5.0, true, true)").get, -0.4845722))
    assert(D_==(eval[Double]("ppois(5.0, 5.0, false, false)").get, 0.3840393))
    assert(D_==(eval[Int]("qpois(0.4, 5.0)").get, 4))
    assert(D_==(eval[Int]("qpois(log(0.4), 5.0, true, true)").get, 4))
    assert(D_==(eval[Int]("qpois(0.4, 5.0, false, false)").get, 5))

    assert(eval[Any]("if (true) NA: Float64 else 0.0").isEmpty)

    assert(eval[Long]("0L").contains(0L))
    assert(eval[Long]("-1L").contains(-1L))
    assert(eval[Long]("1L").contains(1L))
    assert(eval[Long]("0l").contains(0L))
    assert(eval[Long]("-1l").contains(-1L))
    assert(eval[Long]("1l").contains(1L))
    assert(eval[Long]("10000000000L").contains(10000000000L))
    assert(eval[Long]("100000L * 100000L").contains(100000L * 100000L))
    assert(eval[Long]("-10000000000L").contains(-10000000000L))
    assert(eval[Long](Long.MaxValue + "L").contains(Long.MaxValue))
    assert(eval[Long]((Long.MinValue + 1) + "L").contains(Long.MinValue + 1))
    assert(eval[Long](Long.MaxValue + "l").contains(Long.MaxValue))
    assert(eval[Long]((Long.MinValue + 1) + "l").contains(Long.MinValue + 1))
    // FIXME: parser should accept minimum Long/Int literals
    // assert(eval[Long](Long.MinValue.toString+"L").contains(Long.MinValue))
    // assert(eval[Long](Long.MinValue.toString+"l").contains(Long.MinValue))

    assert(eval[Boolean]("isMissing(calls.noCall)").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.isHomRef())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.isHet())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.isHomVar())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.isNonRef())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.isHetNonRef())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.isHetRef())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.nNonRefAlleles())").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall[0])").contains(true))
    assert(eval[Boolean]("isMissing(calls.noCall.ploidy)").contains(true))
    assert(eval[Boolean]("""let c = calls.noCall and alleles = ["A", "T"] in isMissing(c.oneHotAlleles(alleles))""").contains(true)) // FIXME

    assert(eval[Boolean]("isDefined(calls.homRef)").contains(true))
    assert(eval[Boolean]("calls.homRef.isHomRef()").contains(true))
    assert(eval[Boolean]("calls.homRef.isHet()").contains(false))
    assert(eval[Boolean]("calls.homRef.isHomVar()").contains(false))
    assert(eval[Boolean]("calls.homRef.nNonRefAlleles() == 0").contains(true))
    assert(eval[IndexedSeq[Int]]("""let c = calls.homRef and alleles = ["A", "T"] in c.oneHotAlleles(alleles)""").contains(IndexedSeq(2, 0)))

    assert(eval[Boolean]("isDefined(calls.het)").contains(true))
    assert(eval[Boolean]("calls.het.isHomRef()").contains(false))
    assert(eval[Boolean]("calls.het.isHet()").contains(true))
    assert(eval[Boolean]("calls.het.isHomVar()").contains(false))
    assert(eval[Boolean]("calls.het.nNonRefAlleles() == 1").contains(true))
    assert(eval[IndexedSeq[Int]]("""let c = calls.het and alleles = ["A", "T"] in c.oneHotAlleles(alleles)""").contains(IndexedSeq(1, 1)))

    assert(eval[Boolean]("isDefined(calls.homVar)").contains(true))
    assert(eval[Boolean]("calls.homVar.isHomRef()").contains(false))
    assert(eval[Boolean]("calls.homVar.isHet()").contains(false))
    assert(eval[Boolean]("calls.homVar.isHomVar()").contains(true))
    assert(eval[Boolean]("calls.homVar.nNonRefAlleles() == 2").contains(true))
    assert(eval[IndexedSeq[Int]]("""let c = calls.homVar and alleles = ["A", "T"] in c.oneHotAlleles(alleles)""").contains(IndexedSeq(0, 2)))

    assert(eval[Int]("let x = Call(1, 2, true) in x.ploidy").contains(2))
    assert(eval[Int]("let x = Call(1, 2, true) in x[1]").contains(2))
    assert(eval[Int]("let x = Call(3, false) in x.ploidy").contains(1))
    assert(eval[Int]("let x = Call(3, false) in x[0]").contains(3))
    assert(eval[Int]("let x = Call(false) in x.ploidy").contains(0))

    {
      val x = eval[Map[String, IndexedSeq[Int]]]("[1,2,3,4,5].groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq("even", "odd")
      x.get("even") should contain theSameElementsAs Seq(2, 4)
      x.get("odd") should contain theSameElementsAs Seq(1, 3, 5)
    }

    {
      val x = eval[Map[Int, IndexedSeq[Int]]]("[1,2,3,4,5].groupBy(k => k % 2)")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq(0, 1)
      x.get(0) should contain theSameElementsAs Seq(2, 4)
      x.get(1) should contain theSameElementsAs Seq(1, 3, 5)
    }

    {
      val x = eval[Map[Boolean, IndexedSeq[Int]]]("[1,2,3,4,5].groupBy(k => k % 2 == 0)")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq(true, false)
      x.get(true) should contain theSameElementsAs Seq(2, 4)
      x.get(false) should contain theSameElementsAs Seq(1, 3, 5)
    }

    (eval[Map[String, IndexedSeq[Int]]]("[1][:0].groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")").get
      shouldBe empty)

    (eval[Map[Int, IndexedSeq[Int]]]("[1][:0].groupBy(k => k % 2)").get
      shouldBe empty)

    (eval[Map[Boolean, IndexedSeq[Int]]]("[1][:0].groupBy(k => k % 2 == 0)").get
      shouldBe empty)

    {
      val x = eval[Map[String, IndexedSeq[Int]]]("[1].groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq("odd")
      x.get("odd") should contain theSameElementsAs Seq(1)
    }

    {
      val x = eval[Map[String, IndexedSeq[Int]]]("[2].groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq("even")
      x.get("even") should contain theSameElementsAs Seq(2)
    }

    {
      val x = eval[Map[String, IndexedSeq[Int]]]("[1,2,3,4,5].toSet().groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq("even", "odd")
      x.get("even") should contain theSameElementsAs Seq(2, 4)
      x.get("odd") should contain theSameElementsAs Seq(1, 3, 5)
    }

    (eval[Map[String, IndexedSeq[Int]]]("[1][:0].toSet().groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")").get
      shouldBe empty)

    {
      val x = eval[Map[String, IndexedSeq[Int]]]("[1].toSet().groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq("odd")
      x.get("odd") should contain theSameElementsAs Seq(1)
    }

    {
      val x = eval[Map[String, IndexedSeq[Int]]]("[2].toSet().groupBy(k => if (k % 2 == 0) \"even\" else \"odd\")")
      assert(x.isDefined)
      x.get.keySet should contain theSameElementsAs Seq("even")
      x.get("even") should contain theSameElementsAs Seq(2)
    }

    {
      val (t, r) = evalWithType("2 ** 2")
      assert(t.isOfType(TFloat64()))
      assert(r.contains(4))

      val (t2, r2) = evalWithType("2d ** 3.0")
      assert(t2.isOfType(TFloat64()))
      assert(r2.contains(8.0))

      assert(eval("5d * 2 ** 2").contains(20))
      assert(eval("-2**2").contains(-4))
      assert(eval("2**3**2d").contains(64))
      assert(eval("(-2)**2").contains(4))
      assert(eval("-2 ** -2").contains(-0.25))
    }

    assert(eval("1 == 1.0").contains(true))
    assert(eval("[1,2] == [1.0, 2.0]").contains(true))
    assert(eval("[1,2] != [1.1, 2.0]").contains(true))
    assert(eval("{a: 1, b: NA: Call} != {a: 2, b: NA: Call}").contains(true))
    assert(eval("{a: 1, b: NA: Call} == {a: 1, b: NA: Call}").contains(true))

    TestUtils.interceptFatal("Cannot compare arguments") {
      eval("1 == str(1)")
    }
    TestUtils.interceptFatal("Cannot compare arguments") {
      eval("str(1) == 1")
    }

    assert(eval("Dict([1,2,3], [1,2,3])").contains(Map(1 -> 1, 2 -> 2, 3 -> 3)))
    assert(eval("""Dict(["foo", "bar"], [1,2])""").contains(Map("foo" -> 1, "bar" -> 2)))

    assert(eval("isnan(0/0)").contains(true))
    assert(eval("isnan(0d)").contains(false))
    assert(eval("isnan(NA: Float32)").isEmpty)
  }

  @Test def testParseTypes() {
    val s1 = "SIFT_Score: Float, Age: Int"
    val s2 = ""
    val s3 = "SIFT_Score: Float, Age: Int, SIFT2: BadType"

    assert(Parser.parseAnnotationTypes(s1) == Map("SIFT_Score" -> TFloat64(), "Age" -> TInt32()))
    assert(Parser.parseAnnotationTypes(s2) == Map.empty[String, Type])
    intercept[HailException](Parser.parseAnnotationTypes(s3) == Map("SIFT_Score" -> TFloat64(), "Age" -> TInt32()))
  }

  @Test def testTypePretty() {
    // for arbType
    import is.hail.expr.types.Type._

    val sb = new StringBuilder
    check(forAll { (t: Type) =>
      sb.clear()
      t.pretty(sb, 0, compact = true)
      val res = sb.result()
      val parsed = Parser.parseType(res)
      t == parsed
    })
    check(forAll { (t: Type) =>
      sb.clear()
      t.pretty(sb, 0, compact = false)
      val res = sb.result()
      val parsed = Parser.parseType(res)
      t == parsed
    })
    check(forAll { (t: Type) =>
      val s = t.parsableString()
      val parsed = Parser.parseType(s)
      t == parsed
    })
  }

  @Test def testEscaping() {
    val p = forAll { (s: String) =>
      s == unescapeString(escapeString(s))
    }

    p.check()
  }

  @Test def testEscapingSimple() {
    // a == 0x61, _ = 0x5f
    assert(escapeStringSimple("abc", '_', _ => false) == "abc")
    assert(escapeStringSimple("abc", '_', _ == 'a') == "_61bc")
    assert(escapeStringSimple("abc_", '_', _ => false) == "abc_5f")
    assert(unescapeStringSimple("abc", '_') == "abc")
    assert(unescapeStringSimple("abc_5f", '_') == "abc_")
    assert(unescapeStringSimple("_61bc", '_') == "abc")
    assert(unescapeStringSimple("_u0061bc", '_') == "abc")
    assert(escapeStringSimple("my name is 名谦", '_', _ => false) == "my name is _u540d_u8c26")
    assert(unescapeStringSimple("my name is _u540d_u8c26", '_') == "my name is 名谦")

    val p = forAll { (s: String) =>
      s == unescapeStringSimple(escapeStringSimple(s, '_', _.isLetterOrDigit, _.isLetterOrDigit), '_')
    }

    p.check()
  }

  @Test def testImpexes() {

    val g = for {t <- Type.genArb
    a <- t.genValue} yield (t, a)

    object Spec extends Properties("ImpEx") {
      property("json") = forAll(g) { case (t, a) =>
        JSONAnnotationImpex.importAnnotation(JSONAnnotationImpex.exportAnnotation(a, t), t) == a
      }

      property("json-text") = forAll(g) { case (t, a) =>
        val string = compact(JSONAnnotationImpex.exportAnnotation(a, t))
        JSONAnnotationImpex.importAnnotation(parse(string), t) == a
      }

      property("table") = forAll(g.filter { case (t, a) => !t.isOfType(TFloat64()) && a != null }.resize(10)) { case (t, a) =>
        TableAnnotationImpex.importAnnotation(TableAnnotationImpex.exportAnnotation(a, t), t) == a
      }
    }

    Spec.check()
  }

  @Test def testIfNumericPromotion() {
    val ec = EvalContext(Map("c" -> (0, TBoolean()), "l" -> (1, TInt64()), "f" -> (2, TFloat32())))

    def eval[T](s: String): (Type, Option[T]) = {
      val (t, f) = Parser.parseExpr(s, ec)
      (t, Option(f()).map(_.asInstanceOf[T]))
    }

    assert(Parser.parseExpr("if (c) 0 else 0", ec)._1.isInstanceOf[TInt32])
    assert(Parser.parseExpr("if (c) 0 else l", ec)._1.isInstanceOf[TInt64])
    assert(Parser.parseExpr("if (c) f else 0", ec)._1.isInstanceOf[TFloat32])
    assert(Parser.parseExpr("if (c) 0 else 0.0", ec)._1.isInstanceOf[TFloat64])
    assert(eval[Int]("(if (true) 0 else 0.toInt64()).toInt32()") == (TInt32(), Some(0)))
    assert(eval[Int]("(if (true) 0 else 0.toFloat32()).toInt32()") == (TInt32(), Some(0)))
  }

  @Test def testRegistryTypeCheck() {
    val expr = """let l = Locus("1:650000") and isHet = l.isAutosomal and s = "1" in s.toInt32()"""
    val (t, f) = Parser.parseExpr(expr, EvalContext())
    assert(f().asInstanceOf[Int] == 1 && t.isInstanceOf[TInt32])
  }

  @Test def testOrdering() {
    val intOrd = TInt32().ordering

    assert(intOrd.compare(-2, -2) == 0)
    assert(intOrd.compare(null, null) == 0)
    assert(intOrd.compare(5, 7) < 0)
    assert(intOrd.compare(5, null) < 0)
    assert(intOrd.compare(null, -2) > 0)

    val g = for (t <- Type.genArb;
    a <- t.genValue;
    b <- t.genValue) yield (t, a, b)

    val p = forAll(g) { case (t, a, b) =>
      val ord = t.ordering
      ord.compare(a, b) == -ord.compare(b, a)
    }
    p.check()
  }
  
  @Test def testNumericParsing() {
    def evalWithType[T](s: String): (Type, Option[T]) = {
      val (t, f) = Parser.parseExpr(s, EvalContext())
      (t, Option(f()).map(_.asInstanceOf[T]))
    }

    assert(evalWithType("1.1e-15") == TFloat64Optional -> Some(1.1e-15))
    assert(evalWithType("1e-8") == TFloat64Optional -> Some(1e-8))
    assert(evalWithType("1.1") == TFloat64Optional -> Some(1.1))
    assert(evalWithType("1") == TInt32Optional -> Some(1))

    assert(evalWithType("f32#1.1e-5") == TFloat32Optional -> Some(1.1e-5f))
    assert(evalWithType("f32#1.1") == TFloat32Optional -> Some(1.1f))
    assert(evalWithType("f32#-1") == TFloat32Optional -> Some(-1.0f))

    assert(evalWithType("f64#1.1e-5") == TFloat64Optional -> Some(1.1e-5))
    assert(evalWithType("f64#1.1") == TFloat64Optional -> Some(1.1))
    assert(evalWithType("f64#-1") == TFloat64Optional -> Some(-1.0))

    assert(evalWithType("i32#1") == TInt32Optional -> Some(1))
    assert(evalWithType("i32#-1") == TInt32Optional -> Some(-1))
    assert(evalWithType(s"i32#${Int.MaxValue}") == TInt32Optional -> Some(Int.MaxValue))
    assert(evalWithType(s"i32#${Int.MinValue}") == TInt32Optional -> Some(Int.MinValue))

    assert(evalWithType("i64#1") == TInt64Optional -> Some(1L))
    assert(evalWithType("i64#-1") == TInt64Optional -> Some(-1L))
    assert(evalWithType(s"i64#${Long.MaxValue}") == TInt64Optional -> Some(Long.MaxValue))
    assert(evalWithType(s"i64#${Long.MinValue}") == TInt64Optional -> Some(Long.MinValue))
  }
}
