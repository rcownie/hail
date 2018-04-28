package is.hail.nativecode

import is.hail.SparkSuite
import is.hail.annotations._
import is.hail.check.Gen
import is.hail.check.Prop.forAll
import is.hail.expr._
import is.hail.expr.types._
import is.hail.nativecode._
import org.apache.spark.SparkException
import org.testng.annotations.Test
import is.hail.utils._
import is.hail.testUtils._

object NativeObject {
  //@native def objectMethod(a: Long): Long

  lazy val isLoaded: Boolean = {
    val envHailHome = System.getenv("HAIL_HOME")
    val osName = System.getProperty("os.name")
    if ((osName.length >= 6) && osName.substring(0, 6).equals("Mac OS"))
      System.load(s"${envHailHome}/build/resources/main/darwin/libhail.dylib")
    else
      System.load(s"${envHailHome}/build/resources/main/linux-x86-64/libhail.so")
    true
  }
  
}

class TestFieldAccess {
  var fieldLong: Long = 0

  def testMethodCall(a: Long): Long = {
    if (fieldLong == a) 1 else 0
  }
}

class NativeCodeSuite extends SparkSuite {

  @Test def testAAALoadLibHail(): Unit = {
    NativeObject.isLoaded
  }

  @Test def testNativePtr() = {
    System.err.println("testNativePtr() ...")
    NativeObject.isLoaded
    var a = new NativeStatus()
    assert(a.ok)
    assert(a.use_count() == 1)
    var b = new NativeStatus()
    b.copyAssign(a)
    assert(b.get() == a.get())
    assert(b.use_count() == 2)
    assert(a.use_count() == 2)
    var c = new NativeStatus()
    c.moveAssign(b)
    assert(c.get() == a.get())
    assert(b.get() == 0)
    assert(c.use_count() == 2)
    c.close();
    assert(c.get() == 0)
    assert(a.use_count() == 1)
    c.close()
    assert(a.use_count() == 1)
    var d = new NativeStatus()
    d.copyAssign(a)
    assert(d.get() == a.get())
    assert(a.use_count() == 2)
    var e = new NativeStatus()
    e.copyAssign(a)
    assert(a.use_count() == 3)
    e.copyAssign(d)
    assert(a.use_count() == 3)
    e.moveAssign(d)
    assert(d.get() == 0)
    assert(a.use_count() == 2)
    e.close()
    assert(e.get() == 0)
    assert(a.use_count() == 1)
  }

  @Test def testNativeGlobal() = {
    System.err.println("testNativeGlobal() ...")
    NativeObject.isLoaded
    var ret: Long = -1
    val st = new NativeStatus()
    val globalModule = new NativeModule("global")
    val funcHash1 = globalModule.findLongFuncL1(st, "hailTestHash1")
    if (st.fail) System.err.println(s"error: ${st}")
    assert(st.ok)
    val funcHash8 = globalModule.findLongFuncL8(st, "hailTestHash8")
    if (st.fail) System.err.println(s"error: ${st}")
    assert(st.ok)
    ret = funcHash8(0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8)
    System.err.println(s"funcHash8 funcAddr ${funcHash8.get().toHexString} => ${ret.toHexString}")
    assert(ret == 0x87654321L)
    val t0 = System.currentTimeMillis()
    var sum: Long = 0
    val numCalls = 100*1000000
    var countdown = numCalls
    while (countdown > 0) {
      sum = funcHash1(sum)
      countdown -= 1
    }
    val t1 = System.currentTimeMillis()
    val usecsPerCall = ((t1 - t0) * 1000.0) / numCalls
    System.err.println(s"funcHash1() ~ ${usecsPerCall}usecs")
    assert(usecsPerCall < 0.050)
    /*s
    val testObj = new TestFieldAccess()
    NativeCall.testFieldAccess(testObj)
     */
  }

  @Test def testNativeBuild() = {
    System.err.println("testNativeBuild() ...")
    NativeObject.isLoaded
    val sb = new StringBuilder()
    sb.append("#include \"hail/hail.h\"\n")
    sb.append("NAMESPACE_HAIL_MODULE_BEGIN\n")
    sb.append("\n")
    // A very simple function
    sb.append("long testFunc1(long a0) { return a0+1; }\n\n")
    // Now declare our own NativeObj
    sb.append("class MyObj : public NativeObj {\n")
    sb.append("public:\n")
    sb.append("  int val_;\n")
    sb.append("\n")
    sb.append("  MyObj(int val) : val_(val) { }\n")
    sb.append("  ~MyObj() { }\n")
    sb.append("  const char* getClassName() { return \"MyObj\"; }\n")
    sb.append("};\n")
    sb.append("\n")
    sb.append("NativeObjPtr makeMyObj(long val) {\n")
    sb.append("  return MAKE_NATIVE(MyObj, val);\n")
    sb.append("}\n")
    sb.append("\n")
    sb.append("NAMESPACE_HAIL_MODULE_END\n")
    val options = "-ggdb -O2"
    val st = new NativeStatus()
    val mod = new NativeModule(options, sb.toString(), true)
    mod.findOrBuild(st)
    if (st.fail)
      System.err.println(s"error: ${st}")
    else {
      System.err.println("A")
      val testFunc1 = mod.findLongFuncL1(st, "testFunc1")
      if (st.fail)
        System.err.println(s"error: ${st}")
      else {
        val ret = testFunc1(6)
        System.err.println(s"testFunc(6) returns ${ret}")
      }
      testFunc1.close()
    }
    val makeMyObj: NativePtrFuncL1 = mod.findPtrFuncL1(st, "makeMyObj")
    if (st.fail)
      System.err.println(s"error: ${st}")
    assert(st.ok)
    val myObj = new NativePtr(makeMyObj, 55L)
    assert(myObj.get() != 0)
    // Now try getting the binary
    val key = mod.getKey()
    val binary = mod.getBinary()
    val workerMod = new NativeModule(key, binary)
    val workerFunc1 = workerMod.findLongFuncL1(st, "testFunc1")
    assert(st.ok)
    workerFunc1.close()
    workerMod.close()
    st.close()
    mod.close()
  }

}
