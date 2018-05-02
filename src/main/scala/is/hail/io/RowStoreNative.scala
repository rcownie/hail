package is.hail.io

//
// Faster row-store decode using generated C++
//

import is.hail.annotations._
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.types._
import is.hail.io.compress.LZ4Utils
import is.hail.nativecode._
import is.hail.rvd.{OrderedRVDPartitioner, OrderedRVDSpec, RVDSpec, UnpartitionedRVDSpec}
import is.hail.sparkextras.ContextRDD
import is.hail.utils._
import org.apache.spark.rdd.RDD
import org.json4s.{Extraction, JValue}
import org.json4s.jackson.JsonMethods
import java.io.{Closeable, InputStream, OutputStream, PrintWriter}

import is.hail.asm4s._
import org.apache.spark.TaskContext

//
// 
// 
//

final case class NativePackCodecSpec(child: BufferSpec) extends CodecSpec {
  def buildEncoder(t: Type)(out: OutputStream): Encoder = new PackEncoder(t, child.buildOutputBuffer(out))
  
  //
  // A Decoder has close(), readByte(): Byte, and readRegionValue(Region): Long
  // 
  // This gives a Decoder for RVs of the specified Type
  //
  def buildDecoder(t: Type): (InputStream) => Decoder {
    //
    // Generate the Type-specific C++ code
    //
    val sb = new StringBuilder()
    def emit(s: String): Unit {
      sb.append(s)
      sb.append("\n")
    }
    sb.append("""
#include \"hail\"
NAMESPACE_HAIL_MODULE_BEGIN

class NativeDecoder : public NativeObj {
public:
  static const size_t kBufSize = 64*1024;
  
public:
  char* buf_;
  char* bufPos_;
  char* bufLim_;
  long  wantSize_;
  
  NativeDecoder() :
    buf_(malloc(kBufSize)) {
  }
  
  ~NativeDecoder() {
    free(buf);
  }
  
  
  long decodeUntilDoneOrNeedPush();
};

//
// Return 0 if we have a complete RegionValue, N > 0 to request push of N bytes
//
long NativeDecoder::decodeUntilDoneOrNeedPush() {
}

NativeObjPtr makeNativeDecoder() {
  return MAKE_NATIVE(NativeDecoder);
}

long getPushAddr(long objAddr) {
  return (long)((NativeDecoder*)objAddr)->bufLim_);
}

long getPushSize(long objAddr) {
  return ((NativeDecoder*)objAddr)->wantSize_;
}

long decodeUntilDoneOrNeedPush() {
  return ((NativeDecoder*)objAddr)->decodeUntilDoneOrNeedPush();
}

NAMESPACE_HAIL_MODULE_END
""")
  }
}