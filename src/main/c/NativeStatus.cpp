#include "hail/NativeStatus.h"
#include "hail/NativePtr.h"
#include <jni.h>

namespace hail {

NATIVEMETHOD(long, NativeStatus, nativeCtorErrnoOffset)(
  JNIEnv* env,
  jobject thisJ
) {
  auto status = std::make_shared<NativeStatus>();
  // C++ "offsetof" can be weird when used on subclasses
  long errnoOffset = ((long)&status->errno_) - (long)status.get();
  NativeObjPtr ptr = status;
  init_NativePtr(env, thisJ, &ptr);
  return(errnoOffset);
}

NATIVEMETHOD(jstring, NativeStatus, getMsg)(
  JNIEnv* env,
  jobject thisJ
) {
  auto status = static_cast<NativeStatus*>(get_from_NativePtr(env, thisJ));
  const char* s = ((status->errno_ == 0) ? "NoError" : status->msg_.c_str());
  return env->NewStringUTF(s);
}

NATIVEMETHOD(jstring, NativeStatus, getLocation)(
  JNIEnv* env,
  jobject thisJ
) {
  auto status = static_cast<NativeStatus*>(get_from_NativePtr(env, thisJ));
  const char* s = ((status->errno_ == 0) ? "NoLocation" : status->location_.c_str());
  return env->NewStringUTF(s);
}

}
