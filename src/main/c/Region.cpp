#include "hail/Region.h"
#include "hail/NativeObj.h"
#include "hail/NativePtr.h"
#include <jni.h>

namespace hail {

NATIVEMETHOD(void, Region, nativeCtor)(
  JNIEnv* env,
  jobject thisJ
) {
  auto ptr = MAKE_NATIVE(Region);
  init_NativePtr(env, thisJ, &ptr);
}

NATIVEMETHOD(void, Region, clearButKeepMem)(
  JNIEnv* env,
  jobject thisJ
) {
  auto r = reinterpret_cast<Region*>(get_from_NativePtr(env, thisJ));
  r->clear_but_keep_mem();
}

NATIVEMETHOD(void, Region, nativeAlign)(
  JNIEnv* env,
  jobject thisJ,
  jlong a
) {
  auto r = reinterpret_cast<Region*>(get_from_NativePtr(env, thisJ));
  r->align(a);
}

NATIVEMETHOD(long, Region, nativeAlignAllocate)(
  JNIEnv* env,
  jobject thisJ,
  jlong a,
  jlong n
) {
  auto r = reinterpret_cast<Region*>(get_from_NativePtr(env, thisJ));
  return reinterpret_cast<long>(r->allocate(a, n));
}

NATIVEMETHOD(long, Region, nativeAllocate)(
  JNIEnv* env,
  jobject thisJ,
  jlong n
) {
  auto r = reinterpret_cast<Region*>(get_from_NativePtr(env, thisJ));
  return reinterpret_cast<long>(r->allocate(n));
}

} // end hail
