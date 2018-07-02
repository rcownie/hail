// src/main/c/NativeFunc.cpp - native funcs for Scala NativeLongFunc
#include "hail/NativePtr.h"
#include "hail/NativeStatus.h"
#include <cassert>
#include <jni.h>

namespace hail {

namespace {

using NativeFunc = NativeFuncObj<int64_t>;

NativeFunc* to_NativeFunc(JNIEnv* env, jobject thisJ) {
  // Should be a dynamic_cast, but RTTI causes trouble
  return static_cast<NativeFunc*>(get_from_NativePtr(env, thisJ));
}

} // end anon

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL0, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL0, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL1, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL1, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL2, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL2, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL3, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL3, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1,
  jlong a2
  
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1, a2);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL4, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL4, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1, a2, a3);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL5, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL5, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3,
  jlong a4
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1, a2, a3, a4);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL6, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL6, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3,
  jlong a4,
  jlong a5
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1, a2, a3, a4, a5);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL7, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL7, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3,
  jlong a4,
  jlong a5,
  jlong a6
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1, a2, a3, a4, a5, a6);
}

<<<<<<< HEAD
NATIVEMETHOD(jlong, NativeLongFuncL8, nativeApply)(
=======
NATIVEMETHOD(jlong, NativeLongFuncL8, apply)(
>>>>>>> master
  JNIEnv* env,
  jobject thisJ,
  jlong st,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3,
  jlong a4,
  jlong a5,
  jlong a6,
  jlong a7
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  auto status = reinterpret_cast<NativeStatus*>(st);
  return f->func_(status, a0, a1, a2, a3, a4, a5, a6, a7);
}

} // end hail
