// src/main/c/NativeFunc.cpp - native funcs for Scala NativeLongFunc
#include "hail/NativePtr.h"
#include <assert.h>
#include <jni.h>

namespace hail {

namespace {

using NativeFunc = NativeFuncObj<long>;

NativeFunc* to_NativeFunc(JNIEnv* env, jobject thisJ) {
  // Should be a dynamic_cast, but RTTI causes trouble
  return reinterpret_cast<NativeFunc*>(get_from_NativePtr(env, thisJ));
}

} // end anon

NATIVEMETHOD(long, NativeLongFuncL0, apply)(
  JNIEnv* env,
  jobject thisJ
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_();
}

NATIVEMETHOD(long, NativeLongFuncL1, apply)(
  JNIEnv* env,
  jobject thisJ,
  jlong a0
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_(a0);
}

NATIVEMETHOD(long, NativeLongFuncL2, apply)(
  JNIEnv* env,
  jobject thisJ,
  jlong a0,
  jlong a1
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_(a0, a1);
}

NATIVEMETHOD(long, NativeLongFuncL3, apply)(
  JNIEnv* env,
  jobject thisJ,
  jlong a0,
  jlong a1,
  jlong a2
  
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_(a0, a1, a2);
}

NATIVEMETHOD(long, NativeLongFuncL4, apply)(
  JNIEnv* env,
  jobject thisJ,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_(a0, a1, a2, a3);
}

NATIVEMETHOD(long, NativeLongFuncL5, apply)(
  JNIEnv* env,
  jobject thisJ,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3,
  jlong a4
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_(a0, a1, a2, a3, a4);
}

NATIVEMETHOD(long, NativeLongFuncL6, apply)(
  JNIEnv* env,
  jobject thisJ,
  jlong a0,
  jlong a1,
  jlong a2,
  jlong a3,
  jlong a4,
  jlong a5
) {
  auto f = to_NativeFunc(env, thisJ);
  assert(f);
  return f->func_(a0, a1, a2, a3, a4, a5);
}

NATIVEMETHOD(long, NativeLongFuncL7, apply)(
  JNIEnv* env,
  jobject thisJ,
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
  return f->func_(a0, a1, a2, a3, a4, a5, a6);
}

NATIVEMETHOD(long, NativeLongFuncL8, apply)(
  JNIEnv* env,
  jobject thisJ,
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
  return f->func_(a0, a1, a2, a3, a4, a5, a6, a7);
}

} // end hail
