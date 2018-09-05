#include "hail/ObjectArray.h"

namespace hail {

ObjectArray::ObjectArray(JNIEnv* env, jobjectArray objects) {
  ssize_t n = env->GetArraySize(objects);
  vec_.resize(n);
  for (ssize_t j = 0; j < n; ++j) {
    jobject local_ref = env->GetObjectArrayElement(objects, j);
    vec_[j] = env->NewGlobalRef(local_ref);
  }
}

ObjectArray::~ObjectArray() {
  // Need to get a valid JNIEnv to 
  for (ssize_t j = vec_.size(); --j >= 0;) {
    env->ReleaseGlobalRef(vec_[j]);
  }
}

}