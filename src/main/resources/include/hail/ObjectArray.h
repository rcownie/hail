#ifndef HAIL_OBJECTARRAY_H
#define HAIL_OBJECTARRAY_H 1

#include <jni.h>
#include "hail/NativeObj.h"
#include <memory>
#include <vector>

namespace hail {

class ObjectArray : public NativeObj {
 public:
  std::vector<jobject> vec_;

 public:
  // Construct from a Scala Array[Object] passed through JNI as jobjectArray
  ObjectArray(JNIEnv* env, jobjectArray objects);

  ~ObjectArray();
  
  size_t size() const { return vec_.size(); }
  
  jobject operator[](ssize_t idx) const { return vec_[idx]; }
};

using ObjectArrayPtr = std::shared_ptr<ObjectArray>;

}

#endif
