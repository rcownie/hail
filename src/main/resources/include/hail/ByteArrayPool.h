#ifndef HAIL_BYTEARRAYPOOL_H
#define HAIL_BYTEARRAYPOOL_H 1

#include <jni.h>
#include <memory>

// To use java.io.InputStream, we have to read into Scala Array[Byte] buffers.
// But we want to recycle these buffers promptly rather than allowing them to
// be garbage-collected, so we manage them from keep-forever pools in C++.

namespace hail {

class ByteArray {
 public:
  ssize_t capacity_;
  jbyteArray array_;

 public:
  ByteArray() : capacity_(0) { }

  ByteArray(ssize_t capacity, jbyteArray array) : capacity_(capacity), array_(array) { }
  
  ByteArray(const ByteArray& b) : capacity_(b.capacity_), array_(b.array_) { }
  
  ByteArray& operator=(const ByteArray& b) {
    capacity_ = b.capacity_;
    array_ = b.array_;
    return *this;
  }

  // Destructor is called when there no more refs, will save the array for reuse
  ~ByteArray();
  
  ssize_t capacity() const { return capacity_; }
  
  jbyteArray array() const { return array_; }
};

using ByteArrayPtr = std::shared_ptr<ByteArray>;

ByteArrayPtr alloc_byte_array(ssize_t min_capacity);

} // end hail

#endif
