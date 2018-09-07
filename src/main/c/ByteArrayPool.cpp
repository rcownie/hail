#include "hail/ByteArrayPool.h"
#include "hail/Upcalls.h"
#include <cassert>
#include <mutex>
#include <thread>
#include <vector>

namespace hail {

namespace {

ssize_t capacity_table[] = { 4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 128*1024, 256*1024 };
constexpr int kNumBuckets = sizeof(capacity_table)/sizeof(capacity_table[0]);

std::mutex big_mutex;
std::vector<ByteArray> byte_array_buckets[kNumBuckets];

} // end anon

ByteArrayPtr alloc_byte_array(ssize_t min_capacity) {
  assert(min_capacity <= capacity_table[kNumBuckets-1]);
  ssize_t capacity = 0;
  int idx;
  for (idx = 0; idx < kNumBuckets; ++idx) {
    if (min_capacity <= capacity_table[idx]) {
      capacity = capacity_table[idx];
      break;
    }
  }
  std::lock_guard<std::mutex> mylock(big_mutex);
  auto& bucket = byte_array_buckets[idx];
  if (bucket.empty()) {
    // Allocate a new ByteArray
    UpcallEnv up;
    jbyteArray local_ref = up.env()->NewByteArray(capacity);
    jbyteArray global_ref = (jbyteArray)up.env()->NewGlobalRef((jobject)local_ref);
    up.env()->DeleteLocalRef((jobject)local_ref);
    return std::make_shared<ByteArray>(capacity, global_ref);
  } else {
    // Take the most-recently-used ByteArray from the bucket
    auto& back = bucket.back();
    auto ptr = std::make_shared<ByteArray>(back);
    // back.~ByteArray() will be called, but does nothing with capacity_ < 0
    back.capacity_ = -1;
    bucket.resize(bucket.size()-1);
    return ptr;
  }
}

ByteArray::~ByteArray() {
  if (capacity_ < 0) return;
  int idx;
  for (idx = 0; idx < kNumBuckets; ++idx) {
    if (capacity_ == capacity_table[idx]) break;
  }
  assert(idx < kNumBuckets);
  std::lock_guard<std::mutex> mylock(big_mutex);
  auto& bucket = byte_array_buckets[idx];
  bucket.emplace_back(*this);
}

} // end hail
