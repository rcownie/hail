#ifndef HAIL_PACKDECODER_H
#define HAIL_PACKDECODER_H 1

#include "hail/NativeObj.h"
#include "hail/Region.h"
#include <cstdint>

namespace hail {

class PackDecoderBase : public NativeObj {
private:
  static constexpr ssize_t kDefaultCapacity = (64*1024);
public:
  ssize_t capacity_;
  char*   buf_;
  ssize_t pos_;
  ssize_t size_;
  
public:
  PackDecoderBase(ssize_t bufCapacity = 0) :
    capacity_(bufCapacity ? bufCapacity : kDefaultCapacity),
    buf_((char*)malloc(capacity_)),
    pos_(0),
    size_(0) {
  }
  
  virtual ~PackDecoderBase() {
    if (buf_) free(buf_);
  }
  
  virtual int64_t get_field_offset(int field_size, const char* s) {
    auto zeroObj = reinterpret_cast<PackDecoderBase*>(0L);
    if (!strcmp(s, "capacity_")) return (int64_t)&zeroObj->capacity_;
    if (!strcmp(s, "buf_"))      return (int64_t)&zeroObj->buf_;
    if (!strcmp(s, "pos_"))      return (int64_t)&zeroObj->pos_;
    if (!strcmp(s, "size_"))     return (int64_t)&zeroObj->size_;
    return -1;
  }
  
  static ssize_t round_up_align(ssize_t n, ssize_t align) {
    return ((n + (align-1)) & ~(align-1));
  }
  
  static ssize_t missing_bytes(ssize_t nbits) {
    return ((nbits + 7) >> 3);
  }
  
  static ssize_t elements_offset(ssize_t n, bool required, ssize_t align) {
    if (align < sizeof(int32_t)) align = sizeof(int32_t);
    return round_up_align(sizeof(int32_t) + (required ? 0 : missing_bytes(n)), align);
  }
  
  static bool is_missing(char* missing_base, ssize_t idx) {
    return (bool)((missing_base[idx>>3] >> (idx&7)) & 0x1);
  }
  
  // Return values:
  //   0  => have a complete RegionValue
  //   >0 => need push of more data
  //   -1 => no more RegionValue's and no more data
  virtual ssize_t decode_until_done_or_need_push(Region* region, ssize_t push_size) = 0;
  
  ssize_t prepare_for_push() {
    if (pos_ < size_) memcpy(buf_, buf_+pos_, size_-pos_);
    size_ -= pos_;
    pos_ = 0;
    return (capacity_ - size_);
  }
  
  ssize_t decode_one_byte(ssize_t push_size) {
    size_ += push_size;
    if (pos_ >= size_) {
      pos_ = 0;
      size_ = 0;
      return capacity_;
    }
    return (buf_[pos_++] & 0xff);
  }
  
  //
  // Decode methods for primitive types
  //
  bool decode_byte(int8_t* addr) {
    ssize_t pos = pos_;
    if (pos >= size_) return false;
    *addr = *(int8_t*)(buf_+pos);
    pos_ = (pos+1);
    return true;
  }
  
  bool skip_byte() {
    if (pos_ >= size_) return false;
    pos_ += 1;
    return false;
  }
  
  bool decode_int(int32_t* addr) {
    ssize_t pos = pos_;
    int val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      int b = buf_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    *addr = val;
    pos_ = pos;
    return true;
  }
  
  bool skip_int() {
    ssize_t pos = pos_;
    do {
      if (pos >= size_) return false;
    } while ((buf_[pos++] & 0x80) != 0);
    pos_ = pos;
    return true;
  }
  
  bool decode_length(ssize_t* addr) {
    int32_t len = 0;
    if (!decode_int(&len)) return false;
    *addr = (ssize_t)len;
    return true;
  }
  
  bool decode_long(int64_t* addr) {
    ssize_t pos = pos_;
    ssize_t val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      ssize_t b = buf_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    *addr = val;
    pos_ = pos;
    return true;
  }
  
  bool skip_long() {
    ssize_t pos = pos_;
    do {
      if (pos >= size_) return false;
    } while ((buf_[pos++] & 0x80) != 0);
    pos_ = pos;
    return true;
  }
  
  bool decode_float(float* addr) {
    ssize_t pos = pos_;
    if (pos+4 > size_) return false;
    *addr = *(float*)(buf_+pos);
    pos_ = (pos+4);
    return true;
  }
  
  bool skip_float() {
    ssize_t pos = pos_ + sizeof(float);
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  bool decode_double(double* addr) {
    ssize_t pos = pos_;
    if (pos+8 > size_) return false;
    *addr = *(double*)(buf_+pos);
    pos_ = (pos+8);
    return true;
  }
  
  bool skip_double() {
    ssize_t pos = pos_ + sizeof(double);
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
    
  ssize_t decode_bytes(char* addr, ssize_t n) {
    ssize_t pos = pos_;
    if (n > size_-pos) n = size_-pos;
    if (n > 0) {
      memcpy(addr, buf_+pos, n);
      pos_ = (pos + n);
    }
    return n;
  }
  
  ssize_t skip_bytes(ssize_t n) {
    ssize_t pos = pos_;
    if (n > size_-pos) n = size_-pos;
    pos_ = pos + n;
    return n;
  }
};

} // end hail

#endif
