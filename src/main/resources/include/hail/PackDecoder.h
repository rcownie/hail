#ifndef HAIL_PACKDECODER_H
#define HAIL_PACKDECODER_H 1

#include "hail/NativeObj.h"
#include "hail/Region.h"
#include <cstdint>

NAMESPACE_BEGIN(hail)

class PackDecoderBase : public NativeObj {
private:
  static constexpr ssize_t kDefaultCapacity = (64*1024);
public:
  ssize_t capacity_;
  char*   buf_;
  ssize_t pos_;
  ssize_t size_;
  
public:
  PackDecoderBase(ssize_t bufCapacity) :
    capacity_(bufCapacity ? bufCapacity : kDefaultCapacity),
    buf_(malloc(capacity_)),
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
  
  //
  // Return values:
  //   0  => have a complete RegionValue
  //   >0 => need push of more data
  //   -1 => no more RegionValue's and no more data
  //
  virtual ssize_t decode_until_done_or_need_push(Region* region, ssize_t push_size) = 0;
  
  int64_t decode_one_byte() {
    if (pos_ >= size_) return -1;
    int64_t b = (buf_[pos_++] & 0xff);
    return b;
  }
  
  //
  // Decode methods for primitive types
  //
  bool decode_byte(ssize_t off) {
    ssize_t pos = pos_;
    if (pos > size_) return false;
    region->asByte(off) = *(int8_t*)(buf_+pos);
    pos_ = ++pos
    return true;
  }
  
  bool decode_length(ssize_t off) {
    ssize_t pos = pos_;
    int val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      int8_t b = mem_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    *len = val;
    pos_ = pos;
    return true;
  }
  
  inline bool decodeInt(ssize_t off) {
    ssize_t pos = pos_;
    int val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      int b = mem_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    region_->asInt(off) = val;
    pos_ = pos;
    return true;
  }
  
  inline bool decodeLong(ssize_t off) {
    ssize_t pos = pos_;
    ssize_t val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      ssize_t b = mem_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    region_->asLong(off) = val;
    pos_ = pos;
    return true;
  }
  
  inline bool decodeFloat(ssize_t off) {
    ssize_t pos = pos_;
    if (pos+4 > size_) return false;
    region_->asFloat(off) = *(float*)(buf_+pos);
    pos_ = (pos+4);
    return true;
  }
  
  inline bool decodeDouble(ssize_t off) {
    ssize_t pos = pos_;
    if (pos+8 > size_) return false;
    region_->asDouble(off) = *(double*)(buf_+pos);
    pos_ = (pos+8);
    return true;
  }
  
  inline ssize_t decodeBytes(ssize_t off, ssize_t n) {
    ssize_t pos = pos_;
    if (n > size_-pos) n = size_-pos;
    if (n > 0) {
      memcpy(region->asCharStar(off), buf_+pos, n);
      pos_ = (pos + n);
    }
    return n;
  }
};

NAMESPACE_END(hail)

#endif
