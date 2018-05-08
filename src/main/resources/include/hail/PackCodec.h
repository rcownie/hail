#ifndef HAIL_PACKCODEC_H
#define HAIL_PACKCODEC_H 1
//
// include/hail/PackCodec.h - packed file format funcs
//
// Richard Cownie, 
//
#include "hail/CommonDefs.h"
#include "hail/NativeObj.h"
#include "hail/Region.h"

NAMESPACE_BEGIN(hail)

class PackDecoderBase : public NativeObj {
private:
  static const long kDefaultCapacity = (64*1024);
public:
  long  capacity_;
  char* buf_;
  long  pos_;
  long  size_;
  RegionObj* region_;
  
public:
  PackDecoderBase(long bufCapacity) :
    capacity_(bufCapacity ? bufCapacity : kDefaultCapacity),
    buf_(malloc(capacity_)),
    pos_(0),
    size_(0) {
  }
  
  virtual ~PackDecoderBase() {
    if (buf_) free(buf_);
    buf_ = nullptr;
  }
  
  virtual long getFieldOffset(const char* s) {
    auto zeroObj = reinterpret_cast<PackDecoderBase*>(0L);
    if (!strcmp(s, "capacity_")) return (long)&zeroObj->capacity_;
    if (!strcmp(s, "buf_"))      return (long)&zeroObj->buf_;
    if (!strcmp(s, "pos_"))      return (long)&zeroObj->pos_;
    if (!strcmp(s, "size_"))     return (long)&zeroObj->size_;
  }
  
  //
  // Return values:
  //   0  => have a complete RegionValue
  //   >0 => need push of more data
  //   -1 => no more RegionValue's and no more data
  //
  virtual long decodeUntilDoneOrNeedPush(RegionObj* region, long pushSize) = 0;
  
  //
  // Decode methods for primitive types
  //
  inline bool decodeByte(long off) {
    long pos = pos_;
    if (pos > size_) return false;
    region->asByte(off) = *(int8_t*)(buf_+pos);
    pos_ = ++pos
    return true;
  }
  
  inline bool decodeLength(int* len) {
    long pos = pos_;
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
  
  inline bool decodeInt(long off) {
    long pos = pos_;
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
  
  inline bool decodeLong(long off) {
    long pos = pos_;
    long val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      long b = mem_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    region_->asLong(off) = val;
    pos_ = pos;
    return true;
  }
  
  inline bool decodeFloat(long off) {
    long pos = pos_;
    if (pos+4 > size_) return false;
    region_->asFloat(off) = *(float*)(buf_+pos);
    pos_ = (pos+4);
    return true;
  }
  
  inline bool decodeDouble(long off) {
    long pos = pos_;
    if (pos+8 > size_) return false;
    region_->asDouble(off) = *(double*)(buf_+pos);
    pos_ = (pos+8);
    return true;
  }
  
  inline long decodeBytes(long off, long n) {
    long pos = pos_;
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
