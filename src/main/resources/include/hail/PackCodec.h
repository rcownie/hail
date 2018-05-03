#ifndef HAIL_PACKCODEC_H
#define HAIL_PACKCODEC_H 1
//
// include/hail/PackCodec.h - C++ variable-size packed data
//
// Richard Cownie, Hail Team, 2018-05-04
//
#include "hail/CommonDefs.h"
#include "hail/NativeObj.h"
#include <vector>
#include <stdint.h>
#include <string.h>

NAMESPACE_BEGIN(hail)

//
// The C++ decoder looks a little different because we want to do most of
// the decode using methods which don't need to check the data-limit on
// every byte access.
//

class PackDecoderBase {
private:
  static const size_t kBufCapacity = 64*1024;
  // The worst-case byte size of each encoded type
  static const size_t kMaxSizeBoolean = 1;
  static const size_t kMaxSizeByte = 1;
  static const size_t kMaxSizeInt = 5;
  static const size_t kMaxSizeLong = 10;
  static const size_t kMaxSizeFloat = 4;
  static const size_t kMaxSizeDouble = 8;
  
private:
  size_t   capacity_;
  uint8_t* buf_;
  uint8_t* rpos_;
  uint8_t* lim_;
  bool needPush_;
  
public:
  PackDecoderBase() :
    capacity_(kBufCapacity),
    buf_((uint8_t*)malloc(capacity_),
    rpos_(buf_),
    lim_(buf_),
    needPush_(false) {
  }
  
  virtual ~PackDecoderBase() {
    if (buf_) free(buf_);
  }
  
  //
  // Return maximum space available for push
  //
  long prepareForPush() {
    size_t remnantSize = (lim_ - rpos_);
    memcpy(buf_, rpos_, remnantSize);
    lim_ = (buf_ + remnantSize);
    rpos_ = buf_;
    return(capacity_ - remnantSize);
  }
  
  long getPushAddr() {
    return reinterpret_cast<long>(lim_);
  }
  
  void acceptPush(long pushSize) {
    lim_ += pushSize;
  }
  
  //
  // A readXXX method may run out of data, and in that case it
  // sets needPush_ = true, and leaves rpos_ unchanged.
  //
  inline int8_t readByte() {
    auto pos = rpos_;
    if (pos >= lim_) { needPush_ = true; return(0); }
    auto val = *(int8_t*)pos++;
    rpos_ = (uint8_t*)pos;
    return val;
  }
  
  inline int32_t readInt() {
    int32_t val = 0;
    auto pos = rpos_;
    for (int shift = 0;; shift += 7) {
      if (pos >= lim_) { needPush_ = true; return(0); }
      uint8_t b = *pos++;
      val |= (((int32_t)b & 0x7f) << shift);
      if (b & 0x80) break;
    }
    rpos_ = pos;
    return val;
  }
  
  inline int64_t readLong() {
    int32_t val = 0;
    auto pos = rpos_;
    for (int shift = 0;; shift += 7) {
      if (pos >= lim_) { needPush_ = true; return(0); }
      uint8_t b = *pos++;
      val |= (((int64_t)b & 0x7f) << shift);
      if (b & 0x80) break;
    }
    rpos_ = pos;
    return val;
  }
  
  inline float readFloat() {
    auto pos = rpos_;
    if (pos+4 > lim_) { needPush_ = true; return(0); }
    float val = *(float*)pos;
    rpos_ = pos+4;
    return val;
  }

  inline float readDouble() {
    auto pos = rpos_;
    if (pos+8 > lim_) { needPush_ = true; return(0); }
    double val = *(double*)pos;
    rpos_ = pos+8;
    return val;
  }
  
  inline int8_t readByteNoCheck() {
    return (int8_t)(*rpos_++);
  }

  inline int32_t readIntNoCheck() {
    int32_t val = 0;
    auto pos = rpos_;
    for (int shift = 0;; shift += 7) {
      uint8_t b = *pos++;
      val |= (((int32_t)b & 0x7f) << shift);
      if (b & 0x80) break;
    }
    rpos_ = pos;
    return val;
  }
  
  inline int64_t readLongNoCheck() {
    int32_t val = 0;
    auto pos = rpos_;
    for (int shift = 0;; shift += 7) {
      uint8_t b = *pos++;
      val |= (((int64_t)b & 0x7f) << shift);
      if (b & 0x80) break;
    }
    rpos_ = pos;
    return val;
  }
    
  //
  // The arbitrary-size readXX routines return the number of elements
  // read before raising needPush.
  //
  
  long readBytes(void* dstBuf, long n) {
    long avail = (lim_ - rpos_);
    if (n > avail) {
      n = avail;
      needPush_ = true;
    } 
    memcpy(dstBuf, rpos_, n);
    rpos_ += n;
    return n;
  }
  
  long skipBytes(long n) {
    long avail = (lim_ - rpos_);
    if (n > avail) {
      n = avail;
      needPush_ = true;
    } 
    rpos_ += n;
    return n;
  }
  
  long readDoubles(double* dstBuf, long n) {
    long avail = (lim_ - rpos_) / 8;
    if (n > avail) {
      n = avail;
      needPush_ = true;
    } 
    memcpy(dstBuf, rpos_, n*8);
    rpos_ += n*8;
    return n;
  }
  
};

NAMESPACE_END(hail)

#endif

