#ifndef HAIL_NATIVEREGION_H
#define HAIL_NATIVEREGION_H 1
//
// include/hail/NativeRegion.h - off-heap version of Region
//
// Richard Cownie, Hail Team, 2018-05-04
//
#include "hail/CommonDefs.h"
#include "hail/NativeObj.h"
#include <vector>
#include <stdint.h>
#include <string.h>

NAMESPACE_BEGIN(hail)

typedef std::vector<int8_t> VecByte;

class NativeRegion : public NativeObj {
private:
  static const size_t kInitCapacity = 16*1024;
public:
  long  capacity_;
  char* buf_;
  char* lim_;
  
public:
  NativeRegion() :
    capacity_(kInitCapacity),
    buf_(malloc(capacity_)),
    lim_(buf_) {
  }
  
  ~NativeRegion() {
    if (buf_) free(buf_);
  }

  inline long size() const { return(lim_-buf_); }

  inline int8_t  loadByte(long off) const { return *(int8_t*)(buf_+off); }
  inline int32_t loadInt(long off) const { return *(int32_t*)(buf_+off); }
  inline int64_t loadLong(long off) const { return *(int64_t*)(buf_+off); }
  inline float   loadFloat(long off) const { return *(float*)(buf_+off); }
  inline double  loadDouble(long off) const { return *(double*)(buf_+off); }
  inline long    loadAddress(long off) const { return *(long*)(buf_+off); }
  inline bool    loadBoolean(long off) const { return (buf_[off] ? true : false); }

  inline VecByte loadBytes(long off, long n) const {
    VecByte vec(n);
    memcpy(vec.data(), buf_+off, n);
    return vec;
  }
  
  inline void loadBytes(long off, char* dstBuf, long dstOff, long n) const {
    memcpy(dstBuf+dstOff, buf_+off, n);
  }
  
  inline void storeByte(long off, int8_t val) { *(int8_t*)(buf_+off) = val; }
  inline void storeInt(long off, int32_t val) { *(int32_t*)(buf_+off) = val; }
  inline void storeLong(long off, int64_t val) { *(int64_t*)(buf_+off) = val; }
  inline void storeFloat(long off, float val) { *(float*)(buf_+off) = val; }
  inline void storeDouble(long off, double val) { *(double*)(buf_+off) = val; }
  inline void storeAddress(long off, long val) { *(long*)(buf_+off) = val; }
  inline void storeBoolean(long off, bool val) { buf_[off] = (val ? 0x1 : 0x0); }

  inline void storeBytes(long off, const VecByte& vec) {
    memcpy(buf_+off, vec.data(), vec.size());
  }
  
  inline void storeBytes(long off, char* srcBuf, long srcOff, long n) {
    memcpy(buf_+off, srcBuf+srcOff, n);
  }
  
  void resize(long needCap) {
    long newCap = (capacity_ < kInitCapacity) ? kInitCapacity : capacity_<<1);
    while (newCap < needCap) newCap <<= 1;
    char* newBuf = malloc(newCap);
    if (buf_) {
      memcpy(newBuf, buf_, lim_-buf_);
      free(buf_);
    }
    lim_ = newBuf+(lim_-buf_);
    capacity_ = newCap;
    buf_ = newBuf;
  }

  inline void ensure(long n) {
    long needCap = (lim_-buf_)+n;
    if (needCap > capacity_) resize(needCap);
  }
  
  inline void align(long alignment) {
    char* newLim = ((lim_-buf_) + (alignment-1)) & ~(alignment-1);
    for (char* p = lim_; p < newLim;) *p++ = 0;
    lim_ = newLim;
  }
  
  inline void clear(long newEnd) {
    lim_ = (buf_ + newEnd);
  }
  
  inline void clear() {
    lim_ = buf_;
  }
  
  inline long allocate(long n) {
    long off = (lim_-buf_);
    lim_ += n;
    return(off);
  }
  
  inline long allocate(long alignment, long n) {
    align(alignment);
    long off = (lim_-buf_);
    lim_ += n;
    return(off);
  }
  
  bool loadBit(long off, long bitOff) {
    return (bool)(buf_[off+(bitOff>>3)] >> (bitOff & 0x7)) & 0x1);
  }
  
  void storeBit(long off, log bitOff, bool val) {
    if (val) {
      buf_[off+(bitOff>>3)] |= (1 << (bitOff & 0x7));
    } else {
      buf_[off+(bitOff>>3)] &= ~(1 << (bitOff & 0x7));
    }
  }

};

NAMESPACE_END(hail)

#endif
