#ifndef HAIL_REGION_H
#define HAIL_REGION_H 1
//
// include/hail/Region.h - C++ off-heap Region object
//
// Richard Cownie, Hail Team, 2018-05-07
//
#include "hail/CommonDefs.h"
#include "hail/NativeObj.h"
#include <stdint.h>
#include <stdlib.h>
#include <vector>

NAMESPACE_BEGIN(hail)

class RegionObj : public NativeObj {
private:
  static const long kDefaultCapacity = (16*1024);
public:
  long  capacity_;
  char* mem_;
  long  size_;

private:
  RegionObj& operator=(const RegionObj& b); // disallow copy-assign
  
public:
  RegionObj() :
    capacity_(kDefaultCapacity),
    mem_(malloc(capacity_)),
    size_(0) {
  }
  
  RegionObj(long minCap) :
    capacity_((minCap+0xff) & ~0xff),
    mem_(malloc(capacity_)),
    size_(0) {
  }
  
  virtual ~RegionObj() {
    free(mem_);
  }
  
  long getFieldOffset(const char* fieldName) {
    if (!strcmp(fieldName, "capacity_")) return((long)(&((RegionObj*)0)->capacity_);
    if (!strcmp(fieldName, "mem_"))      return((long)(&((RegionObj*)0)->mem_);
    if (!strcmp(fieldName, "size_"))     return((long)(&((RegionObj*)0)->size_);
    return(-1);
  }
  
  inline void clear(long newSize) {
    if (size_ > newSize) size_ = newSize;
  }
  
  inline void clear() {
    size_ = 0;
  }
  
  void grow(long newMinCap) {
    long newCap = capacity_;
    do {
      newCap = ((newCap < kDefaultCapacity) ? kDefaultCapacity : 2*newCap);
    } while (newCap < newMinCap);
    char* newMem = malloc(newCap);
    if (size_ > 0) {
      memcpy(newMem, mem_, size_);
    }
    if (mem_) free(mem_);
    capacity_ = newCap;
    mem_ = newMem;
  }
  
  //
  // WARNING: any time we do an allocate(), that may change the addresses
  // of already-allocated objects.  So we greatly prefer offsets.
  //
  
  long allocate(long n) {
    long newSize = (size_ + n);
    if (capacity_ < newSize) grow(newSize);
    long off = size_;
    size_ = newSize;
    return(off);
  }
  
  long allocate(long align, long n) {
    long alignSize = ((size_ + (align-1)) & ~(align-1));
    long newSize = (alignSize + n);
    if (capacity_ < newSize) grow(newSize);
    char* p = (mem_ + alignSize);
    size_ = newSize;
    return(p);
  }
  
  //
  // Primitive access
  //
  inline int8_t& asByte(long off) { return *(int8_t*)(mem_+off); }
  inline int32_t& asInt(long off) { return *(int32_t*)(mem_+off); }
  inline int64_t& asLong(long off) { return *(int64_t*)(mem_+off); }
  inline int64_t& asAddr(long off) { return *(int64_t*)(mem_+off); }
  inline float& asFloat(long off) { return *(float*)(mem_+off); }
  inline double& asDouble(long off) { return *(double*)(mem_+off); }
  inline char* asCharStar(long off) { return(mem_+off); }
};

typedef std::shared_ptr<RegionObj> Region;

NAMESPACE_END(hail)

#endif
