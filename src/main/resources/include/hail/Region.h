#ifndef HAIL_REGION_H
#define HAIL_REGION_H 1

#include "hail/NativeObj.h"
#include "hail/NativePtr.h"
#include <stdlib.h>
#include <memory>
#include <vector>

namespace hail {

class Region;
using RegionPtr = std::shared_ptr<Region>;

class Region : public NativeObj {
private:
  static const long kChunkCap = (16*1024);
  static const long kMaxSmall = 256;
public:
  char* chunk_;
  long  pos_;
  std::vector<char*> free_chunks_;
  std::vector<char*> full_chunks_;
  std::vector<char*> big_allocs_;
  std::vector<RegionPtr> ancestors_;

public:  
  Region() :
    chunk_(nullptr),
    pos_(kChunkCap) {
  }
  
  virtual ~Region() {
    if (chunk_) free(chunk_);
    for (auto p : free_chunks_) free(p);
    for (auto p : full_chunks_) free(p);
    for (auto p : big_allocs_) free(p);
  }
  
  // clear_but_keep_mem() will make the Region empty without free'ing chunks
  void clear_but_keep_mem() {
    pos_ = (chunk_ ? 0 : kChunkCap);
    for (auto p : full_chunks_) free_chunks_.push_back(p);
    full_chunks_.clear();
    // But we do need to free() the big_allocs_
    for (auto p : big_allocs_) free(p);
    big_allocs_.clear();
  }
  
  void new_chunk() {
    if (chunk_) full_chunks_.push_back(chunk_);
    if (free_chunks_.empty()) {
      chunk_ = (char*)malloc(kChunkCap);
    } else {
      chunk_ = free_chunks_.back();
      free_chunks_.pop_back();
    }
    pos_ = 0;
  }
  
  inline void align(long a) {
    pos_ = (pos_ + a-1) & ~(a-1);
  }
  
  inline char* allocate(long a, long n) {
    long mask = (a-1);
    if (n <= kMaxSmall) {
      long apos = ((pos_ + mask) & ~mask);
      if (apos+n > kChunkCap) {
        new_chunk();
        apos = 0;
      }
      char* p = (chunk_ + apos);
      pos_ = (apos + n);
      return p;
    } else {
      char* p = (char*)malloc((n+mask) & ~mask);
      big_allocs_.push_back(p);
      return p;
    }
  }
    
  inline char* allocate(long n) {
    if (n <= kMaxSmall) {
      long apos = pos_;
      if (apos+n > kChunkCap) {
        new_chunk();
        apos = 0;
      }
      char* p = (chunk_ + apos);
      pos_ = (apos + n);
      return p;
    } else {
      char* p = (char*)malloc(n);
      big_allocs_.push_back(p);
      return p;
    }
  }
  
  virtual const char* get_class_name() { return "Region"; }
  
  virtual long get_field_offset(int field_size, const char* s) {
    if (strcmp(s, "chunk_") == 0) return((long)&chunk_ - (long)this);
    if (strcmp(s, "pos_") == 0) return((long)&chunk_ - (long)this);
    return(-1);
  }
};

} // end hail

#endif
