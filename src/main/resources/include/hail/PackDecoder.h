#ifndef HAIL_PACKDECODER_H
#define HAIL_PACKDECODER_H 1

#include "hail/NativeObj.h"
#include "hail/Region.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>

namespace hail {

class PackDecoderBase : public NativeObj {
private:
  static constexpr ssize_t kDefaultCapacity = (64*1024);
public:
  ssize_t capacity_;
  char*   buf_;
  ssize_t pos_;
  ssize_t size_;
  char*   rv_base_;
  
public:
  PackDecoderBase(ssize_t bufCapacity = 0) :
    capacity_(bufCapacity ? bufCapacity : kDefaultCapacity),
    buf_((char*)malloc(capacity_)),
    pos_(0),
    size_(0),
    rv_base_(nullptr) {
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
    if (!strcmp(s, "rv_base_"))  return (int64_t)&zeroObj->rv_base_;
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
    fprintf(stderr, "DEBUG: decode_byte() -> 0x%02x\n", (*addr) & 0xff);
    return true;
  }
  
  bool skip_byte() {
    if (pos_ >= size_) return false;
    pos_ += 1;
    return false;
  }
  
  bool decode_int(int32_t* addr) {
    ssize_t pos = pos_;
    ssize_t old = pos;
    int val = 0;
    for (int shift = 0;; shift += 7) {
      if (pos >= size_) return false;
      int b = buf_[pos++];
      val |= ((b & 0x7f) << shift);
      if ((b & 0x80) == 0) break;
    }
    fprintf(stderr, "DEBUG: decode_int() -> %d {nbytes %ld}\n", val, pos-old);
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
    fprintf(stderr, "DEBUG: decode_long() -> %ld\n", val);
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
    fprintf(stderr, "DEBUG: decode_float() -> %.6f\n", (double)*addr);
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
    fprintf(stderr, "DEBUG: decode_double() -> %.6f\n", *addr);
    return true;
  }
  
  bool skip_double() {
    ssize_t pos = pos_ + sizeof(double);
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  void hexify(char* out, char* p, ssize_t n) {    
    for (int j = 0; j < n; j += 8) {
      for (int k = 0; k < 8; ++k) {
        int c = (j+k < n) ? (p[j+k] & 0xff) : ' ';
        int nibble = (c>>4) & 0xff;
        *out++ = ((nibble < 10) ? '0'+nibble : 'a'+nibble-10);
        nibble = (c & 0xf);
        *out++ = ((nibble < 10) ? '0'+nibble : 'a'+nibble-10);
        *out++ = ' ';
      }
      for (int k = 0; k < 8; ++k) {
        int c = (j+k < n) ? (p[j+k] & 0xff) : ' ';
        *out++ = ((' ' <= c) && (c <= '~')) ? c : '.';
      }
      *out++ = '\n';
    }
    *out++ = 0;
  }
    
  ssize_t decode_bytes(char* addr, ssize_t n) {
    ssize_t pos = pos_;
    fprintf(stderr, "DEBUG: decode_bytes buf %p size %ld pos %ld n %ld\n", buf_, size_, pos_, n);
    ssize_t ngot = (size_ - pos);
    if (ngot > n) ngot = n;
    if (ngot > 0) {
      fprintf(stderr, "DEBUG: memcpy(%p, %p, %ld)\n", addr, buf_+pos, ngot);
      memcpy(addr, buf_+pos, ngot);
      pos_ = (pos + ngot);
    }
    char hex[256];
    hexify(hex, addr, (ngot < 32) ? ngot : 32);
    fprintf(stderr, "DEBUG: decode_bytes(%ld) -> %ld\n", n, ngot);
    fprintf(stderr, "%s", hex);
    return ngot;
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
