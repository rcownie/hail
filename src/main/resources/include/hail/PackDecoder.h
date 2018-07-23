#ifndef HAIL_PACKDECODER_H
#define HAIL_PACKDECODER_H 1

#include "hail/NativeObj.h"
#include "hail/Region.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

//#define MYDEBUG

#define BIG_METHOD_INLINE 0

#if BIG_METHOD_INLINE
# define MAYBE_INLINE inline
#else
# define MAYBE_INLINE
#endif

namespace hail {

inline ssize_t round_up_align(ssize_t n, ssize_t align) {
  return ((n + (align-1)) & ~(align-1));
}
  
inline ssize_t missing_bytes(ssize_t nbits) {
  return ((nbits + 7) >> 3);
}
  
inline ssize_t elements_offset(ssize_t n, bool required, ssize_t align) {
  return round_up_align(sizeof(int32_t) + (required ? 0 : missing_bytes(n)), align);
}

inline void set_all_missing(char* miss, ssize_t nbits) {
  memset(miss, 0xff, (nbits+7)>>3);
  int partial = (nbits & 0x7);
  if (partial != 0) miss[nbits>>3] = (1<<partial)-1;
}
  
inline bool is_missing(char* missing_base, ssize_t idx) {
  return (bool)((missing_base[idx>>3] >> (idx&7)) & 0x1);
}

inline void set_all_missing(std::vector<char>& missing_vec, ssize_t nbits) {
  ssize_t nbytes = ((nbits+7)>>3);
  if (missing_vec.size() < (size_t)nbytes) missing_vec.resize(nbytes);
  memset(&missing_vec[0], 0xff, nbytes);
  int partial = (nbits & 0x7);
  if (partial != 0) missing_vec[nbits>>3] = (1<<partial)-1;
}
  
inline bool is_missing(const std::vector<char>& missing_vec, ssize_t idx) {
  return (bool)((missing_vec[idx>>3] >> (idx&7)) & 0x1);
}

inline void stretch_size(std::vector<char>& missing_vec, ssize_t minsize) {
  if (ssize(missing_vec) < minsize) missing_vec.resize(minsize);
}
  
class DecoderBase : public NativeObj {
private:
  static constexpr ssize_t kDefaultCapacity = (64*1024);
public:
  ssize_t capacity_;
  char*   buf_;
  ssize_t pos_;
  ssize_t size_;
  char*   rv_base_;
  char    tag_[8];
  
public:
  DecoderBase(ssize_t bufCapacity = 0) :
    capacity_(bufCapacity ? bufCapacity : kDefaultCapacity),
    buf_((char*)malloc(capacity_)),
    pos_(0),
    size_(0),
    rv_base_(nullptr) {
    sprintf(tag_, "%04lx", ((long)this & 0xffff) | 0x8000);
  }
  
  virtual ~DecoderBase() {
    auto buf = buf_;
    buf_ = nullptr;
    if (buf) free(buf);
  }
  
  virtual int64_t get_field_offset(int field_size, const char* s) {
    auto zeroObj = reinterpret_cast<DecoderBase*>(0L);
    if (!strcmp(s, "capacity_")) return (int64_t)&zeroObj->capacity_;
    if (!strcmp(s, "buf_"))      return (int64_t)&zeroObj->buf_;
    if (!strcmp(s, "pos_"))      return (int64_t)&zeroObj->pos_;
    if (!strcmp(s, "size_"))     return (int64_t)&zeroObj->size_;
    if (!strcmp(s, "rv_base_"))  return (int64_t)&zeroObj->rv_base_;
    return -1;
  }
  
  // Return values:
  //   0  => have a complete RegionValue
  //   >0 => need push of more data
  //   -1 => no more RegionValue's and no more data
  virtual ssize_t decode_until_done_or_need_push(Region* region, ssize_t push_size) = 0;
  
  ssize_t prepare_for_push() {
    if (pos_ < size_) {
      memcpy(buf_, buf_+pos_, size_-pos_);
    }
    size_ -= pos_;
    pos_ = 0;
    return (capacity_ - size_); // capacity for new data
  }
  
  ssize_t decode_one_byte(ssize_t push_size) {
    size_ += push_size;
    if (pos_ >= size_) {
      return prepare_for_push(); // returns > 0
    }
    return -(buf_[pos_++] & 0xff); // returns <= 0
  }

#ifdef MYDEBUG
  void hexify(char* out, ssize_t pos, char* p, ssize_t n) {    
    for (int j = 0; j < n; j += 8) {
      sprintf(out, "[%4ld] ", pos+j);
      out += strlen(out);
      for (int k = 0; k < 8; ++k) {
        if (j+k >= n) {
          *out++ = ' ';
          *out++ = ' ';
        } else {
          int c = (j+k < n) ? (p[j+k] & 0xff) : ' ';
          int nibble = (c>>4) & 0xff;
          *out++ = ((nibble < 10) ? '0'+nibble : 'a'+nibble-10);
          nibble = (c & 0xf);
          *out++ = ((nibble < 10) ? '0'+nibble : 'a'+nibble-10);
        }
        *out++ = ' ';
      }
      *out++ = ' ';
      for (int k = 0; k < 8; ++k) {
        int c = (j+k < n) ? (p[j+k] & 0xff) : ' ';
        *out++ = ((' ' <= c) && (c <= '~')) ? c : '.';
      }
      *out++ = '\n';
    }
    *out++ = 0;
  }
#endif
    
};

//
// DecoderId=0 fixed-size int/long
// DecoderId=1 variable-length LEB128
//
template<int DecoderId>
class PackDecoderBase : public DecoderBase {
 public:
  virtual ~PackDecoderBase() { }
  //
  // Decode methods for primitive types
  //
  bool decode_byte(int8_t* addr) {
    ssize_t pos = pos_;
    if (pos >= size_) return false;
    *addr = *(int8_t*)(buf_+pos);
    pos_ = pos+1;
    //fprintf(stderr, "DEBUG: %s A decode_byte() -> 0x%02x [%p]\n", tag_, (*addr) & 0xff, addr);
    return true;
  }
  
  bool skip_byte() {
    ssize_t pos = pos_+1;
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  bool decode_int(int32_t* addr) {
    ssize_t pos = pos_;
    if (pos+4 > size_) return false;
    *addr = *(int32_t*)&buf_[pos];
    pos_ = pos+4;
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s A decode_int() -> %d\n", tag_, *addr);
    char hex[256];
    hexify(hex, pos, buf_+pos, 4);
    fprintf(stderr, "%s", hex);
#endif
    return true;
  }
  
  bool skip_int() {
    ssize_t pos = pos_+4;
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  bool decode_length(ssize_t* addr) {
    int32_t len = 0;
    if (!decode_int(&len)) return false;
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s A decode_length() -> %d\n", tag_, len);
#endif
    *addr = (ssize_t)len;
    return true;
  }
  
  bool decode_long(int64_t* addr) {
    ssize_t pos = pos_;
    if (pos+8 > size_) return false;
    *addr = *(int64_t*)&buf_[pos];
    pos_ = pos+8;
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s A decode_long() -> %ld\n", tag_, (long)*addr);
    char hex[256];
    hexify(hex, pos, buf_+pos, 8);
    fprintf(stderr, "%s", hex);
#endif
    return true;
  }
  
  bool skip_long() {
    ssize_t pos = pos_+8;
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  bool decode_float(float* addr) {
    ssize_t pos = pos_;
    if (pos+4 > size_) return false;
    *addr = *(float*)(buf_+pos);
    pos_ = (pos+4);
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s A decode_float() -> %12e\n", tag_, (double)*addr);
#endif
    return true;
  }
  
  bool skip_float() {
    ssize_t pos = pos_+4;
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  bool decode_double(double* addr) {
    ssize_t pos = pos_;
    if (pos+8 > size_) return false;
    *addr = *(double*)(buf_+pos);
    pos_ = (pos+8);
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s A decode_double() -> %12e\n", tag_, *addr);
#endif
    return true;
  }
  
  bool skip_double() {
    ssize_t pos = pos_+8;
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  ssize_t decode_bytes(char* addr, ssize_t n) {
    ssize_t pos = pos_;
    ssize_t ngot = (size_ - pos);
    if (ngot > n) ngot = n;
    if (ngot > 0) {
      memcpy(addr, buf_+pos, ngot);
      pos_ = (pos + ngot);
    }
#ifdef MYDEBUG
    char hex[256];
    hexify(hex, pos, buf_+pos, (ngot < 32) ? ngot : 32);
    fprintf(stderr, "DEBUG: %s A decode_bytes(%ld) -> %ld\n", tag_, n, ngot);
    fprintf(stderr, "%s", hex);
#endif
    return ngot;
  }
  
  ssize_t skip_bytes(ssize_t n) {
    ssize_t pos = pos_;
    if (n > size_-pos) n = size_-pos;
    pos_ = pos + n;
    return n;
  }
};

template<>
class PackDecoderBase<1> : public DecoderBase {
 public:
  virtual ~PackDecoderBase() { }
  //
  // Decode methods for primitive types
  //
  bool decode_byte(int8_t* addr) {
    ssize_t pos = pos_;
    if (pos >= size_) return false;
    *addr = *(int8_t*)(buf_+pos);
    pos_ = (pos+1);
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s B decode_byte() -> 0x%02x [%p]\n", tag_, (*addr) & 0xff, addr);
#endif
    return true;
  }
  
  bool skip_byte() {
    if (pos_ >= size_) return false;
    pos_ += 1;
    return true;
  }
  
  bool decode_int(int32_t* addr);
  
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
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s B decode_length() -> %d\n", tag_, len);
#endif
    *addr = (ssize_t)len;
    return true;
  }
  
  bool decode_long(int64_t* addr);

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
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s B decode_float() -> %12e\n", tag_, (double)*addr);
#endif
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
#ifdef MYDEBUG
    fprintf(stderr, "DEBUG: %s B decode_double() -> %12e\n", tag_, *addr);
#endif
    return true;
  }
  
  bool skip_double() {
    ssize_t pos = pos_ + sizeof(double);
    if (pos > size_) return false;
    pos_ = pos;
    return true;
  }
  
  ssize_t decode_bytes(char* addr, ssize_t n);

  ssize_t skip_bytes(ssize_t n) {
    ssize_t pos = pos_;
    if (n > size_-pos) n = size_-pos;
    pos_ = pos + n;
    return n;
  }
};

MAYBE_INLINE bool PackDecoderBase<1>::decode_int(int32_t* addr) {
  ssize_t pos = pos_;
  int val = 0;
  for (int shift = 0;; shift += 7) {
    if (pos >= size_) return false;
    int b = buf_[pos++];
    val |= ((b & 0x7f) << shift);
    if ((b & 0x80) == 0) break;
  }
  *addr = val;
#ifdef MYDEBUG
  fprintf(stderr, "DEBUG: %s B decode_int() -> %d\n", tag_, val);
  char hex[256];
  hexify(hex, pos_, buf_+pos_, pos-pos_);
  fprintf(stderr, "%s", hex);
#endif
  pos_ = pos;
  return true;
}

MAYBE_INLINE bool PackDecoderBase<1>::decode_long(int64_t* addr) {
  ssize_t pos = pos_;
  ssize_t val = 0;
  for (int shift = 0;; shift += 7) {
    if (pos >= size_) return false;
    ssize_t b = buf_[pos++];
    val |= ((b & 0x7f) << shift);
    if ((b & 0x80) == 0) break;
  }
  *addr = val;
#ifdef MYDEBUG
  fprintf(stderr, "DEBUG: %s B decode_long() -> %ld\n", tag_, val);
  char hex[256];
  hexify(hex, pos_, buf_+pos_, pos-pos_);
  fprintf(stderr, "%s", hex);
#endif
  pos_ = pos;
  return true;
}
  
MAYBE_INLINE ssize_t PackDecoderBase<1>::decode_bytes(char* addr, ssize_t n) {
  ssize_t pos = pos_;
  ssize_t ngot = (size_ - pos);
  if (ngot > n) ngot = n;
  if (ngot > 0) {
    memcpy(addr, buf_+pos, ngot);
    pos_ = (pos + ngot);
  }
#ifdef MYDEBUG
  char hex[256];
  hexify(hex, pos, buf_+pos, (ngot < 32) ? ngot : 32);
  fprintf(stderr, "DEBUG: %s B decode_bytes(%ld) -> %ld\n", tag_, n, ngot);
  fprintf(stderr, "%s", hex);
#endif
  return ngot;
}

} // end hail

#endif
