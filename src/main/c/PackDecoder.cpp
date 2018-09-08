#include "hail/PackDecoder.h"
#include "hail/ByteArrayPool.h"
#include "hail/Upcalls.h"
#include <cassert>

namespace hail {

DecoderBase::DecoderBase(ssize_t bufCapacity) :
  total_usec_(0),
  total_size_(0),
  stat_int32_(0),
  stat_double_(0),
  input_(),
  capacity_((bufCapacity > 0) ? bufCapacity : kDefaultCapacity),
  buf_((char*)malloc(capacity_ + ((kSentinelSize+0x3f) & ~0x3f))),
  pos_(0),
  size_(0),
  rv_base_(nullptr) {
  sprintf(tag_, "%04lx", ((long)this & 0xffff) | 0x8000);
}

DecoderBase::~DecoderBase() {
  auto buf = buf_;
  buf_ = nullptr;
  if (buf) free(buf);
}

void DecoderBase::set_input(ObjectArray* input) {
  input_ = std::dynamic_pointer_cast<ObjectArray>(input->shared_from_this());
}

int64_t DecoderBase::get_field_offset(int field_size, const char* s) {
  auto zeroObj = reinterpret_cast<DecoderBase*>(0L);
  if (!strcmp(s, "capacity_")) return (int64_t)&zeroObj->capacity_;
  if (!strcmp(s, "buf_"))      return (int64_t)&zeroObj->buf_;
  if (!strcmp(s, "pos_"))      return (int64_t)&zeroObj->pos_;
  if (!strcmp(s, "size_"))     return (int64_t)&zeroObj->size_;
  if (!strcmp(s, "rv_base_"))  return (int64_t)&zeroObj->rv_base_;
  return -1;
}

void DecoderBase::analyze() {
#ifdef MYSTATS
  std::map<double, std::vector<int32_t> > vals;
  ssize_t total = 0;
  for (auto& pair : freq_int32_) {
    total += pair.second.freq_;
  }
  for (auto& pair : freq_int32_) {
    double percent = -(100.0*pair.second.freq_)/total;
    vals[percent].push_back(pair.first);
  }
  char buf[128];
  sprintf(buf, "/tmp/stats_%s", tag_);
  FILE* f = fopen(buf, "a");
  double sum = 0.0;
  for (auto& pair : vals) {
    double score = -pair.first;
    for (auto val : pair.second) {
      sum += score;
      fprintf(f, "%5.3f cumulative %5.3f val %d\n", score, sum, val);
      if (score >= 95.0) break;
    }
  }
  fclose(f);
#endif
}

#ifdef MYDEBUG
void DecoderBase::hexify(char* out, ssize_t pos, char* p, ssize_t n) {    
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

ssize_t DecoderBase::read_to_end_of_block() {
  assert(size_ >= 0);
  assert(size_ <= capacity_);
  assert(pos_ >= 0);
  assert(pos_ <= size_+1);
  auto remnant = (size_ - pos_);
  if (remnant < 0) {
    return -1;
  }
  if (remnant > 0) {
    memcpy(buf_, buf_+pos_, remnant);
  }
  pos_ = 0;
  size_ = remnant;
  int32_t chunk = (capacity_ - size_);
  UpcallEnv up;
  int32_t rc = up.InputBuffer_readToEndOfBlock(input_->at(0), buf_+size_, (jbyteArray)0,
                                               0, chunk);
  fprintf(stderr, "DEBUG: readToEndOfBlock(%d) -> %d\n", chunk, rc);
  assert(rc <= chunk);
  if (rc < 0) {
    pos_ = (size_ + 1); // (pos > size) means end-of-file
    return -1;
  } else {
    size_ += rc;
    // buf is oversized with space for a sentinel to speed up one-byte-int decoding
    memset(buf_+size_, 0xff, kSentinelSize-1);
    buf_[size_+kSentinelSize-1] = 0x00; // terminator for LEB128 loop
    return rc;
  }
}

int64_t DecoderBase::decode_one_byte() {
  ssize_t avail = (size_ - pos_);
  if (avail <= 0) {
    if ((avail < 0) || (read_to_end_of_block() <= 0)) {
      return -1;
    }
  }
  int64_t result = (buf_[pos_++] & 0xff);
  fprintf(stderr, "DEBUG: decode_one_byte() -> 0x%02x\n", (int)result);
  return result;
}

} // end hail
