#ifndef HAIL_NATIVESTATUS_H
#define HAIL_NATIVESTATUS_H 1
//
// include/hail/NativeStatus.h - status/error-reporting
//
// Richard Cownie, Hail Team, 2018-04-24
//
#include "hail/CommonDefs.h"
#include "hail/NativeObj.h"
#include <string>
#include <stdarg.h>

NAMESPACE_BEGIN(hail)

class NativeStatus : public NativeObj {
public:
  int errno_;
  std::string msg_;
  std::string location_;
  
public:
  inline NativeStatus() : errno_(0) { }
  
  virtual ~NativeStatus() { }
  
  inline void clear() {
    // When errno_ == 0, the values of err_ and location_ are ignored
    errno_ = 0;
  }
  
  void set(const char* file, int line, int code, const char* msg, ...) {
    char buf[8*1024];
    sprintf(buf, "%s,%d", file, line);
    location_ = buf;
    errno_ = code;
    va_list argp;
    va_start(argp, msg);
    vsprintf(buf, msg, argp);
    va_end(argp);
    msg_ = buf;
  }
};

typedef std::shared_ptr<NativeStatus> NativeStatusPtr;

#define NATIVE_ERROR(_p, _code, _msg, ...) \
   { (_p)->set(__FILE__, __LINE__, _code, _msg, ##__VA_ARGS__); }

NAMESPACE_END(hail)

#endif