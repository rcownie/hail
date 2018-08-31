#ifndef HAIL_UPCALLS_H
#define HAIL_UPCALLS_H 1

#include <jni.h>
#include <cstdint>
#include <string>

namespace hail {

class UpcallConfig;

class UpcallEnv {
 private:
  UpcallConfig* config_; // once-per-session jobject/classID/methodID's
  JNIEnv* env_;
  bool did_attach_;
  
 public:
  // Constructor ensures thread is attached to JavaVM, and gets a JNIEnv 
  UpcallEnv();

  // Destructor restores the previous state
  ~UpcallEnv();
  
  // Test with same interface as logging calls 
  void set_test_msg(const std::string& msg);
  
  // Logging (through is.hail.utils)
  void info(const std::string& msg);
  void warn(const std::string& msg);
  void error(const std::string& msg);

};

} // end hail

#endif
