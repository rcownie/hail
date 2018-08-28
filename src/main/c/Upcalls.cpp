#include "hail/Upcalls.h"
#include "hail/NativePtr.h"
#include <jni.h>
#include <cstdio>
#include <mutex>
#include <string>

namespace hail {

class UpcallConfig {
 public:
  JavaVM* java_vm_;

  UpcallConfig() :
    java_vm_(get_saved_java_vm()) {
  }
};

namespace {

UpcallConfig* get_config() {
  static UpcallConfig config;
  return &config;
}

} // end anonymous

// Code for JNI interaction is adapted from here:
// https://stackoverflow.com/questions/30026030/what-is-the-best-way-to-save-jnienv/30026231#30026231

// At the moment the C++ does not create any threads, so all threads *should*
// be attached to the vm.  But in case that changes in future, we
// have code to handle the general case.

UpcallEnv::UpcallEnv() :
  config_(get_config()),
  env_(nullptr),
  did_attach_(false) {
  // Is this thread already attached to vm ?
  // The version checks that the running JVM version is compatible
  // with the features used in this code.
  auto vm = config_->java_vm_;
  auto rc = vm->GetEnv((void**)env, JNI_VERSION_1_8);
  if (rc == JNI_EDETACHED) {
    if (vm->AttachCurrentThread(env, nullptr) != JNI_OK) {
      fprintf(stderr, "FATAL: vm->AttachCurrentThread() failed\n");
      assert(0);
    }
    did_attach_ = true;
  } else if (rc == JNI_EVERSION) {
    fprintf(stderr, "FATAL: vm->GetEnv() JNI_VERSION_1_8 not supported\n");
    assert(0);
  }
}

UpcallEnv::~UpcallEnv() {
  if (did_attach_) config_->java_vm_->DetachCurrentThread();
}

UpcallEnv::info(const std::string& msg) {

}

UpcallEnv::warn(const std::string& msg) {
}

UpcallEnv::error(const std::string& msg) {
}

} // namespace hail
