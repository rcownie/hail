#include "hail/Upcalls.h"
#include "hail/NativePtr.h"
#include <jni.h>
#include <cstdio>
#include <mutex>
#include <string>

#define D(x) { printf("DEBUG[%d]: ", __LINE__); printf x ; printf("\n"); fflush(stdout); }

namespace hail {

class UpcallConfig {
 public:
  JavaVM* java_vm_;
  jobject upcalls_;
  jmethodID info_method_;
  jmethodID warn_method_;
  jmethodID error_method_;

  UpcallConfig() {
    
    java_vm_ = get_saved_java_vm();
    JNIEnv* env = nullptr;
    auto rc = java_vm_->GetEnv((void**)&env, JNI_VERSION_1_8);
    assert(rc == JNI_OK);
    auto cl = env->FindClass("is/hail/nativecode/Upcalls");
    auto init_method = env->GetMethodID(cl, "<init>", "()V");
    // NewObject gives a local ref only valid during this downcall
    auto local_upcalls = env->NewObject(cl, init_method);
    // Get a global ref to the new object
    upcalls_ = env->NewGlobalRef(local_upcalls);
    // Java method signatures are described here:
    // http://journals.ecs.soton.ac.uk/java/tutorial/native1.1/implementing/method.html
    info_method_ = env->GetMethodID(cl, "info", "(Ljava/lang/String;)V");
    warn_method_ = env->GetMethodID(cl, "warn", "(Ljava/lang/String;)V");
    error_method_ = env->GetMethodID(cl, "error", "(Ljava/lang/String;)V");
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
  auto rc = vm->GetEnv((void**)&env_, JNI_VERSION_1_8);
  if (rc == JNI_EDETACHED) {
    if (vm->AttachCurrentThread((void**)&env_, nullptr) != JNI_OK) {
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

void UpcallEnv::info(const std::string& msg) {
  jstring msgJ = env_->NewStringUTF(msg.c_str());
  env_->CallVoidMethod(config_->upcalls_, config_->info_method_, msgJ);
  env_->DeleteLocalRef(msgJ);
}

void UpcallEnv::warn(const std::string& msg) {
  jstring msgJ = env_->NewStringUTF(msg.c_str());
  env_->CallVoidMethod(config_->upcalls_, config_->info_method_, msgJ);
  env_->DeleteLocalRef(msgJ);
}

void UpcallEnv::error(const std::string& msg) {
  jstring msgJ = env_->NewStringUTF(msg.c_str());
  env_->CallVoidMethod(config_->upcalls_, config_->info_method_, msgJ);
  env_->DeleteLocalRef(msgJ);
}

} // namespace hail
