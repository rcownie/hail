#include <jni.h>
#include <dlfcn.h>
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <unistd.h>

#define ENABLE_GDB_IN_XTERM 1

// Declare a function to be exported from a DLL with "C" linkage

#define NATIVEMETHOD(cppReturnType, scalaClass, scalaMethod) \
  extern "C" __attribute__((visibility("default"))) \
    cppReturnType Java_is_hail_nativecode_##scalaClass##_##scalaMethod

namespace {

class GdbConfig {
 public:
  GdbConfig() {
#if ENABLE_GDB_IN_XTERM
    fprintf(stderr, "DEBUG: trying to run gdb under xterm ...\n");
    FILE* f = fopen("a.gdb", "w");
    fprintf(f, "handle SIGSEGV noprint\n");
    fprintf(f, "handle SIG62 noprint\n");
    fclose(f);
    int pid = getpid();
    char cmd[512];
    sprintf(
      cmd,
      "xterm -rightbar -e \"sudo gdb -p %d -x a.gdb\" &\n",
      pid
    );
    int rc = system(cmd);
    fprintf(stderr, "DEBUG: system(\"%s\") -> %d\n", cmd, rc);
    for (int j = 10; j >= 0; --j) {
      fprintf(stderr, "%d", j);
      if (j > 0) {
        fprintf(stderr, "..");
        sleep(1);
      }
    }
    fprintf(stderr, " continuing\n");
#endif
  }
};

GdbConfig config;

} // end anon

NATIVEMETHOD(jlong, NativeCode, dlopenGlobal)(
  JNIEnv* env,
  jobject /*thisJ*/,
  jstring dllPathJ
) {
  const char* dll_path = env->GetStringUTFChars(dllPathJ, 0);
  void* handle = dlopen(dll_path, RTLD_GLOBAL|RTLD_LAZY);
  if (!handle) {
    char* msg = dlerror();
    fprintf(stderr, "ERROR: dlopen(\"%s\"): %s\n", dll_path, msg ? msg : "NoError");
  }
  env->ReleaseStringUTFChars(dllPathJ, dll_path);
  return reinterpret_cast<jlong>(handle);
}

NATIVEMETHOD(jlong, NativeCode, dlclose)(
  JNIEnv* env,
  jobject /*thisJ*/,
  jlong handle
) {
  jlong result = dlclose(reinterpret_cast<void*>(handle));
  return result;
}
