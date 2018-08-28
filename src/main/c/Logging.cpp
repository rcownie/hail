#include "hail/Logging.h"
#include "hail/Upcalls.h"
#include <string>

namespace hail {

void info(const std::string& msg) {
  UpcallEnv e;
  e.info(msg);
}

void warn(const std::string& msg) {
  UpcallEnv e;
  e.warn(msg);
}

void error(const std::string& msg) {
  UpcallEnv e;
  e.error(msg);
}

}
