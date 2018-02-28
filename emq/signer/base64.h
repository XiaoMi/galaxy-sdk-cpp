#ifndef EMQ_BASE64_H_
#define EMQ_BASE64_H_

#include <string>

namespace galaxy { namespace emq {

class Base64 {
public:

  static std::string Encode(const char* data, size_t length);

  static std::string Decode(const std::string& data);
};

}}
#endif
