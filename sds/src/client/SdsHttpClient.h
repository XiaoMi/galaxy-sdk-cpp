#ifndef _SDS_HTTPCLIENT_H_
#define _SDS_HTTPCLIENT_H_

#include "client/SdsException.h"
#include "client/SdsHttpTransport.h"
#include "sds/Authentication_types.h"
#include "sds/Common_constants.h"
#include "sds/Common_types.h"


using namespace apache::thrift;
using namespace apache::thrift::transport;
using boost::shared_ptr;

class SdsHttpClient : public SdsHttpTransport {
 public:

  SdsHttpClient(boost::shared_ptr<TTransport> transport,
    const std::string& host, const std::string& path, ThriftProtocol::type thriftProtocol,
    const Credential& credential, bool supportAccountKey, int64_t& clockOffset);


  virtual ~SdsHttpClient();

  virtual void flush();

 protected:

  std::string host_;
  std::string path_;
  Credential credential_;
  bool supportAccountKey_;
  ThriftProtocol::type thriftProtocol_;




  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);

private:
  std::string getAuthenticationHeaders(const uint8_t* buf, uint32_t len);
  std::string getMd5(const uint8_t* buf, uint32_t len);
  std::string getSha1Sign(const std::string& host, const uint64_t& timestamp, const std::string& md5);
  std::string sha1(const std::string& data);
};

#endif

