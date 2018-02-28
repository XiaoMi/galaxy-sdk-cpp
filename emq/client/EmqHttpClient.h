#ifndef _EMQ_HTTPCLIENT_H_
#define _EMQ_HTTPCLIENT_H_

#include "client/EmqException.h"
#include "client/EmqHttpTransport.h"
#include "rpc/Authentication_types.h"
#include "rpc/Common_constants.h"
#include "rpc/Common_types.h"
#include "signer/signer.h"


using namespace apache::thrift;
using namespace apache::thrift::transport;
using boost::shared_ptr;

namespace galaxy { namespace emq {
class EmqHttpClient : public EmqHttpTransport {
 public:

  EmqHttpClient(boost::shared_ptr<TTransport> transport,
  const std::string& host, const std::string& path, rpc::common::ThriftProtocol::type thriftProtocol,
  const rpc::auth::Credential& credential);
  const boost::shared_ptr<TSocket> getTransport();
  void setTransport(boost::shared_ptr<TTransport> transport);


  virtual ~EmqHttpClient();

  virtual void flush();

 protected:

  std::string host_;
  std::string path_;
  rpc::auth::Credential credential_;
  rpc::common::ThriftProtocol::type thriftProtocol_;


  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);

private:
  std::string getMd5(const uint8_t* buf, uint32_t len);
  boost::shared_ptr<LinkedListMultimap> createHeader(const uint8_t* buf, uint32_t len);
};

}}

#endif

