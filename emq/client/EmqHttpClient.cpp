
#include <limits>
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include <openssl/md5.h>
#include <openssl/hmac.h>
#include <sys/time.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <Poco/DateTime.h>
#include <Poco/DateTimeFormatter.h>
#include "client/EmqHttpClient.h"
#include "rpc/Common_constants.h"
#include "signer/signer.h"


using boost::shared_ptr;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Poco;

namespace galaxy { namespace emq {
extern const rpc::common::CommonConstants g_Common_constants;

EmqHttpClient::EmqHttpClient(boost::shared_ptr<TTransport> transport,
    const std::string& host, const std::string& path, rpc::common::ThriftProtocol::type thriftProtocol,
    const rpc::auth::Credential& credential) :
  EmqHttpTransport(transport),
  host_(host),
  path_(path),
  thriftProtocol_(thriftProtocol),
  credential_(credential){
}


EmqHttpClient::~EmqHttpClient() {}

void EmqHttpClient::parseHeader(char* header) {
  char* colon = strchr(header, ':');
  if (colon == NULL) {
    return;
  }
  char* value = colon+1;

  if (boost::istarts_with(header, "Transfer-Encoding")) {
    if (boost::iends_with(value, "chunked")) {
      chunked_ = true;
    }
  } else if (boost::istarts_with(header, "Content-Length")) {
    chunked_ = false;
    contentLength_ = atoi(value);
  }
}

bool EmqHttpClient::parseStatusLine(char* status) {
  char* http = status;
  char* code = strchr(http, ' ');
  if (code == NULL) {
    throw TTransportException(std::string("Bad Status: ") + status);
  }

  *code = '\0';
  while (*(code++) == ' ') {};

  char* msg = strchr(code, ' ');
  if (msg == NULL) {
    throw TTransportException(std::string("Bad Status: ") + status);
  }
  *msg = '\0';

  if (strcmp(code, "200") == 0) {
    statusCode_ = 200;
    return true;
  } else if (strcmp(code, "100") == 0) {
    return false;
  } else {
    statusCode_ = atoi(code);
    errorMessage_ = std::string("Bad Status: ") + status;
    return true;
  }
}

void EmqHttpClient::flush() {
  uint8_t* buf;
  uint32_t len;
  writeBuffer_.getBuffer(&buf, &len);

  std::string protocolHeader = rpc::common::g_Common_constants.THRIFT_HEADER_MAP.find(this->thriftProtocol_)->second;
  std::ostringstream h;
  h <<
    "POST " << path_ << " HTTP/1.1" << CRLF;
  boost::shared_ptr<LinkedListMultimap> pHeaders = createHeader(buf, len);
  for (LinkedListMultimap::const_iterator iter = pHeaders->begin();
        iter != pHeaders->end(); iter++) {
    h << iter->first << ": " << iter->second.front() << CRLF;
  }
  h <<
    "Host: " << host_ << CRLF <<
    "Connection: " << "keep-alive" << CRLF <<
    "User-Agent: Thrift/" << VERSION << " (C++/THttpClient)" << CRLF <<
    "Content-Length: " << len << CRLF <<
    CRLF;
  string header = h.str();
  if(header.size() > (std::numeric_limits<uint32_t>::max)())
    throw TTransportException("Header too big");
  transport_->write((const uint8_t*)header.c_str(), static_cast<uint32_t>(header.size()));
  transport_->write(buf, len);
  transport_->flush();

  writeBuffer_.resetBuffer();
  readHeaders_ = true;
}

boost::shared_ptr<LinkedListMultimap> EmqHttpClient::createHeader(const uint8_t* buf, uint32_t len) {
  boost::shared_ptr<LinkedListMultimap> pHeaders(new LinkedListMultimap());
  std::string protocolHeader = rpc::common::g_Common_constants.THRIFT_HEADER_MAP.find(this->thriftProtocol_)->second;
  (*pHeaders)["Content-Type"].push_back(protocolHeader);
  (*pHeaders)["Content-MD5"].push_back(getMd5(buf, len));
  string date = DateTimeFormatter::format(DateTime(), "%w, %d %b %Y %H:%M:%S %Z");
  (*pHeaders)["Date"].push_back(date);
  string signature = Signer::SignToBase64("POST", path_, *pHeaders, this->credential_.secretKey, kHmacSha1);
  string authStr = "Galaxy-V2 " + this->credential_.secretKeyId + ":" + signature;
  (*pHeaders)["authorization"].push_back(authStr);
  return pHeaders;
}

std::string EmqHttpClient::getMd5(const uint8_t* buf, uint32_t len) {
  MD5_CTX ctx;
  uint8_t md[16];
  MD5_Init(&ctx);
  MD5_Update(&ctx, buf, len);
  MD5_Final(md, &ctx);
  char tmp[3];
  std::ostringstream md5Stream;
  for (int8_t i = 0; i < 16; i++) {
    sprintf(tmp, "%02x", md[i]);
    tmp[2] = '\0';
    md5Stream << tmp;
  }
  return md5Stream.str();
}

const boost::shared_ptr<TSocket> EmqHttpClient::getTransport() {
  return boost::dynamic_pointer_cast <TSocket> (this->transport_);
}

void EmqHttpClient::setTransport(boost::shared_ptr<TTransport> transport) {
  this->transport_ = transport;
}
}}