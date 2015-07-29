
#include <limits>
#include <cstdlib>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <openssl/md5.h>
#include <openssl/hmac.h>
#include <sys/time.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TJSONProtocol.h>
#include "client/SdsHttpClient.h"
#include "sds/Authentication_constants.h"

using boost::shared_ptr;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

extern const AuthenticationConstants g_Authentication_constants;
extern const CommonConstants g_Common_constants;

SdsHttpClient::SdsHttpClient(boost::shared_ptr<TTransport> transport,
    const std::string& host, const std::string& path, ThriftProtocol::type thriftProtocol,
    const Credential& credential, bool supportAccountKey, int64_t& clockOffset) :
  SdsHttpTransport(transport, clockOffset),
  host_(host),
  path_(path),
  thriftProtocol_(thriftProtocol),
  credential_(credential),
  supportAccountKey_(supportAccountKey){

}


SdsHttpClient::~SdsHttpClient() {}

void SdsHttpClient::parseHeader(char* header) {
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
  } else if (boost::istarts_with(header, g_Authentication_constants.HK_TIMESTAMP)) {
    serverTimestamp_ = atoll(value);
  }
}

bool SdsHttpClient::parseStatusLine(char* status) {
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

void SdsHttpClient::flush() {
  uint8_t* buf;
  uint32_t len;
  writeBuffer_.getBuffer(&buf, &len);

  string authHeader = getAuthenticationHeaders(buf, len);
  std::string protocolHeader = g_Common_constants.THRIFT_HEADER_MAP.find(this->thriftProtocol_)->second;
  std::ostringstream h;
  h <<
    "POST " << path_ << " HTTP/1.1" << CRLF <<
    "Content-Type: " << protocolHeader << CRLF <<
    "Content-Length: " << len << CRLF <<
    "Accept: " << protocolHeader << CRLF <<
    "User-Agent: Thrift/" << VERSION << " (C++/THttpClient)" << CRLF <<
    authHeader <<
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

std::string SdsHttpClient::getAuthenticationHeaders(const uint8_t* buf, uint32_t len) {
  std::ostringstream h;
  std::vector<std::string> headers;
  headers.push_back(g_Authentication_constants.HK_HOST);
  headers.push_back(g_Authentication_constants.HK_TIMESTAMP);
  headers.push_back(g_Authentication_constants.HK_CONTENT_MD5);
  std::string md5 = getMd5(buf, len);
  uint64_t timestamp = time(NULL) + clockOffset_;
  std::string host = this->host_;
  HttpAuthorizationHeader authHeader;
  authHeader.__set_algorithm(MacAlgorithm::HmacSHA1);
  authHeader.__set_userType(this->credential_.type);
  authHeader.__set_secretKeyId(this->credential_.secretKeyId);
  authHeader.__set_signature(this->getSha1Sign(host, timestamp, md5));
  authHeader.__set_supportAccountKey(this->supportAccountKey_);
  authHeader.__set_signedHeaders(headers);
  boost::shared_ptr<TMemoryBuffer> mb(new TMemoryBuffer());
  TJSONProtocol protocol(mb);
  authHeader.write(&protocol);
  std::string jsonHeader = mb->getBufferAsString();
  h <<
    g_Authentication_constants.HK_HOST << ": "<< host_ << CRLF <<
    g_Authentication_constants.HK_TIMESTAMP << ": "<< timestamp << CRLF <<
    g_Authentication_constants.HK_CONTENT_MD5 << ": "<< md5 << CRLF <<
    g_Authentication_constants.HK_AUTHORIZATION << ": "<< jsonHeader << CRLF;
  return h.str();
}

std::string SdsHttpClient::getMd5(const uint8_t* buf, uint32_t len) {
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

std::string SdsHttpClient::getSha1Sign(const std::string &host, const uint64_t &timestamp, const std::string &md5) {
  std::ostringstream h;
  h << host << '\n' <<
    timestamp << '\n' <<
    md5;
  std::string signData = h.str();
  return sha1(signData);
}

std::string SdsHttpClient::sha1(const std::string& data) {
  uint8_t* digest;
  digest = HMAC(EVP_sha1(), credential_.secretKey.c_str(), credential_.secretKey.length(),
    (uint8_t*)data.c_str(), data.length(), NULL, NULL);
  std::ostringstream sha1Stream;
  char tmp[3];
  for (int8_t i = 0; i < 20; i++) {
    sprintf(tmp, "%02x", (uint32_t)digest[i]);
    tmp[2] = '\0';
    sha1Stream << tmp;
  }
  return sha1Stream.str();
}
