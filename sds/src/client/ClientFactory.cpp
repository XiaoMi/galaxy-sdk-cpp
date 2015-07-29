
#include <limits>
#include <cstdlib>
#include <sstream>
#include <unistd.h>
#include <stdexcept>
#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_io.hpp>


#include "client/ClientFactory.h"
#include "client/SdsException.h"
#include "client/SdsHttpClient.h"
#include "client/SdsSSLSocket.h"
#include "sds/Errors_types.h"
#include "sds/Common_constants.h"
#include "sds/Errors_constants.h"

using boost::shared_ptr;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;


extern const ErrorsConstants g_Errors_constants;
extern const CommonConstants g_Common_constants;

boost::shared_ptr<TProtocol> getProtocol(ThriftProtocol::type thriftProtocol,
  boost::shared_ptr<TTransport> transport) {
   boost::shared_ptr<TProtocol> protocol;
  switch (thriftProtocol) {
    case ThriftProtocol::TCOMPACT:
      return boost::shared_ptr<TProtocol> (new TCompactProtocol(transport));
    case ThriftProtocol::TBINARY:
      return boost::shared_ptr<TProtocol> (new TBinaryProtocol(transport));
    case ThriftProtocol::TJSON:
      return boost::shared_ptr<TProtocol> (new TJSONProtocol(transport));
    default:
      throw std::invalid_argument("Unexpected protocol type");
  }
}

int64_t backOffTime(ErrorCode::type errorCode) {
  if (g_Errors_constants.ERROR_BACKOFF.find(errorCode) == g_Errors_constants.ERROR_BACKOFF.end()) {
    return -1;
  }
  return g_Errors_constants.ERROR_BACKOFF.find(errorCode)->second;
}

bool shouldRetry(ErrorCode::type errorCode, int32_t retry) {
  int64_t sleepMs = backOffTime(errorCode);
  if (sleepMs >= 0 && retry < g_Errors_constants.MAX_RETRY) {
    usleep(sleepMs * 1000);
    return true;
  }
  return false;
}

std::string getRequestId() {
  boost::uuids::basic_random_generator<boost::mt19937> gen;
  boost::uuids::uuid u = gen();
  return boost::lexical_cast<std::string>(u).substr(0, 8);
}

std::string getQueryString(const std::string& type) {
  return "?id=" + getRequestId() + "&type=" +type;
}

void parseUrl(const std::string& url, std::string& scheme, std::string& host, std::string& path, int32_t& port) {
  std::size_t pos1, pos2;
  pos1 = url.find("://");
  if (pos1 == std::string::npos) {
    throw std::invalid_argument(std::string("Invalid url: ") + url);
  }
  scheme = url.substr(0, pos1);
  if (scheme != "http" && scheme != "https") {
    throw std::invalid_argument(std::string("Unknown scheme: ") + scheme + " only support http or https");
  }
  std::string less = url.substr(pos1 + 3);
  pos1 = less.find(":");
  pos2 = less.find("/");
  if (pos1 != std::string::npos) {
    host = less.substr(0, pos1);
  } else {
    if (pos2 == std::string::npos) {
      host = less;
    } else {
      host = less.substr(0, pos2);
    }
  }
  if (pos1 == std::string::npos) {
    if (scheme == "http") {
      port = 80;
    } else {
      port = 443;
    }
  } else {
    if (pos2 == std::string::npos) {
      port = atoi(less.substr(pos1 + 1).c_str());
    } else {
      port = atoi(less.substr(pos1 + 1, pos2).c_str());
    }
  }
  if (pos2 == std::string::npos) {
    path = "/";
  } else {
    path = less.substr(pos2);
  }
}

ClientFactory::ClientFactory(const Credential& credential, ThriftProtocol::type thriftProtocol) :
  credential_(credential),
  thriftProtocol_(thriftProtocol) {
}

ClientFactory::~ClientFactory(){}

TableClientProxy ClientFactory::newDefaultTableClient(bool supportAccountKey) {
  std::string url = g_Common_constants.DEFAULT_SERVICE_ENDPOINT + g_Common_constants.TABLE_SERVICE_PATH;
  return newTableClient(url, supportAccountKey);
}


TableClientProxy ClientFactory::newTableClient(const std::string& url, bool supportAccountKey) {
  std::string scheme;
  std::string host;
  std::string path;
  int32_t port;
  parseUrl(url, scheme, host, path, port);
  bool isHttps = false;
  if (scheme == "https") {
    isHttps = true;
  }
  return TableClientProxy(this->credential_, host, port, path, isHttps, supportAccountKey,
    this->thriftProtocol_);
}

AdminClientProxy ClientFactory::newDefaultAdminClient(bool supportAccountKey) {
  std::string url = g_Common_constants.DEFAULT_SERVICE_ENDPOINT + g_Common_constants.ADMIN_SERVICE_PATH;
  return newAdminClient(url, supportAccountKey);
}

AdminClientProxy ClientFactory::newAdminClient(const std::string& url, bool supportAccountKey) {
  std::string scheme;
  std::string host;
  std::string path;
  int32_t port;
  parseUrl(url, scheme, host, path, port);
  bool isHttps = false;
  if (scheme == "https") {
    isHttps = true;
  }
  return AdminClientProxy(this->credential_, host, port, path, isHttps, supportAccountKey,
    this->thriftProtocol_);
}

AuthClientProxy ClientFactory::newDefaultAuthClient(bool supportAccountKey) {
  std::string url = g_Common_constants.DEFAULT_SERVICE_ENDPOINT + g_Common_constants.AUTH_SERVICE_PATH;
  return newAuthClient(url, supportAccountKey);
}

AuthClientProxy ClientFactory::newAuthClient(const std::string& url, bool supportAccountKey) {
  std::string scheme;
  std::string host;
  std::string path;
  int32_t port;
  parseUrl(url, scheme, host, path, port);
  bool isHttps = false;
  if (scheme == "https") {
    isHttps = true;
  }
  return AuthClientProxy(this->credential_, host, port, path, isHttps, supportAccountKey,
    this->thriftProtocol_);
}

/**
 * TableClientProxy
 */

TableClientProxy::TableClientProxy(const Credential& credential, const std::string& host,
  int32_t port, const std::string& path, bool isHttps, bool supportAccountKey, ThriftProtocol::type thriftProtocol) :
  credential_(credential),
  host_(host),
  port_(port),
  path_(path),
  isHttps_(isHttps),
  supportAccountKey_(supportAccountKey),
  thriftProtocol_(thriftProtocol){
}

TableClientProxy::~TableClientProxy(){}

void TableClientProxy::getServerVersion(Version& _return) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getServerVersion"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.getServerVersion(_return);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::validateClientVersion(const Version& clientVersion) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("validateClientVersion"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.validateClientVersion(clientVersion);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

int64_t TableClientProxy::getServerTime() {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getServerTime"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      int64_t time = tableClient.getServerTime();
      transport->close();
      return time;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::get(GetResult& _return, const GetRequest& request) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("get"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.get(_return, request);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::put(PutResult& _return, const PutRequest& request) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("put"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.put(_return, request);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::increment(IncrementResult& _return, const IncrementRequest& request) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("increment"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.increment(_return, request);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::remove(RemoveResult& _return, const RemoveRequest& request) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("remove"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.remove(_return, request);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::scan(ScanResult& _return, const ScanRequest& request) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("scan"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.scan(_return, request);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void TableClientProxy::batch(BatchResult& _return, const BatchRequest& request) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("batch"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      TableServiceClient tableClient(protocol);
      transport->open();
      tableClient.batch(_return, request);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

/**
 * AdminClientProxy
 */

AdminClientProxy::AdminClientProxy(const Credential& credential, const std::string& host, int32_t port,
  const std::string& path, bool isHttps, bool supportAccountKey, ThriftProtocol::type thriftProtocol):
  credential_(credential),
  host_(host),
  port_(port),
  path_(path),
  isHttps_(isHttps),
  supportAccountKey_(supportAccountKey),
  thriftProtocol_(thriftProtocol){
}

AdminClientProxy::~AdminClientProxy(){}

void AdminClientProxy::getServerVersion(Version& _return) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getServerVersion"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.getServerVersion(_return);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::validateClientVersion(const Version& clientVersion) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("validateClientVersion"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.validateClientVersion(clientVersion);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

int64_t AdminClientProxy::getServerTime() {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getServerTime"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      int64_t time = adminClient.getServerTime();
      transport->close();
      return time;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::saveAppInfo(const AppInfo& appInfo) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("saveAppInfo"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.saveAppInfo(appInfo);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::getAppInfo(AppInfo& _return, const std::string& appId) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getAppInfo"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.getAppInfo(_return, appId);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::findAllApps(std::vector<AppInfo> & _return) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("findAllApps"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.findAllApps(_return);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::findAllTables(std::vector<TableInfo> & _return) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("findAllTables"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.findAllTables(_return);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::createTable(TableInfo& _return, const std::string& tableName, const TableSpec& tableSpec) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("createTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.createTable(_return, tableName, tableSpec);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::dropTable(const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("dropTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.dropTable(tableName);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::lazyDropTable(const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("lazyDropTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.lazyDropTable(tableName);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::alterTable(const std::string& tableName, const TableSpec& tableSpec) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("alterTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.alterTable(tableName, tableSpec);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::cloneTable(const std::string& srcName, const std::string& destTable, const bool flushTable) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("cloneTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.cloneTable(srcName, destTable, flushTable);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::disableTable(const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("disableTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.disableTable(tableName);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::enableTable(const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("enableTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.enableTable(tableName);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::describeTable(TableSpec& _return, const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("describeTable"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.describeTable(_return, tableName);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::getTableStatus(TableStatus& _return, const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getTableStatus"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.getTableStatus(_return, tableName);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

TableState::type AdminClientProxy::getTableState(const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getTableState"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      TableState::type stat = adminClient.getTableState(tableName);
      transport->close();
      return stat;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

int64_t AdminClientProxy::getTableSize(const std::string& tableName) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getTableSize"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      int64_t size = adminClient.getTableSize(tableName);
      transport->close();
      return size;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::getTableSplits(std::vector<TableSplit> & _return, const std::string& tableName, const Dictionary& startKey, const Dictionary& stopKey) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getTableSplits"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.getTableSplits(_return, tableName, startKey, stopKey);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::queryMetric(TimeSeriesData& _return, const MetricQueryRequest& query) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("queryMetric"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.queryMetric(_return, query);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::queryMetrics(std::vector<TimeSeriesData> & _return, const std::vector<MetricQueryRequest> & queries) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("queryMetrics"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.queryMetrics(_return, queries);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::findAllAppInfo(std::vector<AppInfo> & _return) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("findAllAppInfo"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.findAllAppInfo(_return);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AdminClientProxy::putClientMetrics(const ClientMetrics& clientMetrics) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("putClientMetrics"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AdminServiceClient adminClient(protocol);
      transport->open();
      adminClient.putClientMetrics(clientMetrics);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

/**
 * AuthClientProxy
 */

AuthClientProxy::AuthClientProxy(const Credential& credential, const std::string& host,
  int32_t port, const std::string& path, bool isHttps, bool supportAccountKey, ThriftProtocol::type thriftProtocol) :
  credential_(credential),
  host_(host),
  port_(port),
  path_(path),
  isHttps_(isHttps),
  supportAccountKey_(supportAccountKey),
  thriftProtocol_(thriftProtocol){
}

AuthClientProxy::~AuthClientProxy(){}

void AuthClientProxy::getServerVersion(Version& _return) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getServerVersion"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AuthServiceClient authClient(protocol);
      transport->open();
      authClient.getServerVersion(_return);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AuthClientProxy::validateClientVersion(const Version& clientVersion) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("validateClientVersion"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AuthServiceClient authClient(protocol);
      transport->open();
      authClient.validateClientVersion(clientVersion);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

int64_t AuthClientProxy::getServerTime() {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("getServerTime"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AuthServiceClient authClient(protocol);
      transport->open();
      int64_t time = authClient.getServerTime();
      transport->close();
      return time;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}

void AuthClientProxy::createCredential(Credential& _return, const OAuthInfo& oauthInfo) {
  int32_t retry = 0;
  int64_t clockOffset = 0;
  while(true) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps_) {
      boost::shared_ptr<SdsSSLSocketFactory> factory(new SdsSSLSocketFactory());
      socket = factory->createSocket(host_, port_);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host_, port_));
    }
    boost::shared_ptr<TTransport> transport(new SdsHttpClient(socket, host_, path_ + getQueryString("createCredential"), thriftProtocol_,
      credential_, supportAccountKey_, clockOffset));
    boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
    ErrorCode::type errorCode;
    try {
      AuthServiceClient authClient(protocol);
      transport->open();
      authClient.createCredential(_return, oauthInfo);
      transport->close();
      return;
    } catch (const SdsException& sdse) {
      transport->close();
      errorCode = sdse.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw sdse;
      }
    } catch (const ServiceException& se) {
      transport->close();
      errorCode = se.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        throw SdsException(se.errorCode, se.errorMessage, se.details, se.callId, se.requestId);
      }
    }
  }
}
