
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
#include "client/EmqException.h"
#include "client/EmqHttpClient.h"
#include "client/EmqSSLSocket.h"
#include "emq/Common_types.h"
#include "emq/Common_constants.h"
#include "emq/Constants_constants.h"

using namespace std;
using boost::shared_ptr;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace emq::common;
using namespace emq::constants;
using namespace emq::common;

namespace galaxy { namespace emq {
extern const ::emq::common::CommonConstants g_Common_constants;
extern const ::emq::constants::ConstantsConstants g_Constants_constants;


boost::shared_ptr<TProtocol> getProtocol(::rpc::common::ThriftProtocol::type thriftProtocol,
  boost::shared_ptr<TTransport> transport) {
   boost::shared_ptr<TProtocol> protocol;
  switch (thriftProtocol) {
    case rpc::common::ThriftProtocol::TCOMPACT:
      return boost::shared_ptr<TProtocol> (new TCompactProtocol(transport));
    case rpc::common::ThriftProtocol::TBINARY:
      return boost::shared_ptr<TProtocol> (new TBinaryProtocol(transport));
    case rpc::common::ThriftProtocol::TJSON:
      return boost::shared_ptr<TProtocol> (new TJSONProtocol(transport));
    default:
      throw std::invalid_argument("Unexpected protocol type");
  }
}

int64_t backOffTime(::emq::common::ErrorCode::type errorCode) {
  if (::emq::common::g_Common_constants.ERROR_BACKOFF.find(errorCode) == ::emq::common::g_Common_constants.ERROR_BACKOFF.end()) {
    return -1;
  }
  return ::emq::common::g_Common_constants.ERROR_BACKOFF.find(errorCode)->second;
}

bool shouldRetry(::emq::common::ErrorCode::type errorCode, int32_t retry) {
  int64_t sleepMs = backOffTime(errorCode);
  if (sleepMs >= 0 && retry < ::emq::common::g_Common_constants.MAX_RETRY) {
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

ClientFactory::ClientFactory(const rpc::auth::Credential& credential, rpc::common::ThriftProtocol::type thriftProtocol) :
  credential_(credential),
  thriftProtocol_(thriftProtocol) {
}

ClientFactory::~ClientFactory(){}

QueueClientProxy ClientFactory::newQueueClient(const std::string& endpoint) {
  return newQueueClient(endpoint, ::emq::constants::g_Constants_constants.DEFAULT_CLIENT_CONN_TIMEOUT,
    ::emq::constants::g_Constants_constants.DEFAULT_CLIENT_SOCKET_TIMEOUT,
    ::emq::constants::g_Constants_constants.DEFAULT_CLIENT_SOCKET_TIMEOUT, 1);
}

QueueClientProxy ClientFactory::newQueueClient(const std::string& endpoint, int connectionTimeout,
  int sendTimeout, int receiveTimeout, int maxPoolSize) {
  std::string url = endpoint + ::emq::constants::g_Constants_constants.QUEUE_SERVICE_PATH;
  std::string scheme;
  std::string host;
  std::string path;
  int32_t port;
  parseUrl(url, scheme, host, path, port);
  bool isHttps = false;
  if (scheme == "https") {
    isHttps = true;
  }
  return QueueClientProxy(this->credential_, this->thriftProtocol_,
    ConnectionPool(host, port, path, this->thriftProtocol_, this->credential_, connectionTimeout,
    sendTimeout, receiveTimeout, maxPoolSize, isHttps));
}


MessageClientProxy ClientFactory::newMessageClient(const std::string& endpoint) {
  return newMessageClient(endpoint, ::emq::constants::g_Constants_constants.DEFAULT_CLIENT_CONN_TIMEOUT,
    ::emq::constants::g_Constants_constants.DEFAULT_CLIENT_SOCKET_TIMEOUT,
    ::emq::constants::g_Constants_constants.DEFAULT_CLIENT_SOCKET_TIMEOUT, 1);
}

MessageClientProxy ClientFactory::newMessageClient(const std::string& endpoint, int connectionTimeout,
  int sendTimeout, int receiveTimeout, int maxPoolSize) {
  std::string url = endpoint + ::emq::constants::g_Constants_constants.MESSAGE_SERVICE_PATH;
  std::string scheme;
  std::string host;
  std::string path;
  int32_t port;
  parseUrl(url, scheme, host, path, port);
  bool isHttps = false;
  if (scheme == "https") {
    isHttps = true;
  }
  return MessageClientProxy(this->credential_, this->thriftProtocol_,
    ConnectionPool(host, port, path, this->thriftProtocol_, this->credential_, connectionTimeout,
    sendTimeout, receiveTimeout, maxPoolSize, isHttps));
}

/**
 * QueueClientProxy
 */

QueueClientProxy::QueueClientProxy(const rpc::auth::Credential& credential,
  rpc::common::ThriftProtocol::type thriftProtocol, const ConnectionPool& pool) :
  credential_(credential),
  thriftProtocol_(thriftProtocol),
  pool_(pool){
}


void QueueClientProxy::getServiceVersion(::emq::common::Version& _return) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("getServiceVersion"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.getServiceVersion(_return);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("getServiceVersion"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::validClientVersion(const ::emq::common::Version& clientVersion) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("validClientVersion"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.validClientVersion(clientVersion);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("validClientVersion"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::createQueue(CreateQueueResponse& _return, const CreateQueueRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("createQueue"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.createQueue(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("createQueue"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::deleteQueue(const DeleteQueueRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deleteQueue"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.deleteQueue(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deleteQueue"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::purgeQueue(const PurgeQueueRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("purgeQueue"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.purgeQueue(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("purgeQueue"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}


void QueueClientProxy::setQueueAttribute(SetQueueAttributesResponse& _return,
  const SetQueueAttributesRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("setQueueAttribute"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.setQueueAttribute(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("setQueueAttribute"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::setQueueQuota(SetQueueQuotaResponse& _return, const SetQueueQuotaRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("setQueueQuota"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.setQueueQuota(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("setQueueQuota"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::getQueueInfo(GetQueueInfoResponse& _return, const GetQueueInfoRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("getQueueInfo"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.getQueueInfo(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("getQueueInfo"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::listQueue(ListQueueResponse& _return, const ListQueueRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("listQueue"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.listQueue(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("listQueue"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::setQueueRedrivePolicy(SetQueueRedrivePolicyResponse& _return, const SetQueueRedrivePolicyRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("setQueueRedrivePolicy"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.setQueueRedrivePolicy(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("setQueueRedrivePolicy"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::removeQueueRedrivePolicy(const RemoveQueueRedrivePolicyRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("removeQueueRedrivePolicy"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.removeQueueRedrivePolicy(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("removeQueueRedrivePolicy"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::setPermission(const SetPermissionRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("setPermission"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.setPermission(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("setPermission"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::revokePermission(const RevokePermissionRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("revokePermission"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.revokePermission(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("revokePermission"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::queryPermission(QueryPermissionResponse& _return, const QueryPermissionRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("queryPermission"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.queryPermission(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("queryPermission"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::queryPermissionForId(QueryPermissionForIdResponse& _return, const QueryPermissionForIdRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("queryPermissionForId"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.queryPermissionForId(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("queryPermissionForId"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::listPermissions(ListPermissionsResponse& _return, const ListPermissionsRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("listPermissions"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.listPermissions(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("listPermissions"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::createTag(CreateTagResponse& _return, const CreateTagRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("createTag"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.createTag(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("createTag"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::deleteTag(const DeleteTagRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deleteTag"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.deleteTag(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deleteTag"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::getTagInfo(GetTagInfoResponse& _return, const GetTagInfoRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("getTagInfo"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.getTagInfo(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("getTagInfo"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::listTag(ListTagResponse& _return, const ListTagRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("listTag"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.listTag(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("listTag"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::queryMetric(TimeSeriesData& _return, const QueryMetricRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("queryMetric"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.queryMetric(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("queryMetric"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::queryPrivilegedQueue(QueryPrivilegedQueueResponse& _return, const QueryPrivilegedQueueRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("queryPrivilegedQueue"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.queryPrivilegedQueue(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("queryPrivilegedQueue"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::verifyEMQAdmin(VerifyEMQAdminResponse& _return) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("verifyEMQAdmin"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.verifyEMQAdmin(_return);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("verifyEMQAdmin"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::verifyEMQAdminRole(VerifyEMQAdminRoleResponse& _return, const VerifyEMQAdminRoleRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("verifyEMQAdminRole"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.verifyEMQAdminRole(_return, request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("verifyEMQAdminRole"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::copyQueue(const CopyQueueRequest& request) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("copyQueue"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.copyQueue(request);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("copyQueue"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void QueueClientProxy::getQueueMeta(GetQueueMetaResponse& _return, const std::string& queueName) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("getQueueMeta"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      QueueServiceClient queueClient(protocol);
      queueClient.getQueueMeta(_return, queueName);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("getQueueMeta"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

/**
 * MessageClientProxy
 */

MessageClientProxy::MessageClientProxy(const rpc::auth::Credential& credential, rpc::common::ThriftProtocol::type thriftProtocol,
  const ConnectionPool& pool):
  credential_(credential),
  thriftProtocol_(thriftProtocol),
  pool_(pool){
}

void MessageClientProxy::getServiceVersion(::emq::common::Version& _return) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("getServiceVersion"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.getServiceVersion(_return);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("getServiceVersion"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void MessageClientProxy::validClientVersion(const ::emq::common::Version& clientVersion) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("validClientVersion"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.validClientVersion(clientVersion);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException &te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("validClientVersion"));
        retry++;
      } else {
        pool_.retConnection(transport);
        throw te;
      }
    }
  }
}

void MessageClientProxy::sendMessage(SendMessageResponse& _return, const SendMessageRequest& sendMessageRequest) {
  int32_t retry = 0;
  std::string query = getQueryString("sendMessage");
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(query);
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.sendMessage(_return, sendMessageRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("sendMessage"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::sendMessageBatch(SendMessageBatchResponse& _return, const SendMessageBatchRequest& sendMessageBatchRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("sendMessageBatch"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.sendMessageBatch(_return, sendMessageBatchRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("sendMessageBatch"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::receiveMessage(std::vector<ReceiveMessageResponse> & _return, const ReceiveMessageRequest& receiveMessageRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("receiveMessage"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.receiveMessage(_return, receiveMessageRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("receiveMessage"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::changeMessageVisibilitySeconds(const ChangeMessageVisibilityRequest& changeMessageVisibilityRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("changeMessageVisibilitySeconds"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.changeMessageVisibilitySeconds(changeMessageVisibilityRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("changeMessageVisibilitySeconds"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::changeMessageVisibilitySecondsBatch(ChangeMessageVisibilityBatchResponse& _return, const ChangeMessageVisibilityBatchRequest& changeMessageVisibilityBatchRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("changeMessageVisibilitySecondsBatch"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.changeMessageVisibilitySecondsBatch(_return, changeMessageVisibilityBatchRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("changeMessageVisibilitySecondsBatch"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::deleteMessage(const DeleteMessageRequest& deleteMessageRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deleteMessage"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.deleteMessage(deleteMessageRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deleteMessage"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::deleteMessageBatch(DeleteMessageBatchResponse& _return, const DeleteMessageBatchRequest& deleteMessageBatchRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deleteMessageBatch"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.deleteMessageBatch(_return, deleteMessageBatchRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deleteMessageBatch"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::deadMessage(const DeadMessageRequest& deadMessageRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deadMessage"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.deadMessage(deadMessageRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deadMessage"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::deadMessageBatch(DeadMessageBatchResponse& _return, const DeadMessageBatchRequest& deadMessageBatchRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deadMessageBatch"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.deadMessageBatch(_return, deadMessageBatchRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deadMessageBatch"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::peekMessage(std::vector<PeekMessageResponse> & _return, const PeekMessageRequest& peekMessageRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("peekMessage"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.peekMessage(_return, peekMessageRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("peekMessage"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::deletePeekMessage(const DeletePeekMessageRequest& deletePeekMessageRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deletePeekMessage"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.deletePeekMessage(deletePeekMessageRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deletePeekMessage"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}

void MessageClientProxy::deletePeekMessageBatch(DeletePeekMessageBatchResponse& _return, const DeletePeekMessageBatchRequest& deletePeekMessageBatchRequest) {
  int32_t retry = 0;
  boost::shared_ptr<EmqHttpClient> transport = pool_.getConnection(getQueryString("deletePeekMessageBatch"));
  while(true) {
    try {
      boost::shared_ptr<TProtocol> protocol = getProtocol(thriftProtocol_, transport);
      MessageServiceClient messageClient(protocol);
      messageClient.deletePeekMessageBatch(_return, deletePeekMessageBatchRequest);
      pool_.retConnection(transport);
      return;
    } catch (const TTransportException& te) {
      pool_.closeConnection(transport);
      if (retry < 1) {
        transport = pool_.newConnection(getQueryString("deletePeekMessageBatch"));
        retry++;
      } else {
        throw te;
      }
    } catch (const EmqException& emqe) {
      ::emq::common::ErrorCode::type errorCode = emqe.getErrorCode();
      if (shouldRetry(errorCode, retry)) {
        retry++;
      } else {
        pool_.retConnection(transport);
        throw emqe;
      }
    } catch (const ::emq::common::GalaxyEmqServiceException& gese) {
      ::emq::common::ErrorCode::type errorCode = (::emq::common::ErrorCode::type) gese.errorCode;
      if (shouldRetry(errorCode, retry)) {
        retry++;
      }
      else {
        pool_.retConnection(transport);
        throw EmqException(errorCode, gese.errMsg, gese.details,
          gese.requestId, gese.queueName);
      }
    }
  }
}
}}
