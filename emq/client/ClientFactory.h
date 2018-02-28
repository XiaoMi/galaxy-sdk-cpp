#ifndef _EMQ_CLIENTFACTORY_H_
#define _EMQ_CLIENTFACTORY_H_

#include "emq/MessageService.h"
#include "emq/Message_types.h"
#include "emq/EMQBaseService.h"
#include "emq/QueueService.h"
#include "rpc/Authentication_types.h"
#include "emq/Common_types.h"
#include "client/ConnectionPool.h"

using namespace emq::queue;
using namespace emq::message;

namespace galaxy { namespace emq {
class QueueClientProxy;
class MessageClientProxy;

class ClientFactory {
  public:
    ClientFactory(const rpc::auth::Credential& credential, ::rpc::common::ThriftProtocol::type thriftProtocol
      = rpc::common::ThriftProtocol::TBINARY);
    virtual ~ClientFactory();
    QueueClientProxy newQueueClient(const std::string& endpoint);
    QueueClientProxy newQueueClient(const std::string& endpoint, int connectionTimeout, int sendTimeout, int receiveTimeout, int maxPoolSize);
    MessageClientProxy newMessageClient(const std::string& endpoint);
    MessageClientProxy newMessageClient(const std::string& endpoint, int connectionTimeout, int sendTimeout, int receiveTimeout, int maxPoolSize);

  private:
    rpc::auth::Credential credential_;
    rpc::common::ThriftProtocol::type thriftProtocol_;
};

class QueueClientProxy : public ::emq::queue::QueueServiceIf,  virtual public ::emq::common::EMQBaseServiceIf {

  public:
    QueueClientProxy(const rpc::auth::Credential& credential, rpc::common::ThriftProtocol::type thriftProtocol, const ConnectionPool& pool);
    void getServiceVersion(::emq::common::Version& _return);
    void validClientVersion(const ::emq::common::Version& clientVersion);
    void createQueue(CreateQueueResponse& _return, const CreateQueueRequest& request);
    void deleteQueue(const DeleteQueueRequest& request);
    void purgeQueue(const PurgeQueueRequest& request);
    void setQueueAttribute(SetQueueAttributesResponse& _return, const SetQueueAttributesRequest& request);
    void setQueueQuota(SetQueueQuotaResponse& _return, const SetQueueQuotaRequest& request);
    void getQueueInfo(GetQueueInfoResponse& _return, const GetQueueInfoRequest& request);
    void listQueue(ListQueueResponse& _return, const ListQueueRequest& request);
    void setQueueRedrivePolicy(SetQueueRedrivePolicyResponse& _return, const SetQueueRedrivePolicyRequest& request);
    void removeQueueRedrivePolicy(const RemoveQueueRedrivePolicyRequest& request);
    void setPermission(const SetPermissionRequest& request);
    void revokePermission(const RevokePermissionRequest& request);
    void queryPermission(QueryPermissionResponse& _return, const QueryPermissionRequest& request);
    void queryPermissionForId(QueryPermissionForIdResponse& _return, const QueryPermissionForIdRequest& request);
    void listPermissions(ListPermissionsResponse& _return, const ListPermissionsRequest& request);
    void createTag(CreateTagResponse& _return, const CreateTagRequest& request);
    void deleteTag(const DeleteTagRequest& request);
    void getTagInfo(GetTagInfoResponse& _return, const GetTagInfoRequest& request);
    void listTag(ListTagResponse& _return, const ListTagRequest& request);
    void queryMetric(TimeSeriesData& _return, const QueryMetricRequest& request);
    void queryPrivilegedQueue(QueryPrivilegedQueueResponse& _return, const QueryPrivilegedQueueRequest& request);
    void verifyEMQAdmin(VerifyEMQAdminResponse& _return);
    void verifyEMQAdminRole(VerifyEMQAdminRoleResponse& _return, const VerifyEMQAdminRoleRequest& request);
    void copyQueue(const CopyQueueRequest& request);
    void getQueueMeta(GetQueueMetaResponse& _return, const std::string& queueName);
//    virtual ~QueueClientProxy();

  private:
    rpc::auth::Credential credential_;
    rpc::common::ThriftProtocol::type thriftProtocol_;
    ConnectionPool pool_;
};

class MessageClientProxy : public ::emq::message::MessageServiceIf, virtual public ::emq::common::EMQBaseServiceIf {
  public:
    MessageClientProxy(const rpc::auth::Credential& credential,
      rpc::common::ThriftProtocol::type thriftProtocol, const ConnectionPool& pool);
    void getServiceVersion(::emq::common::Version& _return);
    void validClientVersion(const ::emq::common::Version& clientVersion);
    void sendMessage(SendMessageResponse& _return, const SendMessageRequest& sendMessageRequest);
    void sendMessageBatch(SendMessageBatchResponse& _return, const SendMessageBatchRequest& sendMessageBatchRequest);
    void receiveMessage(std::vector<ReceiveMessageResponse> & _return, const ReceiveMessageRequest& receiveMessageRequest);
    void changeMessageVisibilitySeconds(const ChangeMessageVisibilityRequest& changeMessageVisibilityRequest);
    void changeMessageVisibilitySecondsBatch(ChangeMessageVisibilityBatchResponse& _return, const ChangeMessageVisibilityBatchRequest& changeMessageVisibilityBatchRequest);
    void deleteMessage(const DeleteMessageRequest& deleteMessageRequest);
    void deleteMessageBatch(DeleteMessageBatchResponse& _return, const DeleteMessageBatchRequest& deleteMessageBatchRequest);
    void deadMessage(const DeadMessageRequest& deadMessageRequest);
    void deadMessageBatch(DeadMessageBatchResponse& _return, const DeadMessageBatchRequest& deadMessageBatchRequest);
    void peekMessage(std::vector<PeekMessageResponse> & _return, const PeekMessageRequest& peekMessageRequest);
    void deletePeekMessage(const DeletePeekMessageRequest& deletePeekMessageRequest);
    void deletePeekMessageBatch(DeletePeekMessageBatchResponse& _return, const DeletePeekMessageBatchRequest& deletePeekMessageBatchRequest);
//    virtual ~MessageClientProxy();

  private:
    rpc::auth::Credential credential_;
    rpc::common::ThriftProtocol::type thriftProtocol_;
    ConnectionPool pool_;
};
#endif
}}