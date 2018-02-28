#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include "../rpc/Authentication_types.h"
#include "../client/ClientFactory.h"

using namespace std;
using namespace emq::queue;
using namespace emq::message;
using namespace galaxy::emq;


int main(int argc, char *argv[]) {
  rpc::auth::UserType::type userType = rpc::auth::UserType::APP_SECRET;
  std::string appKey = ""; //Your appKey
  std::string appSecret = ""; //Your appSecret
  std::string endpoint = ""; //Set endpoint
  std::string queueName = "testCppExampleQueue";
  int readQps = 100;
  int writeQps = 100;
  rpc::auth::Credential credential;
  credential.__set_type(userType);
  credential.__set_secretKeyId(appKey);
  credential.__set_secretKey(appSecret);
  ClientFactory clientFactory(credential);
  QueueClientProxy queueClient = clientFactory.newQueueClient(endpoint);
  MessageClientProxy messageClient = clientFactory.newMessageClient(endpoint);

  // create queue
  CreateQueueRequest createQueueRequest;
  QueueQuota queueQuota;
  QueueAttribute queueAttribute;
  Throughput throughput;
  throughput.__set_readQps(readQps);
  throughput.__set_writeQps(writeQps);
  queueQuota.__set_throughput(throughput);
  createQueueRequest.__set_queueAttribute(queueAttribute);
  createQueueRequest.__set_queueQuota(queueQuota);
  createQueueRequest.__set_queueName(queueName);
  CreateQueueResponse createQueueResponse;
  queueClient.createQueue(createQueueResponse, createQueueRequest);
  queueName = createQueueResponse.queueName;

// create tag
  std::string tagName = "tagTest";
  CreateTagRequest createTagRequest;
  createTagRequest.__set_queueName(queueName);
  createTagRequest.__set_tagName(tagName);
  CreateTagResponse createTagResponse;
  queueClient.createTag(createTagResponse, createTagRequest);

// send message
  std::string messageBody = "test message body";
  SendMessageRequest sendMessageRequest;
  sendMessageRequest.__set_queueName(queueName);
  sendMessageRequest.__set_messageBody(messageBody);
  SendMessageResponse sendMessageResponse;
  messageClient.sendMessage(sendMessageResponse, sendMessageRequest);
  cout << "Message Id: " << sendMessageResponse.messageID << std::endl;
  cout << "Message Body MD5: " << sendMessageResponse.bodyMd5 << std::endl;

// receive message
  ReceiveMessageRequest receiveMessageRequest;
  receiveMessageRequest.__set_queueName(queueName);
  receiveMessageRequest.__set_maxReceiveMessageNumber(3);
  std::vector<ReceiveMessageResponse> receiveMessageResponse;
  while (true) {
    messageClient.receiveMessage(receiveMessageResponse, receiveMessageRequest);
    if (receiveMessageResponse.size() > 0) {
      break;
    }
  }
  std::vector<DeleteMessageBatchRequestEntry> entryList;
  for (int i = 0; i < receiveMessageResponse.size(); i++) {
    cout << "Message Id: " << receiveMessageResponse[i].messageID << std::endl;
    cout << "Message Body: " << receiveMessageResponse[i].messageBody << std::endl;
    cout << "Message ReceiptHandle: " << receiveMessageResponse[i].receiptHandle << std::endl;
    DeleteMessageBatchRequestEntry entry;
    entry.__set_receiptHandle(receiveMessageResponse[i].receiptHandle);
    entryList.push_back(entry);
  }

//delete massage
  DeleteMessageBatchRequest deleteMessageBatchRequest;
  deleteMessageBatchRequest.__set_queueName(queueName);
  deleteMessageBatchRequest.__set_deleteMessageBatchRequestEntryList(entryList);
  DeleteMessageBatchResponse deleteMessageBatchResponse;
  messageClient.deleteMessageBatch(deleteMessageBatchResponse, deleteMessageBatchRequest);

// receive from queue tag
  receiveMessageRequest.__set_queueName(queueName);
  receiveMessageRequest.__set_maxReceiveMessageNumber(3);
  receiveMessageRequest.__set_tagName(tagName);
  receiveMessageResponse.clear();
  entryList.clear();
  while (true) {
    messageClient.receiveMessage(receiveMessageResponse, receiveMessageRequest);
    if (receiveMessageResponse.size() > 0) {
      break;
    }
  }

  for (int i = 0; i < receiveMessageResponse.size(); i++) {
    cout << "Message Id: " << receiveMessageResponse[i].messageID << std::endl;
    cout << "Message Body: " << receiveMessageResponse[i].messageBody << std::endl;
    cout << "Message ReceiptHandle: " << receiveMessageResponse[i].receiptHandle << std::endl;
    DeleteMessageBatchRequestEntry entry;
    entry.__set_receiptHandle(receiveMessageResponse[i].receiptHandle);
    entryList.push_back(entry);
  }

  deleteMessageBatchRequest.__set_queueName(queueName);
  deleteMessageBatchRequest.__set_deleteMessageBatchRequestEntryList(entryList);
  messageClient.deleteMessageBatch(deleteMessageBatchResponse, deleteMessageBatchRequest);


// delete queue
  DeleteQueueRequest deleteQueueRequest;
  deleteQueueRequest.__set_queueName(queueName);
  queueClient.deleteQueue(deleteQueueRequest);
}