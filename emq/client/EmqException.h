#ifndef _EMQ_EXCEPTION_H_
#define _EMQ_EXCEPTION_H_

#include <exception>
#include "emq/Common_types.h"
#include "rpc/Errors_types.h"
namespace galaxy { namespace emq {
class EmqException : public std::exception {
public:
  EmqException(::emq::common::ErrorCode::type errorCode, const std::string& errorMessage,
    const std::string& details, const std::string& requestId, const std::string& queueName);
  EmqException(rpc::errors::HttpStatusCode::type httpStatusCode, const std::string& errorMessage);
  EmqException(const EmqException& other);
  virtual ~EmqException() throw();

  rpc::errors::HttpStatusCode::type getHttpStatusCode() const;
  ::emq::common::ErrorCode::type getErrorCode() const;
  std::string getErrorMessage() const;
  std::string getDetails() const;
  std::string getRequestId() const;
  std::string getQueueName() const;
  std::string getType() const;
  virtual const char* what() const throw();

private:
  rpc::errors::HttpStatusCode::type httpStatusCode_;
  ::emq::common::ErrorCode::type errorCode_;
  std::string errorMessage_;
  std::string details_;
  std::string requestId_;
  std::string queueName_;
  std::string type_;
  char message_[1024];
  void setMessage();
};

}}

#endif