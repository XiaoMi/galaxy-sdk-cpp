
#include <sstream>
#include "client/EmqException.h"


namespace galaxy { namespace emq {
EmqException::EmqException(::emq::common::ErrorCode::type errorCode, const std::string& errorMessage,
  const std::string& details, const std::string& requestId, const std::string& queueName) :
  httpStatusCode_((rpc::errors::HttpStatusCode::type) 200),
  errorCode_(errorCode),
  errorMessage_(errorMessage),
  details_(details),
  requestId_(requestId),
  queueName_(queueName),
  type_("service"){
  setMessage();
}

EmqException::EmqException(rpc::errors::HttpStatusCode::type httpStatusCode, const std::string& errorMessage){
  switch(httpStatusCode) {
    case rpc::errors::HttpStatusCode::INVALID_AUTH:
      errorCode_ = ::emq::common::ErrorCode::UNKNOWN;
      break;
    case rpc::errors::HttpStatusCode::CLOCK_TOO_SKEWED:
      errorCode_ = ::emq::common::ErrorCode::UNKNOWN;
      break;
    case rpc::errors::HttpStatusCode::REQUEST_TOO_LARGE:
      errorCode_ = ::emq::common::ErrorCode::BAD_REQUEST;
      break;
    case rpc::errors::HttpStatusCode::BAD_REQUEST:
      errorCode_ = ::emq::common::ErrorCode::BAD_REQUEST;
      break;
    case rpc::errors::HttpStatusCode::INTERNAL_ERROR:
      errorCode_ = ::emq::common::ErrorCode::INTERNAL_ERROR;
      break;
    default:
      errorCode_ = ::emq::common::ErrorCode::UNKNOWN;
  }
  httpStatusCode_ = httpStatusCode;
  errorMessage_ = errorMessage;
  details_ = "";
  requestId_ = "";
  queueName_ = "";
  type_ = "transport";
  setMessage();
}

EmqException::EmqException(const EmqException& other) :
  httpStatusCode_(other.httpStatusCode_),
  errorCode_(other.errorCode_),
  errorMessage_(other.errorMessage_),
  details_(other.details_),
  requestId_(other.requestId_),
  queueName_(other.queueName_),
  type_(other.type_){
  setMessage();
}


rpc::errors::HttpStatusCode::type EmqException::getHttpStatusCode() const {
  return httpStatusCode_;
}
::emq::common::ErrorCode::type EmqException::getErrorCode() const {
  return errorCode_;
}

std::string EmqException::getErrorMessage() const {
  return errorMessage_;
}

std::string EmqException::getDetails() const {
  return details_;
}


std::string EmqException::getQueueName() const {
  return queueName_;
}

std::string EmqException::getRequestId() const {
  return requestId_;
}

std::string EmqException::getType() const {
  return type_;
}

void EmqException::setMessage() {
  if (this->getType() == "transport") {
    sprintf(message_, "%s%d%s%d%s%s%s", "HTTP transport error [http status code: ", httpStatusCode_,
      ", error code: ", errorCode_, ", error message: ", errorMessage_.c_str(), "]");
  } else if (this->getType() == "service") {
    sprintf(message_, "%s%d%s%s%s%s%s%s%s%s%s", "Service error [error code: ", errorCode_,
      ", error message: ", errorMessage_.c_str(), ", details: ", details_.c_str(), ", queue name: ",
      queueName_.c_str(), ", request id: ", requestId_.c_str(), "]");
  } else {
    sprintf(message_, "%s", "Unknown exception");
  }
}

const char* EmqException::what() const throw() {
  return this->message_;
}

EmqException::~EmqException() throw() {}
}}