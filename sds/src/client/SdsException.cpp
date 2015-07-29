
#include "client/SdsException.h"
#include <sstream>


SdsException::SdsException(ErrorCode::type errorCode, const std::string &errorMessage,
  const std::string &details, const std::string &callId, const std::string &requestId) :
  httpStatusCode_((HttpStatusCode::type) 200),
  errorCode_(errorCode),
  errorMessage_(errorMessage),
  details_(details),
  callId_(callId),
  requestId_(requestId),
  type_("service"){
  setMessage();
}

SdsException::SdsException(HttpStatusCode::type httpStatusCode, const std::string &errorMessage){
  switch(httpStatusCode) {
    case HttpStatusCode::INVALID_AUTH:
      errorCode_ = ErrorCode::INVALID_AUTH;
      break;
    case HttpStatusCode::CLOCK_TOO_SKEWED:
      errorCode_ = ErrorCode::CLOCK_TOO_SKEWED;
      break;
    case HttpStatusCode::REQUEST_TOO_LARGE:
      errorCode_ = ErrorCode::REQUEST_TOO_LARGE;
      break;
    case HttpStatusCode::BAD_REQUEST:
      errorCode_ = ErrorCode::BAD_REQUEST;
      break;
    case HttpStatusCode::INTERNAL_ERROR:
      errorCode_ = ErrorCode::INTERNAL_ERROR;
      break;
    default:
      errorCode_ = ErrorCode::UNKNOWN;
  }
  httpStatusCode_ = httpStatusCode;
  errorMessage_ = errorMessage;
  details_ = "";
  callId_ = "";
  requestId_ = "";
  type_ = "transport";
  setMessage();
}

SdsException::SdsException(const SdsException& other) :
  httpStatusCode_(other.httpStatusCode_),
  errorCode_(other.errorCode_),
  errorMessage_(other.errorMessage_),
  details_(other.details_),
  callId_(other.callId_),
  requestId_(other.requestId_),
  type_(other.type_){
  setMessage();
}

HttpStatusCode::type SdsException::getHttpStatusCode() const {
  return httpStatusCode_;
}
ErrorCode::type SdsException::getErrorCode() const {
  return errorCode_;
}

std::string SdsException::getErrorMessage() const {
  return errorMessage_;
}

std::string SdsException::getDetails() const {
  return details_;
}


std::string SdsException::getCallId() const {
  return callId_;
}

std::string SdsException::getRequestId() const {
  return requestId_;
}

std::string SdsException::getType() const {
  return type_;
}

void SdsException::setMessage() {
  if (this->getType() == "transport") {
    sprintf(message_, "%s%d%s%d%s%s%s", "HTTP transport error [http status code: ", httpStatusCode_,
      ", error code: ", errorCode_, ", error message: ", errorMessage_.c_str(), "]");
  } else if (this->getType() == "service") {
    sprintf(message_, "%s%d%s%s%s%s%s%s%s%s%s", "Service error [error code: ", errorCode_,
      ", error message: ", errorMessage_.c_str(), ", details: ", details_.c_str(), ", call id: ",
      callId_.c_str(), ", request id: ", requestId_.c_str(), "]");
  } else {
    sprintf(message_, "%s", "Unknown exception");
  }
}

const char* SdsException::what() const throw() {
  return this->message_;
}

SdsException::~SdsException() throw() {}
