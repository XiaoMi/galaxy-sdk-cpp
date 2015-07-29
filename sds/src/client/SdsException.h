#ifndef _SDS_EXCEPTION_H_
#define _SDS_EXCEPTION_H_

#include <exception>
#include "sds/Errors_types.h"

class SdsException : public std::exception {
public:
  SdsException(ErrorCode::type errorCode, const std::string &errorMessage,
    const std::string& details, const std::string& callId, const std::string& requestId);
  SdsException(HttpStatusCode::type httpStatusCode, const std::string& errorMessage);
  SdsException(const SdsException& other);
  virtual ~SdsException() throw();

  HttpStatusCode::type getHttpStatusCode() const;
  ErrorCode::type getErrorCode() const;
  std::string getErrorMessage() const;
  std::string getDetails() const;
  std::string getCallId() const;
  std::string getRequestId() const;
  std::string getType() const;
  virtual const char* what() const throw();

private:
  HttpStatusCode::type httpStatusCode_;
  ErrorCode::type errorCode_;
  std::string errorMessage_;
  std::string details_;
  std::string callId_;
  std::string requestId_;
  std::string type_;
  char message_[1024];
  void setMessage();
};

#endif