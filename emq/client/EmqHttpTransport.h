#ifndef _EMQ_HTTPTRANSPORT_H_
#define _EMQ_HTTPTRANSPORT_H_

#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/transport/TTransport.h>

using namespace apache::thrift;
using namespace apache::thrift::transport;

namespace galaxy { namespace emq {
class EmqHttpTransport : public TVirtualTransport<EmqHttpTransport> {
 public:
  EmqHttpTransport(boost::shared_ptr<TTransport> transport);

  virtual ~EmqHttpTransport();

  void open() {
    transport_->open();
  }

  bool isOpen() {
    return transport_->isOpen();
  }

  bool peek() {
    return transport_->peek();
  }

  void close() {
    transport_->close();
  }

  uint32_t read(uint8_t* buf, uint32_t len);

  uint32_t readEnd();

  void write(const uint8_t* buf, uint32_t len);

  virtual void flush() = 0;

 protected:

  boost::shared_ptr<TTransport> transport_;

  TMemoryBuffer writeBuffer_;
  TMemoryBuffer readBuffer_;

  bool readHeaders_;
  bool chunked_;
  bool chunkedDone_;
  uint32_t chunkSize_;
  uint32_t contentLength_;

  char* httpBuf_;
  uint32_t httpPos_;
  uint32_t httpBufLen_;
  uint32_t httpBufSize_;
  int32_t statusCode_;
  std::string errorMessage_;

  virtual void init();

  uint32_t readMoreData();
  char* readLine();

  void readHeaders();
  virtual void parseHeader(char* header) = 0;
  virtual bool parseStatusLine(char* status) = 0;

  uint32_t readChunked();
  void readChunkedFooters();
  uint32_t parseChunkSize(char* line);

  uint32_t readContent(uint32_t size);

  void refill();
  void shift();

  static const char* CRLF;
  static const int CRLF_LEN;
};
}}

#endif
