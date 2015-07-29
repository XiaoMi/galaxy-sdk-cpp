
#include <sys/time.h>
#include "client/SdsException.h"
#include "client/SdsHttpTransport.h"
#include "sds/Errors_types.h"

using namespace apache::thrift;
using namespace apache::thrift::transport;

const char* SdsHttpTransport::CRLF = "\r\n";
const int SdsHttpTransport::CRLF_LEN = 2;

SdsHttpTransport::SdsHttpTransport(boost::shared_ptr<TTransport> transport, int64_t &clockOffset) :
  transport_(transport),
  clockOffset_(clockOffset),
  readHeaders_(true),
  chunked_(false),
  chunkedDone_(false),
  chunkSize_(0),
  contentLength_(0),
  httpBuf_(NULL),
  httpPos_(0),
  httpBufLen_(0),
  httpBufSize_(1024) {
  init();
}

void SdsHttpTransport::init() {
  httpBuf_ = (char*)std::malloc(httpBufSize_+1);
  if (httpBuf_ == NULL) {
    throw std::bad_alloc();
  }
  httpBuf_[httpBufLen_] = '\0';
}

SdsHttpTransport::~SdsHttpTransport() {
  if (httpBuf_ != NULL) {
    std::free(httpBuf_);
  }
}

uint32_t SdsHttpTransport::read(uint8_t* buf, uint32_t len) {
  if (readBuffer_.available_read() == 0) {
    readBuffer_.resetBuffer();
    uint32_t got = readMoreData();
    if (got == 0) {
      return 0;
    }
  }
  return readBuffer_.read(buf, len);
}

uint32_t SdsHttpTransport::readEnd() {
  // Read any pending chunked data (footers etc.)
  if (chunked_) {
    while (!chunkedDone_) {
      readChunked();
    }
  }
  return 0;
}

uint32_t SdsHttpTransport::readMoreData() {
  uint32_t size;

  // Get more data!
  refill();

  if (readHeaders_) {
    readHeaders();
  }

  if (chunked_) {
    size = readChunked();
  } else {
    size = readContent(contentLength_);
    readHeaders_ = true;
  }
//  readHeaders_ = true;
  return size;
}

uint32_t SdsHttpTransport::readChunked() {
  uint32_t length = 0;

  char* line = readLine();
  uint32_t chunkSize = parseChunkSize(line);
  if (chunkSize == 0) {
    readChunkedFooters();
  } else {
    // Read data content
    length += readContent(chunkSize);
    // Read trailing CRLF after content
    readLine();
  }
  return length;
}

void SdsHttpTransport::readChunkedFooters() {
  // End of data, read footer lines until a blank one appears
  while (true) {
    char* line = readLine();
    if (strlen(line) == 0) {
      chunkedDone_ = true;
      break;
    }
  }
}

uint32_t SdsHttpTransport::parseChunkSize(char* line) {
  char* semi = strchr(line, ';');
  if (semi != NULL) {
    *semi = '\0';
  }
  uint32_t size = 0;
  sscanf(line, "%x", &size);
  return size;
}

uint32_t SdsHttpTransport::readContent(uint32_t size) {
  uint32_t need = size;
  while (need > 0) {
    uint32_t avail = httpBufLen_ - httpPos_;
    if (avail == 0) {
      // We have given all the data, reset position to head of the buffer
      httpPos_ = 0;
      httpBufLen_ = 0;
      refill();

      // Now have available however much we read
      avail = httpBufLen_;
    }
    uint32_t give = avail;
    if (need < give) {
      give = need;
    }
    readBuffer_.write((uint8_t*)(httpBuf_+httpPos_), give);
    httpPos_ += give;
    need -= give;
  }
  return size;
}

char* SdsHttpTransport::readLine() {
  while (true) {
    char* eol = NULL;

    eol = strstr(httpBuf_+httpPos_, CRLF);

    // No CRLF yet?
    if (eol == NULL) {
      // Shift whatever we have now to front and refill
      shift();
      refill();
    } else {
      // Return pointer to next line
      *eol = '\0';
      char* line = httpBuf_+httpPos_;
      httpPos_ = static_cast<uint32_t>((eol-httpBuf_) + CRLF_LEN);
      return line;
    }
  }

}

void SdsHttpTransport::shift() {
  if (httpBufLen_ > httpPos_) {
    // Shift down remaining data and read more
    uint32_t length = httpBufLen_ - httpPos_;
    memmove(httpBuf_, httpBuf_+httpPos_, length);
    httpBufLen_ = length;
  } else {
    httpBufLen_ = 0;
  }
  httpPos_ = 0;
  httpBuf_[httpBufLen_] = '\0';
}

void SdsHttpTransport::refill() {
  uint32_t avail = httpBufSize_ - httpBufLen_;
  if (avail <= (httpBufSize_ / 4)) {
    httpBufSize_ *= 2;
    httpBuf_ = (char*)std::realloc(httpBuf_, httpBufSize_+1);
    if (httpBuf_ == NULL) {
      throw std::bad_alloc();
    }
  }

  // Read more data
  uint32_t got = transport_->read((uint8_t*)(httpBuf_+httpBufLen_), httpBufSize_-httpBufLen_);
  httpBufLen_ += got;
  httpBuf_[httpBufLen_] = '\0';

  if (got == 0) {
    throw TTransportException("Could not refill buffer");
  }
}

void SdsHttpTransport::readHeaders() {
  // Initialize headers state variables
  contentLength_ = 0;
  chunked_ = false;
  chunkedDone_ = false;
  chunkSize_ = 0;

  // Control state flow
  bool statusLine = true;
  bool finished = false;

  // Loop until headers are finished
  while (true) {
    char* line = readLine();
    if (strlen(line) == 0) {
      if (finished) {
        readHeaders_ = false;
        if (statusCode_ != 200) {
          if ((HttpStatusCode::type) statusCode_ == HttpStatusCode::CLOCK_TOO_SKEWED) {
            int64_t now = time(NULL);
            clockOffset_ = serverTimestamp_ - now;
          }
          throw SdsException((HttpStatusCode::type) statusCode_, errorMessage_);
        }
        return;
      } else {
        // Must have been an HTTP 100, keep going for another status line
        statusLine = true;
      }
    } else {
      if (statusLine) {
        statusLine = false;
        finished = parseStatusLine(line);
      } else {
        parseHeader(line);
      }
    }
  }
}

void SdsHttpTransport::write(const uint8_t* buf, uint32_t len) {
  writeBuffer_.write(buf, len);
}
