#include <unistd.h>
#include "client/TableScanner.h"


TableScanner::TableScanner(const TableClientProxy& tableClient, const ScanRequest& scanRequest) :
  tableClient_(tableClient),
  scanRequest_(scanRequest),
  recordPtr_(NULL),
  baseWaitTime_(500 * 1000),
  retryTime_(0),
  startKey_(scanRequest.startKey),
  isFinished_(false){
}

TableScanner::~TableScanner() {
}

bool TableScanner::hasNext() {
  if (this->recordPtr_ == NULL || this->iter_ == this->recordPtr_->end()) {
    if (this->isFinished_) {
      return false;
    } else {
      if (this->retryTime_ > 0) {
        usleep(this->baseWaitTime_ << (this->retryTime_ - 1));
      }
      this->resultPtr_ = boost::shared_ptr<ScanResult>(new ScanResult());
      this->tableClient_.scan(*(this->resultPtr_), scanRequest_);
      recordPtr_ = &(resultPtr_->records);
      if (!resultPtr_->__isset.nextStartKey) {
        this->isFinished_ = true;
      } else {
        if (recordPtr_->size() == this->scanRequest_.limit) {
          this->retryTime_ = 0;
        } else {
          this->retryTime_++;
        }
      }
      scanRequest_.__set_startKey(resultPtr_->nextStartKey);
      this->iter_ = recordPtr_->begin();
    }
  }
  return this->iter_ != this->recordPtr_->end();
}

Dictionary TableScanner::next() {
  Dictionary record = *(this->iter_);
  this->iter_++;
  return record;
}