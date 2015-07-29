#ifndef _SDS_TABLESCANNER_H_
#define _SDS_TABLESCANNER_H_

#include "client/ClientFactory.h"
#include "sds/Table_types.h"

class TableScanner {

public:
  TableScanner(const TableClientProxy& tableClient, const ScanRequest& scanRequest);
  bool hasNext();
  Dictionary next();
  ~TableScanner();

private:
  TableClientProxy tableClient_;
  ScanRequest scanRequest_;
  std::vector<Dictionary> * recordPtr_;
  boost::shared_ptr<ScanResult> resultPtr_;
  int64_t baseWaitTime_;
  int32_t retryTime_;
  Dictionary startKey_;
  std::vector<Dictionary>::iterator iter_;
  bool isFinished_;
};

#endif