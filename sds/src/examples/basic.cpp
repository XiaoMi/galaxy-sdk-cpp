#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include "../client/ClientFactory.h"
#include "../client/SdsException.h"
#include "../sds/Authentication_types.h"
#include "../sds/Common_constants.h"
#include "../sds/Table_types.h"
#include "../client/DatumUtil.h"
#include "../client/TableScanner.h"

using namespace std;

extern const CommonConstants g_Common_constants;

TableSpec getTableSpec() {
  IndexSpec primaryKey;
  KeySpec key0;
  key0.__set_attribute("cityId");
  primaryKey.push_back(key0);
  KeySpec key1;
  key1.__set_attribute("timestamp");
  key1.__set_asc(false);
  primaryKey.push_back(key1);
  std::map<std::string, DataType::type> attributes;
  attributes.insert(pair<std::string, DataType::type>("cityId", DataType::STRING));
  attributes.insert(pair<std::string, DataType::type>("timestamp", DataType::INT64));
  attributes.insert(pair<std::string, DataType::type>("score", DataType::DOUBLE));
  attributes.insert(pair<std::string, DataType::type>("pm25", DataType::INT64));
  TableSchema schema;
  schema.__set_primaryIndex(primaryKey);
  schema.__set_attributes(attributes);

  TableQuota tableQuota;
  tableQuota.__set_size(100 * 1024 * 1024);
  ProvisionThroughput throughput;
  throughput.__set_readCapacity(20);
  throughput.__set_writeCapacity(20);
  TableMetadata metadata;
  metadata.__set_quota(tableQuota);
  metadata.__set_throughput(throughput);
  TableSpec tableSpec;
  tableSpec.__set_schema(schema);
  tableSpec.__set_metadata(metadata);
  return tableSpec;
}

int main(int argc, char *argv[]) {
  UserType::type userType = UserType::APP_SECRET;
  std::string appKey = ""; //Your appKey
  std::string appSecret = ""; //Your appSecret
  std::string endpoint = "http://cnbj0.sds.api.xiaomi.com";
  std::string tableName = "sds-cpp-example";
  std::string cities[] = {"北京", "Beihai", "Dalian", "Dandong", "Fuzhou", "Guangzhou", "Haikou",
                       "Hankou", "Huangpu", "Jiujiang", "Lianyungang", "Nanjing", "Nantong", "Ningbo",
                       "Qingdao", "Qinhuangdao", "Rizhao", "Sanya", "Shanghai", "Shantou", "Shenzhen",
                       "Tianjin", "Weihai", "Wenzhou", "Xiamen", "Yangzhou", "Yantai"};
  Credential credential;
  credential.__set_type(userType);
  credential.__set_secretKeyId(appKey);
  credential.__set_secretKey(appSecret);
  ClientFactory clientFactory(credential);
  AdminClientProxy adminClient = clientFactory.newAdminClient(endpoint + g_Common_constants.ADMIN_SERVICE_PATH);
  TableClientProxy tableClient = clientFactory.newTableClient(endpoint + g_Common_constants.TABLE_SERVICE_PATH);
  TableInfo tableInfo;
  TableSpec tableSpec = getTableSpec();
  try {
    adminClient.dropTable(tableName);
  } catch(const SdsException& sdse) {
    if (sdse.getErrorCode() != ErrorCode::RESOURCE_NOT_FOUND) {
      throw sdse;
    }
  }

  adminClient.createTable(tableInfo, tableName, tableSpec);
  cout << "create table " << tableInfo.name << " successful" << endl;
  int64_t now = time(NULL);
  // put data
  for (int i = 0; i < 10; i++) {
    PutRequest put;
    put.__set_tableName(tableName);
    Dictionary record;
    record.insert(pair<std::string, Datum>("cityId", DatumUtil::stringDatum(cities[i])));
    record.insert(pair<std::string, Datum>("timestamp", DatumUtil::int64Datum(now)));
    record.insert(pair<std::string, Datum>("score", DatumUtil::doubleDatum((rand() % 100)/ 100.0)));
    record.insert(pair<std::string, Datum>("pm25", DatumUtil::int64Datum((int64_t) (rand() % 500))));
    put.__set_record(record);
    PutResult putResult;
    tableClient.put(putResult, put);
    cout << "put record #" << i << endl;
  }

  // get data
  GetResult getResult;
  GetRequest get;
  get.__set_tableName(tableName);
  Dictionary keys;
  keys.insert(pair<std::string, Datum>("cityId", DatumUtil::stringDatum(cities[0])));
  keys.insert(pair<std::string, Datum>("timestamp", DatumUtil::int64Datum(now)));
  get.__set_keys(keys);
  std::vector<std::string> getAttr;
  getAttr.push_back("pm25");
  get.__set_attributes(getAttr);
  tableClient.get(getResult, get);
  Dictionary item = getResult.item;
  cout << "get data: pm25 [" << DatumUtil::int64Value(item.find("pm25")->second) << "]"<< endl;

  // scan data
  ScanRequest scan;
  ScanResult  scanResult;
  scan.__set_tableName(tableName);
  std::vector<std::string> scanAttr;
  scanAttr.push_back("cityId");
  scanAttr.push_back("score");
  scan.__set_attributes(scanAttr);
  scan.__set_limit(3);

  TableScanner scanner(tableClient, scan);
  while(scanner.hasNext()) {
    Dictionary record = scanner.next();
    cout << "cityId: " << DatumUtil::stringValue(record.find("cityId")->second) <<
        ", score: " << DatumUtil::doubleValue(record.find("score")->second) << endl;
  }
}
