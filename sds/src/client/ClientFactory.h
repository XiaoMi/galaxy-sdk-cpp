#ifndef _SDS_CLIENTFACTORY_H_
#define _SDS_CLIENTFACTORY_H_

#include "sds/AdminService.h"
#include "sds/Admin_types.h"
#include "sds/Authentication_types.h"
#include "sds/BaseService.h"
#include "sds/AuthService.h"
#include "sds/Common_types.h"
#include "sds/TableService.h"
#include "sds/Table_types.h"

class TableClientProxy;
class AdminClientProxy;
class AuthClientProxy;

class ClientFactory {
  public:
    ClientFactory(const Credential& credential, ThriftProtocol::type thriftProtocol = ThriftProtocol::TBINARY);
    virtual ~ClientFactory();
    TableClientProxy newDefaultTableClient(bool supportAccountKey = false);
    TableClientProxy newTableClient(const std::string& url, bool supportAccountKey = false);
    AdminClientProxy newDefaultAdminClient(bool supportAccountKey = false);
    AdminClientProxy newAdminClient(const std::string& url, bool supportAccountKey = false);
    AuthClientProxy newDefaultAuthClient(bool supportAccountKey = false);
    AuthClientProxy newAuthClient(const std::string& url, bool supportAccountKey = false);

  private:
    Credential credential_;
    ThriftProtocol::type thriftProtocol_;
};

class TableClientProxy : public TableServiceIf,  virtual public BaseServiceIf {

  public:
    TableClientProxy(const Credential& credential, const std::string& host, int32_t port,
      const std::string& path, bool isHttps, bool supportAccountKey, ThriftProtocol::type thriftProtocol);
    virtual ~TableClientProxy();
    void getServerVersion(Version& _return);
    void validateClientVersion(const Version& clientVersion);
    int64_t getServerTime();
    void get(GetResult& _return, const GetRequest& request);
    void put(PutResult& _return, const PutRequest& request);
    void increment(IncrementResult& _return, const IncrementRequest& request);
    void remove(RemoveResult& _return, const RemoveRequest& request);
    void scan(ScanResult& _return, const ScanRequest& request);
    void batch(BatchResult& _return, const BatchRequest& request);

  private:
    Credential credential_;
    std::string host_;
    int32_t port_;
    std::string path_;
    bool isHttps_;
    bool supportAccountKey_;
    ThriftProtocol::type thriftProtocol_;
};

class AdminClientProxy : public AdminServiceIf, virtual public BaseServiceIf {
  public:
    AdminClientProxy(const Credential& credential, const std::string& host, int32_t port,
      const std::string& path, bool isHttps, bool supportAccountKey, ThriftProtocol::type thriftProtocol);
    virtual ~AdminClientProxy();
    void getServerVersion(Version& _return);
    void validateClientVersion(const Version& clientVersion);
    int64_t getServerTime();
    void saveAppInfo(const AppInfo& appInfo);
    void getAppInfo(AppInfo& _return, const std::string& appId);
    void findAllApps(std::vector<AppInfo> & _return);
    void findAllTables(std::vector<TableInfo> & _return);
    void createTable(TableInfo& _return, const std::string& tableName, const TableSpec& tableSpec);
    void dropTable(const std::string& tableName);
    void lazyDropTable(const std::string& tableName);
    void alterTable(const std::string& tableName, const TableSpec& tableSpec);
    void cloneTable(const std::string& srcName, const std::string& destTable, const bool flushTable);
    void disableTable(const std::string& tableName);
    void enableTable(const std::string& tableName);
    void describeTable(TableSpec& _return, const std::string& tableName);
    void getTableStatus(TableStatus& _return, const std::string& tableName);
    TableState::type getTableState(const std::string& tableName);
    int64_t getTableSize(const std::string& tableName);
    void getTableSplits(std::vector<TableSplit> & _return, const std::string& tableName, const Dictionary& startKey, const Dictionary& stopKey);
    void queryMetric(TimeSeriesData& _return, const MetricQueryRequest& query);
    void queryMetrics(std::vector<TimeSeriesData> & _return, const std::vector<MetricQueryRequest> & queries);
    void findAllAppInfo(std::vector<AppInfo> & _return);
    void putClientMetrics(const ClientMetrics& clientMetrics);
  private:
    Credential credential_;
    std::string host_;
    int32_t port_;
    std::string path_;
    bool isHttps_;
    bool supportAccountKey_;
    ThriftProtocol::type thriftProtocol_;
};

class AuthClientProxy : public AuthServiceIf, virtual public BaseServiceIf {
  public:
    AuthClientProxy(const Credential& credential, const std::string& host, int32_t port,
     const std::string& path, bool isHttps, bool supportAccountKey, ThriftProtocol::type thriftProtocol);
    virtual ~AuthClientProxy();
    void getServerVersion(Version& _return);
    void validateClientVersion(const Version& clientVersion);
    int64_t getServerTime();
    void createCredential(Credential& _return, const OAuthInfo& oauthInfo);

  private:
    Credential credential_;
    std::string host_;
    int32_t port_;
    std::string path_;
    bool isHttps_;
    bool supportAccountKey_;
    ThriftProtocol::type thriftProtocol_;
};
#endif