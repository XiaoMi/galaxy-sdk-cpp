#ifndef _EMQ_CONNECTION_POOL_H_
#define _EMQ_CONNECTION_POOL_H_

#include <iostream>
#include <string>
#include <list>
#include <memory>
#include <pthread.h>
#include <thrift/transport/TSocket.h>
#include "client/EmqHttpClient.h"
#include "client/EmqSSLSocket.h"

namespace galaxy { namespace emq {
  class ConnectionPool {
    public:
      ConnectionPool(const std::string& host_, int port_, const std::string& path_, rpc::common::ThriftProtocol::type thriftProtocol_,
         const rpc::auth::Credential& credential_, int connectionTimeout_, int sendTimeout_, int receiveTimeout_, int maxSize_, bool isHttps_);
      boost::shared_ptr<EmqHttpClient> getConnection(const std::string& queryString);
      void retConnection(boost::shared_ptr<EmqHttpClient> connection);
      boost::shared_ptr<EmqHttpClient> newConnection(const std::string& queryString);
      void closeConnection(boost::shared_ptr<EmqHttpClient> connection);
      ~ConnectionPool();

    private:
      int connectionTimeout;
      int sendTimeout;
      int receiveTimeout;
      int maxSize;
      int curSize;
      rpc::common::ThriftProtocol::type thriftProtocol;
      std::string host;
      int port;
      std::string path;
      rpc::auth::Credential credential;
      std::list<boost::shared_ptr<TSocket> > connList;
      pthread_mutex_t lock;
      bool isHttps;
      boost::shared_ptr<EmqSSLSocketFactory> factory;
  };
}}
#endif

