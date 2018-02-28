#include <memory>
#include <sstream>
#include <stdexcept>
#include "client/ConnectionPool.h"
#include "client/EmqException.h"

using namespace std;

namespace galaxy { namespace emq {
ConnectionPool::ConnectionPool(const std::string& host_, int port_, const std::string& path_, rpc::common::ThriftProtocol::type thriftProtocol_,
   const rpc::auth::Credential& credential_, int connectionTimeout_, int sendTimeout_, int receiveTimeout_, int maxSize_, bool isHttps_) :
   host(host_),
   port(port_),
   path(path_),
   thriftProtocol(thriftProtocol_),
   credential(credential_),
   connectionTimeout(connectionTimeout_),
   sendTimeout(sendTimeout_),
   receiveTimeout(receiveTimeout_),
   maxSize(maxSize_),
   isHttps(isHttps_) {
   pthread_mutex_init(&lock, NULL);
    if (isHttps) {
      factory = boost::shared_ptr<EmqSSLSocketFactory>(new EmqSSLSocketFactory());
    }
}

boost::shared_ptr<EmqHttpClient> ConnectionPool::getConnection(const std::string& queryString) {
  boost::shared_ptr<TSocket> socket;
  pthread_mutex_lock(&lock);
  if (connList.size() > 0) {
    socket = connList.front();
    connList.pop_front();
    boost::shared_ptr<EmqHttpClient> transport(new EmqHttpClient(socket, host, path + queryString, thriftProtocol,
      credential));
    pthread_mutex_unlock(&lock);
    return transport;
  } else {
    pthread_mutex_unlock(&lock);
    return newConnection(queryString);
  }
}

boost::shared_ptr<EmqHttpClient> ConnectionPool::newConnection(const std::string& queryString) {
  pthread_mutex_lock(&lock);
  if (curSize < maxSize) {
    boost::shared_ptr<TSocket> socket;
    if (isHttps) {
      socket = factory->createSocket(host, port);
    } else {
      socket = boost::shared_ptr<TSocket>(new TSocket(host, port));
    }
    socket->setConnTimeout(connectionTimeout);
    socket->setRecvTimeout(receiveTimeout);
    socket->setSendTimeout(sendTimeout);
    socket->setNoDelay(true);
    socket->setKeepAlive(true);
    boost::shared_ptr<EmqHttpClient> transport(new EmqHttpClient(socket, host, path + queryString, thriftProtocol,
     credential));
    transport->open();
    curSize++;
    pthread_mutex_unlock(&lock);
    return transport;
  } else {
    pthread_mutex_unlock(&lock);
    std::ostringstream h;
    h << "Exceed max connection: " << maxSize;
    throw std::runtime_error(h.str());
  }
}

void ConnectionPool::retConnection(boost::shared_ptr<EmqHttpClient> connection) {
  pthread_mutex_lock(&lock);
  connList.push_back(connection->getTransport());
  connection->setTransport(boost::shared_ptr<EmqHttpClient> ((EmqHttpClient*)NULL));
  pthread_mutex_unlock(&lock);
}

void ConnectionPool::closeConnection(boost::shared_ptr<EmqHttpClient> connection) {
  pthread_mutex_lock(&lock);
  connection->close();
  curSize--;
  pthread_mutex_unlock(&lock);
}

ConnectionPool::~ConnectionPool() {
  pthread_mutex_lock(&lock);
  list<boost::shared_ptr<TSocket> >::iterator iter;
  for(iter = connList.begin(); iter != connList.end(); ++iter) {
    (*iter)->close();
  }
  connList.clear();
  curSize = 0;
  pthread_mutex_unlock(&lock);
}

}}