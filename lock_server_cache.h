#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <queue>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <set>

class lock_server_cache {
 private:
  int nacquire;
  pthread_mutex_t mutex;
  //pthread_cond_t cond;
  pthread_mutex_t mutex_list[6];
  int nmutex;
  pthread_cond_t cond_list[6];
  int ncond;
  std::map<lock_protocol::lockid_t, std::set<std::string> > lock_waiting_set;
  std::map<lock_protocol::lockid_t, bool> lock_stat_map;
  std::map<lock_protocol::lockid_t, int> lock_lock_map;
  std::map<lock_protocol::lockid_t, int> lock_cond_map;
  std::map<lock_protocol::lockid_t, std::string> lock_owner_map;
 // std::map<lock_protocol::lockid_t, std::queue<std::string> > lock_queue;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
