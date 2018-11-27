// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache()
{
    pthread_mutex_init(&mutex, NULL);
 /*   for (int i = 0;i< 200;i++){
      pthread_mutex_init(&mutex_list[i], NULL);
   //   pthread_cond_init(&cond_list[i], NULL);
    }
  //  ncond = 0;
    nmutex = 0;*/
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
    // Get lock lock.
/*    pthread_mutex_lock(&mutex);
    if (lock_lock_map.find(lid) == lock_lock_map.end()){
        lock_lock_map[lid] = nmutex;
        //lock_cond_map[lid] = ncond;
        nmutex++;
        //ncond++;
    }
    pthread_mutex_unlock(&mutex);
  */  
    pthread_mutex_lock(&mutex);
tprintf("Server: start acquire: %s\n", id.c_str());
    lock_protocol::status ret = lock_protocol::OK;
    if (lock_owner_map.find(lid) != lock_owner_map.end()){
       tprintf("Server: owner now: %s\n", lock_owner_map[lid].c_str());
    }
    // Get lock directly.
    if (lock_owner_map.find(lid) == lock_owner_map.end() || lock_owner_map[lid] == "" || lock_owner_map[lid] == id){
tprintf("Server: direct acquire: %s\n", id.c_str());
        if (lock_owner_map[lid] == id){
            ret = lock_protocol::RPCERR;
            pthread_mutex_unlock(&mutex);
            return ret;
        }
        lock_owner_map[lid] = id;
        if (lock_waiting_set.find(lid) != lock_waiting_set.end()){
            lock_waiting_set[lid].erase(id);
            if (lock_waiting_set[lid].empty()){
              lock_stat_map[lid] = false;
            }
        }
        pthread_mutex_unlock(&mutex);
        return ret;
    }
    // Retry
    ret = lock_protocol::RETRY;
    
    // Add client to waiting set.
    if (lock_waiting_set.find(lid) == lock_waiting_set.end()){
        std::set<std::string> s;
        s.insert(id);
        lock_waiting_set[lid] = s;
    }
    else {
        lock_waiting_set[lid].insert(id);
    }
    
    if (lock_stat_map.find(lid) == lock_stat_map.end() || lock_stat_map[lid] == false){
        // There isn't lock revoking.
        lock_stat_map[lid] = true;
        std::string port = lock_owner_map[lid].substr(lock_owner_map[lid].find(":", 0)+1);
        tprintf("Server: acquire: revoke%s\n", lock_owner_map[lid].c_str());
        pthread_mutex_unlock(&mutex);
      //  sockaddr_in dstsock;
      //  make_sockaddr(port.c_str(), &dstsock);
      //  rpcc *cl = new rpcc(dstsock);
      //  if (cl->bind() < 0) {
      //      printf("lock_server revoke: call bind\n");
      //  }
        handle h(port);
        rpcc *cl = h.safebind();
        int r;
        rlock_protocol::status ret2 = cl->call(rlock_protocol::revoke, lid, r);
    }
    else {
        // There is lock revoking.
        pthread_mutex_unlock(&mutex);
    }
    
    return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
    lock_protocol::status ret = lock_protocol::OK;
    pthread_mutex_lock(&mutex);
    
tprintf("Server: start release: %s\n", id.c_str());
    // Which will never happen.
    if (lock_owner_map[lid] != id){
      tprintf("Server: release: %s: not owner\n", id.c_str());
      pthread_mutex_unlock(&mutex);
        return ret;
    }

    if (lock_waiting_set[lid].empty()){
        tprintf("Server: release: %s: retry:empty\n", id.c_str());
        lock_stat_map[lid] = false;
        lock_owner_map[lid] = "";
        pthread_mutex_unlock(&mutex);
        return ret;
    } 
    // Next client in waiting set gets ownership.
    std::string next_id = *(lock_waiting_set[lid].begin());
   // std::string port = next_id.substr(next_id.find(":", 0)+1);
    
    lock_waiting_set[lid].erase(next_id);
  /*  if (next_id == id){
      if (!lock_waiting_set[lid].empty()){
        next_id = *(lock_waiting_set[lid].begin());
        lock_waiting_set[lid].erase(next_id);
        lock_waiting_set[lid].insert(id);
      }
    }
  */
    std::string port = next_id.substr(next_id.find(":", 0)+1);
   // lock_stat_map[lid] = false;
   // lock_owner_map[lid] = next_id;
tprintf("Server: release: set owner %s\n", next_id.c_str());
 //   pthread_mutex_unlock(&mutex);
 //   sockaddr_in dstsock;
 //   make_sockaddr(port.c_str(), &dstsock);
 //   rpcc *cl = new rpcc(dstsock);
 //   if (cl->bind() < 0) {
 //       printf("lock_server revoke: call bind\n");
 //   }
     handle h(port);
     rpcc *cl = h.safebind();
tprintf("Server: release: retry %s\n", next_id.c_str());
    int r2;
    rlock_protocol::status ret2 = cl->call(rlock_protocol::retry, lid, r2);
 /*   if (ret2 == rlock_protocol::RPCERR){
       tprintf("Server: release: %s: retry:error\n", id.c_str());
       pthread_mutex_unlock(&mutex);
        return lock_protocol::NOENT;
    }*/ 
    lock_stat_map[lid] = false;
    lock_owner_map[lid] = next_id;
    // If there are other clients waiting for the lock.
    if (!lock_waiting_set[lid].empty()){
        lock_stat_map[lid] = true;
tprintf("Server: release: revoke%s\n", next_id.c_str());
        pthread_mutex_unlock(&mutex);
        ret2 = cl->call(rlock_protocol::revoke, lid, r2);
    }
    else{
         pthread_mutex_unlock(&mutex);
    }
    
  //  pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

