// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
printf("acquire\n");	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if (lock_map.find(lid) != lock_map.end()){
    while (lock_map[lid] == true){
      pthread_cond_wait(&cond, &mutex);
    }
  }  
  lock_map[lid] = true;
  nacquire ++;
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
printf("release\n");	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if (lock_map.find(lid) == lock_map.end() || lock_map[lid] == false){
    return lock_protocol::NOENT;
  }
  lock_map[lid] = false;
  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mutex);
  return ret;
}
