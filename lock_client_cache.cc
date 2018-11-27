// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst,
                                     class lock_release_user *_lu)
: lock_client(xdst), lu(_lu)
{
    srand(time(NULL)^last_port);
    rlock_port = ((rand()%32000) | (0x1 << 10));
    const char *hname;
    // VERIFY(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
    sockaddr_in dstsock;
    make_sockaddr(xdst.c_str(), &dstsock);
    cl = new rpcc(dstsock);
    if (cl->bind() < 0) {
        printf("lock_client: call bind\n");
    }
    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&cond, NULL);
//    for (int i = 0;i< 200;i++){
//      pthread_mutex_init(&mutex_list[i], NULL);
//      pthread_cond_init(&cond_list[i], NULL);
//    }
//    ncond = 0;
//    nmutex = 0;
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
    // Get lock lock.
   /* pthread_mutex_lock(&mutex);
    if (lock_lock_map.find(lid) == lock_lock_map.end()){
        lock_lock_map[lid] = nmutex;
        lock_cond_map[lid] = ncond;
        nmutex++; ncond++;
    }
    pthread_mutex_unlock(&mutex);*/
    
    pthread_mutex_lock(&mutex);
    int ret = lock_protocol::OK;
    tprintf("%d: Acquire Start\n", rlock_port);
    
    while (lock_stat_map.find(lid) != lock_stat_map.end() && lock_stat_map[lid] == RELEASING){
       tprintf("%d: Acquire waiting releasing\n", rlock_port);
        pthread_cond_wait(&cond, &mutex);
    }
    // Add nacquire.
    if (lock_acquire_map.find(lid) == lock_acquire_map.end()){
        lock_acquire_map[lid] = 1;
    }
    else {
        lock_acquire_map[lid] ++;
    }

    // Send acquire to server.
    if (lock_stat_map.find(lid) == lock_stat_map.end() || lock_stat_map[lid] == NONE){
        lock_stat_map[lid] = ACQUIRING;
        pthread_mutex_unlock(&mutex);
        
        int r;
        tprintf("%d: Acquire Sent\n", rlock_port);

        lock_protocol::status ret2 = cl->call(lock_protocol::acquire, lid, id, r);
        pthread_mutex_lock(&mutex);
        if (ret2 == lock_protocol::OK && lock_stat_map[lid] == ACQUIRING){
            tprintf("%d: Acquire directly\n", rlock_port);
            lock_stat_map[lid] = FREE;
        }
        if (ret2 == lock_protocol::RPCERR && lock_stat_map[lid] == ACQUIRING){
            lock_stat_map[lid] = FREE;
            lock_revoke_map[lid]++;
        }
    }
    
    while (lock_stat_map[lid] != FREE){
        tprintf("%d: Acquire waiting free\n", rlock_port);
        pthread_cond_wait(&cond, &mutex);
    }
    lock_stat_map[lid] = LOCKED;
    
    tprintf("%d: Acquire Done, nacquire: %d\n", rlock_port, lock_acquire_map[lid]);
    pthread_mutex_unlock(&mutex);
    return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    pthread_mutex_lock(&mutex);
    tprintf("%d: Release Start\n", rlock_port);
    
    // Which will never happen.
    if (lock_stat_map.find(lid) == lock_stat_map.end() || lock_stat_map[lid] != LOCKED || lock_acquire_map[lid] == 0){
        tprintf("release: no ent\n");
        pthread_mutex_unlock(&mutex);
        return lock_protocol::NOENT;
    }
    
    lock_acquire_map[lid] --;
    
    if (lock_acquire_map[lid] == 0 && lock_revoke_map[lid] > 0){
        // Revoke
       
        lock_stat_map[lid] = RELEASING;
       // printf("%d: Revoke Done\n", rlock_port);
        lock_revoke_map[lid] = 0;
       // lock_stat_map[lid] = NONE;
        pthread_mutex_unlock(&mutex);
        int r;
        lock_protocol::status ret = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&mutex);
        tprintf("%d: Revoke Done\n", rlock_port);
        lock_revoke_map[lid] = 0;
        lock_stat_map[lid] = NONE;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&mutex);
        return lock_protocol::OK;
    }
    
    tprintf("%d: Release Done\n", rlock_port);
    lock_stat_map[lid] = FREE;
    pthread_cond_broadcast(&cond);
    
    pthread_mutex_unlock(&mutex);
    return lock_protocol::OK;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
                                  int &)
{
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&mutex);
    tprintf("%d: Revoke start\n", rlock_port);
    
    if (lock_stat_map[lid] == FREE && lock_acquire_map[lid] == 0){
        ret = rlock_protocol::OK;
        tprintf("%d: Revoke directly\n", rlock_port);
      //  lock_stat_map[lid] = NONE;
        lock_revoke_map[lid] = 0;
        lock_stat_map[lid] = RELEASING;
        pthread_mutex_unlock(&mutex);
        int r;
        lock_protocol::status ret2 = cl->call(lock_protocol::release, lid, id, r);
        pthread_mutex_lock(&mutex);
        lock_stat_map[lid] = NONE;
 tprintf("%d: Revoke directly back\n", rlock_port);
        lock_revoke_map[lid] = 0;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&mutex);
        return ret;
    }
    
    if (lock_revoke_map.find(lid) == lock_revoke_map.end()){
        lock_revoke_map[lid] = 1;
    }
    else {
        lock_revoke_map[lid] ++;
    }
    
    tprintf("%d: nrevoke: %d\n", rlock_port, lock_revoke_map[lid]);
    pthread_mutex_unlock(&mutex);
    return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
                                 int &)
{
    int ret = rlock_protocol::OK;
    pthread_mutex_lock(&mutex);
    tprintf("%d: Retry\n", rlock_port);
    
    if (lock_stat_map[lid] == ACQUIRING && lock_acquire_map[lid] > 0){
      lock_stat_map[lid] = FREE;
    }
    else if (lock_acquire_map[lid] == 0){
      printf("%d: Retry again?\n", rlock_port);
   //   lock_stat_map[lid] = NONE;
      pthread_mutex_unlock(&mutex);
      return rlock_protocol::RPCERR;
    }
    
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);
    return ret;
}



