#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"
#include "tprintf.h"
using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
printf("NameNode: init\n");
fflush(stdout);
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(extent_dst, lock_dst);
  NewThread(this, &NameNode::CheckHeartbeat);
fflush(stdout);
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
tprintf("NameNode:GetBlockLocations\n");
  lc->acquire(ino);
  list<NameNode::LocatedBlock> located_blocks;
  list<DatanodeIDProto> candidates = GetDatanodes();
printf("candidates: %d\n", candidates.size());
fflush(stdout);
  if (candidates.empty()){
    lc->release(ino);
    return located_blocks;
  }

  list<blockid_t> block_ids;
  ec->get_block_ids(ino, block_ids);
  uint64_t size = 0;
  extent_protocol::attr a1;
  ec->getattr(ino, a1);
  int s1 = a1.size;
printf("inode size: %d\n", s1);
  for (list<blockid_t>::iterator it=block_ids.begin(); it!=block_ids.end(); it++){
    string content;
    ec->read_block(*it, content);
    if (content.size() + size >= s1){
      located_blocks.push_back(LocatedBlock(*it, size, s1-size, GetDatanodes()));
      break;
    }
    located_blocks.push_back(LocatedBlock(*it, size, (uint64_t)content.size(), GetDatanodes()));
    size += content.size();
  }
  lc->release(ino);
printf("read blocks: %d\n", located_blocks.size());
fflush(stdout);
  return located_blocks;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
tprintf("NameNode: Complete: %d\n", new_size);
  ec->complete(ino, new_size);
  lc->release(ino);
fflush(stdout);
  return true;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
 // throw HdfsException("Not implemented");
tprintf("NameNode: AppendBlock\n");
  blockid_t bid;
  extent_protocol::attr a1;
  ec->getattr(ino, a1);
  int s1 = a1.size;
  ec->append_block(ino, bid);
  extent_protocol::attr a2;
  ec->getattr(ino, a2);
  int s2 = a2.size;
fflush(stdout);
  return LocatedBlock(bid, s1, s2-s1, GetDatanodes());
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
tprintf("NameNode: Rename src_dirino : %d, src_name: %s, dst_dir_ino: %d, dst_name: %s\n", src_dir_ino, src_name.c_str(), dst_dir_ino, dst_name.c_str());
fflush(stdout);
  lc->acquire(src_dir_ino);
  if (src_dir_ino != dst_dir_ino){
  lc->acquire(dst_dir_ino);}
  bool res = false;
  list<yfs_client::dirent> childlist;
  yfs->readdir_without_lock(src_dir_ino, childlist);
  for (list<yfs_client::dirent>:: iterator it = childlist.begin(); it != childlist.end(); it++){
    if (it->name == src_name){
      yfs_client::inum ino = it->inum;
      childlist.erase(it);
      yfs->writedir_without_lock(src_dir_ino, childlist);
      list<yfs_client::dirent> childlist2;
      yfs->readdir_without_lock(dst_dir_ino, childlist2);
      for (list<yfs_client::dirent>:: iterator it2 = childlist2.begin(); it2 != childlist2.end(); it2++){
        if (it2->name == dst_name){
          if (src_dir_ino != dst_dir_ino){
          lc->release(dst_dir_ino);}
          lc->release(src_dir_ino);
          return false;
	}
      }
      res = true;
      yfs_client::dirent ent;
      ent.name = dst_name;
      ent.inum = ino;
      childlist2.push_back(ent);
      yfs->writedir_without_lock(dst_dir_ino, childlist2);
      break;
    }
  }
  if (src_dir_ino != dst_dir_ino){
  lc->release(dst_dir_ino);}
  lc->release(src_dir_ino);
fflush(stdout);
  return res;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
printf("NameNode: mkdir\n");
  int res = yfs->mkdir(parent, name.c_str(), mode, ino_out);
fflush(stdout);
  return true;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
printf("NameNode: create\n");
  lc->acquire(parent);
  if (!yfs->isdir_without_lock(parent)){
    lc->release(parent);
    return false;
  }
  list<yfs_client::dirent> childlist;
  yfs->readdir_without_lock(parent, childlist);
  for (list<yfs_client::dirent>:: iterator it = childlist.begin(); it != childlist.end(); it++){
    if (it->name == name){
      lc->release(parent);
      return false;
    }
  }
  yfs_client::dirent ent;
  ent.name = name;
  ec->create(extent_protocol::T_FILE, ino_out);
  ent.inum = ino_out;
  childlist.push_back(ent);
  yfs->writedir_without_lock(parent, childlist);
 // lc->acquire(ino_out);
  lc->release(parent);
  lc->acquire(ino_out);
printf("ino: %d\n", ino_out);
fflush(stdout);
  return true;
}

bool NameNode::Isfile(yfs_client::inum ino) {
printf("NameNode: isfile %d\n", ino);
  bool res = yfs->isfile_without_lock(ino);
fflush(stdout);
  return res;
}

bool NameNode::Isdir(yfs_client::inum ino) {
printf("NameNode: isdir %d\n", ino);
  bool res = yfs->isdir_without_lock(ino);
fflush(stdout);
  return res;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
printf("NameNode: getfile %d\n", ino);
  int res = yfs->getfile_without_lock(ino, info);
fflush(stdout);
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
printf("NameNode: getdir %d\n", ino);
  int res = yfs->getdir_without_lock(ino, info);
fflush(stdout);
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
printf("NameNode: readdir %d\n", ino);
  int res = yfs->readdir_without_lock(ino, dir);
fflush(stdout);
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
printf("NameNode: unlink \n");
  int res = yfs->unlink_without_lock(parent, name.c_str());
fflush(stdout);
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  datanodes_heartbeat.insert(id);
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  for (std::set<blockid_t>::iterator it = blocks_to_be_replicated.begin(); it !=blocks_to_be_replicated.end(); it++){
    ReplicateBlock(*it, master_datanode, id);
  }
  datanodes_state[id] = true;
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  lc->acquire(0);
  list<DatanodeIDProto> datanodes_alive;
  for (std::map<DatanodeIDProto, bool>::iterator it = datanodes_state.begin(); it != datanodes_state.end(); it++){
    if (it->second == true){
      datanodes_alive.push_back(it->first);
    }
  }
  lc->release(0);
//  datanodes_alive.push_back(master_datanode);
  return datanodes_alive;
}

void NameNode::CheckHeartbeat(){
  while (true){
  //  sleep(3);
    lc->acquire(0);
    for (std::map<DatanodeIDProto, bool>::iterator it = datanodes_state.begin(); it != datanodes_state.end(); it++){
      if (datanodes_heartbeat.find(it->first) == datanodes_heartbeat.end()){
printf("Check:  false\n");
	datanodes_state[it->first] = false;
      }
      else {
printf("Check: still alive\n");
        if (it->second == false){
printf("Check: recovery\n");
          RegisterDatanode(it->first);
        }
      }
    }
fflush(stdout);
    datanodes_heartbeat.clear();
    lc->release(0);
    sleep(1);
  }
}
