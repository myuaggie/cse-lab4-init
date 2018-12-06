#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"
#include "tprintf.h"
using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */
  NewThread(this, &DataNode::SendHeartbeatRepeatedly);
  return 0;
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  string content;
  ec->read_block(bid, content);
tprintf("DataNode: ReadBlock %d, offset %d, len %d\n", bid, offset, len);
  buf = content.substr(offset, len); 
fflush(stdout);
  return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
tprintf("DataNode: WriteBlock %d, offset %d, len %d\n", bid, offset, len);
  string content;
  ec->read_block(bid, content);
//printf("content old: %s\n", content.c_str());
  if (content.size() > offset){
    content = content.replace(offset, len, buf);
  }
  else if (len = 0){
    content += "\0";
  }
  else {
    content += buf;
  }
//printf("content now: %s\n", content.c_str());
//  ec->write_block(bid, buf.substr(offset, len));
  ec->write_block(bid, content);
fflush(stdout);
  return true;
}

void DataNode::SendHeartbeatRepeatedly(){
  while (true){
printf("send heart beat\n");
fflush(stdout);
    SendHeartbeat();
    sleep(1);
  }
}
