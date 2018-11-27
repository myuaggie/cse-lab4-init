#include "extent_client.h"
#include <stdio.h>

#define FILE_NUM 50
#define LARGE_FILE_SIZE 512*64

extent_client *ec;

int main(int argc, char *argv[])
{
  ec = new extent_client();
  extent_protocol::extentid_t id;
  extent_protocal::attr a;
  ec->create(extent_protocol::T_DIR, id);
  ec->getattr(id, a);
  std::string buf('1', 15199);
  ec->put(id, buf);
  buf += std::string('\0', 50337);
  buf += std::string('1', 17);
  ec->put(id, buf);
  std::string buf2;
  ec->get(id, buf2);
  printf("put:%s\n",buf);
  printf("get:%s\n",buf2);
}
