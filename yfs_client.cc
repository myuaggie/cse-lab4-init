// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
//  lc = new lock_client(lock_dst);
  lc = new lock_client_cache(lock_dst);
  lc->acquire(1);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
  lc->release(1);
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile_without_lock(inum inum)
{
    extent_protocol::attr a;
    //lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
      //  lc->release(inum);
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        //lc->release(inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    //lc->release(inum);
    return false;
}

bool
yfs_client::isfile(inum inum)
{
    lc->acquire(inum);
    bool ret = isfile_without_lock(inum);
    lc->release(inum);
    return ret;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir_without_lock(inum inum)
{
printf("isdir\n");
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;
    //lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
      //  lc->release(inum);
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
       // lc->release(inum);
	return true;
    } 
    //printf("isdir: %lld is a file\n", inum);
    //lc->release(inum);
    return false;
}

bool
yfs_client::isdir(inum inum)
{
    lc->acquire(inum);
    bool ret = isdir_without_lock(inum);
    lc->release(inum);
    return ret;
}

int
yfs_client::getfile_without_lock(inum inum, fileinfo &fin)
{
    int r = OK;
    //lc->acquire(inum);
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);
release:
   // lc->release(inum);
    return r;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    lc->acquire(inum);
    int ret = getfile_without_lock(inum, fin);
    lc->release(inum);
    return ret;
}

int
yfs_client::getdir_without_lock(inum inum, dirinfo &din)
{
    int r = OK;
    //lc->acquire(inum);
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
   // lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    lc->acquire(inum);
    int ret = getdir_without_lock(inum, din);
    lc->release(inum);
    return ret;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr_without_lock(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    if (ino < 0 || ino >= INODE_NUM) return IOERR;
   // lc->acquire(ino);
    std::string content;
    EXT_RPC(ec->get(ino, content));
    content.resize(size, '\0');
    EXT_RPC(ec->put(ino, content));
release:
   // lc->release(ino);
    return r;
}

int
yfs_client::setattr(inum ino, size_t size)
{
    lc->acquire(ino);
    int ret = setattr_without_lock(ino, size);
    lc->release(ino);
    return ret;
}

int
yfs_client::create_without_lock(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;	
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    if (!isdir_without_lock(parent) || name==NULL) return IOERR;
    //check filename validation.
    std::list<dirent> childlist;
    //lc->acquire(parent);
    if ((r=readdir_without_lock(parent, childlist)) != OK) {
    // lc->release(parent);
      return r;
    }
    for (std::list<dirent>::iterator it =childlist.begin(); it != childlist.end(); it++){
      if (it->name == std::string(name)){
	//lc->release(parent);
        return EXIST;
      }
    }
    dirent ent;
    ent.name = std::string(name);
    ec->create(extent_protocol::T_FILE, ino_out);
 
    ent.inum =  ino_out;
    childlist.push_back(ent);
    r=writedir_without_lock(parent, childlist);
   // lc->release(parent);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int ret = create_without_lock(parent, name, mode, ino_out);
    lc->release(parent);
    return ret;
}

int
yfs_client::mkdir_without_lock(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    if (!isdir_without_lock(parent) || name==NULL) return IOERR;
    std::list<dirent> childlist;
    //lc->acquire(parent);
    if ((r=readdir_without_lock(parent, childlist)) != OK) {
	//lc->release(parent);
	return r;
    }
    for (std::list<dirent>::iterator it = childlist.begin(); it != childlist.end(); it++){
      if (it->name == std::string(name)){
	//lc->release(parent);
	return EXIST;
      }
    }
    dirent ent;
    ent.name = std::string(name);
    ec->create(extent_protocol::T_DIR, ino_out);
    ent.inum = ino_out;
    childlist.push_back(ent);
    if ((r=writedir_without_lock(parent, childlist)) != OK){
	//lc->release(parent);
	return r;
    }
   // lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int ret = mkdir_without_lock(parent, name, mode, ino_out);
    lc->release(parent);
    return ret;
}

int
yfs_client::lookup_without_lock(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    if (!isdir_without_lock(parent) || name==NULL) return IOERR;
    std::string filename(name);
    // check filename validation.
    std::list<dirent> childlist;
    found = false;
    if ((r = readdir_without_lock(parent, childlist)) != OK) {
	return r;
    }
    
    for (std::list<dirent>::iterator it = childlist.begin(); it != childlist.end(); it++){
      if (it->name == filename){
        found = true;
        ino_out = it->inum;
        break;
      }
    }
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    lc->acquire(parent);
    int ret = lookup_without_lock(parent, name, found, ino_out);
    lc->release(parent);
    return ret;
}

int
yfs_client::readdir_without_lock(inum dir, std::list<dirent> &list)
{
    int r = OK;
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    if (!isdir_without_lock(dir)) return IOERR;
    std::string buf;
    std::istringstream ss;
    char ch;
    ec->get(dir, buf);
    if (buf.size() == 0) { return r; }
    ss.str(buf);
    int32_t total;
    ss.read((char *)&total, sizeof(int32_t));
    while (total > 0){
      ss.get(ch);
      uint8_t filename_size = (uint8_t)ch;
      char temp_buf[filename_size];
      ss.read(temp_buf, filename_size);
      dirent ent;
      ent.name = std::string(temp_buf, filename_size);
      inum inode;
      ss.read((char *)&inode, sizeof(inum));
      ent.inum = inode;
      list.push_back(ent);
      total --;
    }
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    lc->acquire(dir);
    int ret = readdir_without_lock(dir, list);
    lc->release(dir);
    return ret;
}

int
yfs_client::writedir_without_lock(inum dir, std::list<dirent> &list)
{
    int r = OK;
    if (!isdir_without_lock(dir)) return IOERR;
   std::ostringstream ss;
   int its = 0;
   for (std::list<dirent>::iterator it = list.begin(); it!=list.end(); it++){
     its ++;
   }
   ss.write((char *)&its, sizeof(int32_t));
   for (std::list<dirent>::iterator it = list.begin(); it!=list.end(); it++){
     ss.put((char)(uint8_t)it->name.length());
     ss.write(it->name.c_str(), it->name.length());
     ss.write((char *)&it->inum, sizeof(inum));
   }
   EXT_RPC(ec->put(dir, ss.str()));
release:
    return r;
}

int
yfs_client::writedir(inum dir, std::list<dirent> &list)
{
    lc->acquire(dir);
    int ret = writedir_without_lock(dir, list);
    lc->release(dir);
    return ret;
}

int
yfs_client::read_without_lock(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    if (!isfile_without_lock(ino)) return IOERR;
    std::string content;
   // lc->acquire(ino);
    ec->get(ino, content);
    size_t len = size;
    if ((size_t)off >= content.size()){
      data = "";
    //  lc->release(ino);
      return r;
    }
    if ((size+off) > content.size()){
      len = content.size() - off;
    }
    data = content.substr(off, len);
   // lc->release(ino);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    lc->acquire(ino);
    int ret = read_without_lock(ino, size, off, data);
    lc->release(ino);
    return ret;
}

int
yfs_client::write_without_lock(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    if (!isfile_without_lock(ino)) return IOERR;
    std::string content;
   // lc->acquire(ino);
    ec->get(ino, content);
    size_t len = content.size();
    std::string text;
    if ((size_t)off < len){
      if (((size_t)off + size) >= len){
        text = content.substr(0, off);
        text += std::string(data, size);
        ec->put(ino, text.substr(0, off+size));
        bytes_written = size;
      }
      else{
        text = content.substr(0, off);
        text += std::string(data, size);
        text += content.substr(off+size, len-off-size);
        ec->put(ino, text.substr(0, len));
        bytes_written = size;
      }
    }
    else if (off >(off_t)len){
      text = content;
      text += std::string(off-len, 0);
      text += std::string(data, size);
      ec->put(ino, text);
      std::string test;
      ec->get(ino,test);
      bytes_written = size + off -len;
    }
    else {
      text = content + std::string(data, size);
       ec->put(ino, text.substr(0, off+size));
      bytes_written = size;
    }
  // lc->release(ino);
   return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
                  size_t &bytes_written)
{
    lc->acquire(ino);
    int ret = write_without_lock(ino, size, off, data, bytes_written);
    lc->release(ino);
    return ret;
}

int yfs_client::unlink_without_lock(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    if (!isdir_without_lock(parent) || name == NULL) return IOERR;
    std::list<dirent> childlist;
    inum ino;
   // lc->acquire(parent);
    if ((r=readdir_without_lock(parent, childlist)) != OK) {
       // lc->release(parent);
        return r;
    }
    for (std::list<dirent>::iterator it =childlist.begin(); it != childlist.end(); it++){
      if (it->name == std::string(name)){
        childlist.erase(it);
        ino = it->inum;
        break;
      }
    }
    writedir_without_lock(parent, childlist);
    ec->remove(ino);
   // lc->release(parent);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    lc->acquire(parent);
    int ret = unlink_without_lock(parent, name);
    lc->release(parent);
    return ret;
}

int yfs_client::symlink_without_lock(inum parent, const char * name, const char *link, inum &ino_out){
  int r= OK;
  if (!isdir_without_lock(parent) || name == NULL || link == NULL) return IOERR;
  std::list<dirent> childlist;
  //lc->acquire(parent);
  if ((r=readdir_without_lock(parent, childlist)) != OK) {
      //lc->release(parent);
      return r;
  }
  for (std::list<dirent>::iterator it =childlist.begin(); it != childlist.end(); it++){
      if (it->name == std::string(name)){
       //  lc->release(parent);
         return EXIST;
      }
  }
  ec->create(extent_protocol::T_SYMLINK, ino_out);
  dirent ent;
  ent.name = std::string(name);
  ent.inum = ino_out;
  childlist.push_back(ent);
  writedir_without_lock(parent, childlist);
  ec->put(ino_out, std::string(link));
 // lc->release(parent);
  return r;
}

int yfs_client::symlink(inum parent, const char * name, const char *link, inum &ino_out){
    lc->acquire(parent);
    int ret = symlink_without_lock(parent, name, link, ino_out);
    lc->release(parent);
    return ret;
}

int yfs_client::readlink_without_lock(inum ino, std::string & link){
  if (!issymlink_without_lock(ino)) return IOERR;
 // lc->acquire(ino);
  ec->get(ino, link);
  //lc->release(ino);
  return OK;
}

int yfs_client::readlink(inum ino, std::string & link){
    lc->acquire(ino);
    int ret = readlink_without_lock(ino, link);
    lc->release(ino);
    return ret;
}

bool
yfs_client::issymlink_without_lock(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;
  //  lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
     //   lc->release(inum);
	return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
      //  lc->release(inum);
	return true;
    }
    //printf("isdir: %lld is a file\n", inum);
   // lc->release(inum);
    return false;
}

bool
yfs_client::issymlink(inum inum)
{
    lc->acquire(inum);
    bool ret = issymlink_without_lock(inum);
    lc->release(inum);
    return ret;
}

int yfs_client::getsymlink_without_lock(inum inum, fileinfo & fin){
    int r = OK;

    printf("getsymlink %016llx\n", inum);
    extent_protocol::attr a;
   // lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getsymlink %016llx -> sz %llu\n", inum, fin.size);

release:
   // lc->release(inum);
    return r;

}

int yfs_client::getsymlink(inum inum, fileinfo & fin){
    lc->acquire(inum);
    bool ret = getsymlink_without_lock(inum, fin);
    lc->release(inum);
    return ret;
}
