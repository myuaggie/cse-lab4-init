#ifndef yfs_client_h
#define yfs_client_h

#include <string>

#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  //lock_client *lc;
  lock_client_cache *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);

 public:
  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int writedir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  
  /** you may need to add symbolic link related methods here.*/
  int symlink(inum, const char *, const char *, inum &);
  int readlink(inum, std::string &);
  bool issymlink(inum);
  int getsymlink(inum, fileinfo &);

  bool isfile_without_lock(inum);
  bool isdir_without_lock(inum);

  int getfile_without_lock(inum, fileinfo &);
  int getdir_without_lock(inum, dirinfo &);

  int setattr_without_lock(inum, size_t);
  int lookup_without_lock(inum, const char *, bool &, inum &);
  int create_without_lock(inum, const char *, mode_t, inum &);
  int readdir_without_lock(inum, std::list<dirent> &);
  int writedir_without_lock(inum, std::list<dirent> &);
  int write_without_lock(inum, size_t, off_t, const char *, size_t &);
  int read_without_lock(inum, size_t, off_t, std::string &);
  int unlink_without_lock(inum,const char *);
  int mkdir_without_lock(inum , const char *, mode_t , inum &);
  
  /** you may need to add symbolic link related methods here.*/
  int symlink_without_lock(inum, const char *, const char *, inum &);
  int readlink_without_lock(inum, std::string &);
  bool issymlink_without_lock(inum);
  int getsymlink_without_lock(inum, fileinfo &);
};

#endif 
