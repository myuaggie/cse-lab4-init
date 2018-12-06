#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  char buf[BLOCK_SIZE];
  for (int block_idx = IBLOCK(INODE_NUM+1, sb.nblocks); block_idx < BLOCK_NUM; block_idx++){
    blockid_t block = BBLOCK(block_idx);
    read_block(block, buf);
    if (((buf[block_idx%BPB/8]>>(block_idx%BPB%8)) & 1) == 0){
      buf[block_idx%BPB/8] |= (1 << block_idx%BPB%8);
      write_block(block, buf);
      return block_idx;
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  char buf[BLOCK_SIZE];
  if (id >= BLOCK_NUM) return;
  blockid_t block = BBLOCK(id);
  read_block(block, buf);
  buf[id%BPB/8] &= ~(1 << id%BPB%8);
  write_block(block, buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
printf("init im\n");
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  char buf[BLOCK_SIZE];
  for (int inode_idx = 1; inode_idx <= INODE_NUM; inode_idx++){
    blockid_t block = IBLOCK(inode_idx, bm->sb.nblocks);
    bm->read_block(block, buf);
    inode_t * ino = (inode_t *)buf + (inode_idx-1) % IPB;
    if (ino->type) continue;
    inode_t * inode = (inode_t *)malloc(sizeof(inode_t));
    inode->type = type;
    inode->size = 0;
    inode->atime = inode->mtime = inode->ctime = (unsigned int)time(NULL);
    put_inode(inode_idx, inode);
printf("\tim: alloc_inode %d\n", inode_idx);
    return inode_idx; 
  }
  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
printf("\tim: free_inode %d\n", inum);
  if (inum >= INODE_NUM) return;
  inode_t * inode = get_inode(inum);
  if (inode->type == 0) return;
  inode->type = 0;
  put_inode(inum, inode);
  free(inode);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + (inum-1)%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + (inum-1)%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) ((a)>(b) ? (b) : (a))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  if (inum >= INODE_NUM || buf_out == NULL || size == NULL) return;
  inode_t *inode = get_inode(inum);
  if (inode == NULL || inode->type == 0) return;
  *size = inode->size;
  *buf_out = (char*)malloc(inode->size);
  int block_num = inode->size / BLOCK_SIZE;
  int block_index = 0;
  if (block_num <= NDIRECT){
    for (; block_index < block_num; block_index++){
      blockid_t block = inode->blocks[block_index];
      bm->read_block(block, *buf_out+block_index*BLOCK_SIZE);
    }
    if (inode->size % BLOCK_SIZE != 0){
      block_num ++;
      if (block_num <= NDIRECT){
        char buf[BLOCK_SIZE];
        bm->read_block(inode->blocks[block_index], buf);
        memcpy(*buf_out+block_index*BLOCK_SIZE, buf, inode->size % BLOCK_SIZE);
      }
      else{
        blockid_t indirect_blocks[NINDIRECT];
        char buf[BLOCK_SIZE];
        bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
        bm->read_block(indirect_blocks[0], buf);
        memcpy(*buf_out+block_index*BLOCK_SIZE, buf, inode->size % BLOCK_SIZE);
      }
    }
  }
  else{
    for (; block_index < NDIRECT; block_index++){
      bm->read_block(inode->blocks[block_index], *buf_out+block_index*BLOCK_SIZE);
    }
    blockid_t indirect_blocks[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
    for (; block_index < block_num; block_index++){
      bm->read_block(indirect_blocks[block_index-NDIRECT], *buf_out+block_index*BLOCK_SIZE);
    }
    if (inode->size % BLOCK_SIZE != 0){
      char buf[BLOCK_SIZE];
      bm->read_block(indirect_blocks[block_index-NDIRECT], buf);
      memcpy(*buf_out+block_index*BLOCK_SIZE, buf, inode->size % BLOCK_SIZE);
    }
  }
  inode->atime = (unsigned int)time(NULL);
  put_inode(inum, inode);
  free(inode);

printf("\tim: read_file %d: %s\n", inum, *buf_out);
/* alloc/free blocks if needed */
  return;
}

void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  if (inum >= INODE_NUM || buf == NULL) return;
  int block_num = size / BLOCK_SIZE;
  if (size % BLOCK_SIZE != 0){
    block_num ++;
  }
  if ((unsigned int)block_num > MAXFILE) return;
  inode_t *inode = get_inode(inum);
  int old_block_num = inode->size / BLOCK_SIZE;
  if (inode->size % BLOCK_SIZE){
    old_block_num ++;
  }
  blockid_t indirect_blocks[NINDIRECT];
  if (block_num > old_block_num){
    int block_idx = old_block_num;
    if (old_block_num <= NDIRECT){
      for (; block_idx < MIN(block_num, NDIRECT); block_idx++){
        inode->blocks[block_idx] = bm->alloc_block();
      }
      if (block_num > NDIRECT){
        inode->blocks[NDIRECT] = bm->alloc_block();
        bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
        for (; block_idx < block_num; block_idx++){
          indirect_blocks[block_idx - NDIRECT] = bm->alloc_block();
        }
        bm->write_block(inode->blocks[NDIRECT], (char *)indirect_blocks);
      }
    }
    else{
      bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
      for (; block_idx < block_num; block_idx++){
        indirect_blocks[block_idx - NDIRECT] = bm->alloc_block();  
      }
      bm->write_block(inode->blocks[NDIRECT], (char *)indirect_blocks); 
    }
  }
  else{
    int block_idx = block_num;
    if (old_block_num <= NDIRECT){
      for (; block_idx < old_block_num; block_idx++){
        bm->free_block(inode->blocks[block_idx]);
      }
    }
    else{
      bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
      for (; block_idx < NDIRECT; block_idx++){
        bm->free_block(inode->blocks[block_idx]);
      }
      for (; block_idx < old_block_num; block_idx++){
        bm->free_block(indirect_blocks[block_idx - NDIRECT]);
      }
      bm->free_block(inode->blocks[NDIRECT]);
    }
  }
  char temp_buf[BLOCK_SIZE];
  int block_idx = 0;
  for (; block_idx< MIN(NDIRECT, block_num); block_idx++){
    int len = MIN(BLOCK_SIZE, size - block_idx * BLOCK_SIZE);
    memcpy(temp_buf, buf + BLOCK_SIZE * block_idx, len);
    bm->write_block(inode->blocks[block_idx], temp_buf);
  }
  if (block_idx < block_num){
    bm->write_block(inode->blocks[NDIRECT], (char *)indirect_blocks);
  }
  for (; block_idx < block_num; block_idx++){
    int len = MIN(BLOCK_SIZE, size - block_idx * BLOCK_SIZE);
    memcpy(temp_buf, buf + BLOCK_SIZE * block_idx, len);
    bm->write_block(indirect_blocks[block_idx-NDIRECT], temp_buf);
  }
  inode->size = size;
  inode->atime = inode->mtime = inode->ctime = (unsigned int)time(NULL);
  put_inode(inum, inode);
  free(inode);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  if (inum >= INODE_NUM) return;
  inode_t *inode = get_inode(inum);
  if (inode == NULL) return;
  a.type = inode->type;
  a.size = inode->size;
  a.atime = inode->atime;
  a.mtime = inode->mtime;
  a.ctime = inode->ctime;
  free(inode);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  if (inum >= INODE_NUM) return;
  inode_t *inode = get_inode(inum);
  int block_num = inode->size / BLOCK_SIZE;
  if (inode->size % BLOCK_SIZE != 0){
    block_num ++;
  }
  int block_index = 0;
  if (block_num <= NDIRECT){
    for (;block_index < block_num; block_index++){
      bm->free_block(inode->blocks[block_index]);
    }
  }
  else{
    for (;block_index < NDIRECT; block_index++){
      bm->free_block(inode->blocks[block_index]);
    }
    blockid_t indirect_blocks[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char *)indirect_blocks);
    for (;block_index < block_num; block_index++){
      bm->free_block(indirect_blocks[block_index - NDIRECT]);
    }
  }
  free_inode(inum);
  return;
}

void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
  /*
   * your code goes here.
   */
printf("im: append_block\n");
  if (inum >= INODE_NUM) return;
  inode_t *inode = get_inode(inum);
  int block_num = inode->size / BLOCK_SIZE;
  if (inode->size % BLOCK_SIZE != 0){
    block_num ++;
  }
  blockid_t indirect_blocks[NINDIRECT];
  if (block_num < NDIRECT){
    bid = bm->alloc_block();
    inode->blocks[block_num] = bid;  
  }
  else if (block_num == NDIRECT){
    inode->blocks[NDIRECT] = bm->alloc_block();
    bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
    bid = bm->alloc_block();
    indirect_blocks[0] = bid;
    bm->write_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
  }
  else {
    bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
    bid = bm->alloc_block();
    indirect_blocks[block_num-NDIRECT] = bid;
    bm->write_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
  }
  inode->size += BLOCK_SIZE;
  put_inode(inum, inode);
  return;
}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  /*
   * your code goes here.
   */
printf("im: get_block_ids\n");
  if (inum >= INODE_NUM) return;
  inode_t *inode = get_inode(inum);
  int block_num = inode->size / BLOCK_SIZE;
  if (inode->size % BLOCK_SIZE != 0){
    block_num ++;
  }
  blockid_t indirect_blocks[NINDIRECT];
  if (block_num <= NDIRECT){
    for (int i=0;i<block_num;i++){
      block_ids.push_back(inode->blocks[i]);
printf("%d\n", inode->blocks[i]);
    }
  }
  else {
    int block_idx;
    for (block_idx=0;block_idx<NDIRECT;block_idx++){
      block_ids.push_back(inode->blocks[block_idx]);
printf("%d\n", inode->blocks[block_idx]);
    }
    bm->read_block(inode->blocks[NDIRECT], (char*)indirect_blocks);
    for (; block_idx<block_num;block_idx++){
      block_ids.push_back(indirect_blocks[block_idx-NDIRECT]);
printf("%d\n", indirect_blocks[block_idx-NDIRECT]);
    }
  }
  return;
}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */
/*  if (id<0 || id >= BLOCK_NUM || buf == NULL) return;
  memcpy(buf, disk::blocks[id], BLOCK_SIZE);
  return;*/
printf("im: read_block\n");
  bm->read_block(id, buf);
}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */
/*  if (id<0 || id >= BLOCK_NUM || buf == NULL) return;
  memcpy(disk::blocks[id], buf, BLOCK_SIZE);
  return;*/
printf("im: write_block\n");
  bm->write_block(id, buf);
}

void
inode_manager::complete(uint32_t inum, uint32_t size)
{
  /*
   * your code goes here.
   */
printf("im: complete\n");
  if (inum >= INODE_NUM) return;
  inode_t *inode = get_inode(inum);
  inode->size = size;
  inode->mtime = (unsigned int)time(NULL);
  put_inode(inum, inode);
  return;
}
