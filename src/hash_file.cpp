#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cstdint>
#include "bf.h"
#include "hash_file.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

#define CALL_OR_DIE(call)   \
{                           \
  HT_ErrorCode code = call; \
  if (code != HT_OK) {      \
    printf("Error\n");      \
    exit(code);             \
  }                         \
}

int* DoubleHashTable(int* hashTable, int depth);                                          // Doubles hashtable, rearenge table to new depth
int* SplitBucketAndReareange(int* hashTable, int index, int global_depth, int newblock);  // Splits bucket, empties first and adds second
HT_ErrorCode PrintAllBlocks(int indexDesc);                                               // Debug method, prints all blocks
HT_ErrorCode LoadHashtableFromDrive(int filedesc);                                        // Loads Hashtable from drive
HT_ErrorCode SaveHashtabletoDrive(int filedesc, char* hashTable);                         // Saves hashtable to drive, overwrites existing

static FileInfo* OpenFiles[BF_MAX_OPEN_FILES];
static int OpenFilesCount;

unsigned int hash_function(unsigned int key, int depth) 
{
  // A simple hash transformation using a prime number multiplier
  unsigned int hash_code = key * 2654435761u;
  
  // Calculate the number of bits to shift
  int shift = 32 - depth;  // Assuming 32-bit unsigned int

  // Shift to use the most significant 'depth' bits
  return hash_code >> shift;
}

HT_ErrorCode HT_Init() 
{
  for (int i = 0; i < BF_MAX_OPEN_FILES; i++)
  {
    OpenFiles[i] = NULL;
  }

  OpenFilesCount = 0;
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char* fileName, int depth) 
{
  for (int i = 0; i < OpenFilesCount; i++)
  {
    if ((OpenFiles[i] != NULL) && strcmp(OpenFiles[i]->fileName, fileName) == 0) // Check for duplicate
    {
      printf("File %s, already exists!\n", fileName);
      return HT_ERROR;
    }
  }

  int fd1;
  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &fd1));

  CALL_BF(BF_AllocateBlock(fd1, block)); // Allocate first block
  char* data = BF_Block_GetData(block);

  // Init HT_info of file
  HT_info* info = (HT_info*)data;
  info->fileType = HASH;
  info->entries_per_block = ((BF_BLOCK_SIZE - sizeof(HT_Block_info)) / sizeof(Record));
  info->global_depth = depth;
  info->hashtableDiscBlocksNum = 0;
  info->entries_count = 0;
  info->hashtableEntriesBlockNum = 0;

  int hashTableSize = pow(2, depth);
  int hashTableDataSize = sizeof(int) * hashTableSize;

  int* hashtable = (int*)malloc(hashTableDataSize); // Allocate table in memmory
  for(int i = 0; i < hashTableSize; i++) // Init table
  {
    *(hashtable + i) = -1; // Empty bucket
  }

  SaveHashtabletoDrive(fd1, (char*)hashtable); // Save table to drive
  free(hashtable); // Free from memmory
  
  CALL_BF(BF_GetBlock(fd1, 0, block));
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(fd1));

  printf("HT_CreateIndex was successful for file %s\n", fileName);
  return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char* fileName, int* indexDesc)
{
  if (OpenFilesCount >= BF_MAX_OPEN_FILES) // Check for Max Number of files
  {
    printf("Error opening file %s, can't open more than %d files!", fileName, BF_MAX_OPEN_FILES);
    return HT_ERROR;
  }

  int fd1;
  CALL_BF(BF_OpenFile(fileName, &fd1));

  int index = -1;
  for (int i = 0; i < BF_MAX_OPEN_FILES; i++)
  {
    if (OpenFiles[i] == NULL) // Instert in first empty available
    {
      index = i;
      break;
    }
  }

  OpenFiles[index] = (FileInfo*)malloc(sizeof(FileInfo)); //Init file entry in OpenFiles array
  OpenFiles[index]->fileDesc = fd1;
  OpenFiles[index]->fileName = (char*)malloc(sizeof(char) * (strlen(fileName) + 1));
  strcpy(OpenFiles[index]->fileName, fileName);

  LoadHashtableFromDrive(fd1); // Load hashtable from drive

  *indexDesc = index;
  OpenFilesCount++;
  return HT_OK;
}

HT_ErrorCode HT_CloseFile(int indexDesc)
{
  if (OpenFilesCount <= 0)
  {
    printf("Error closing file, no files are open!");
    return HT_ERROR;
  }

  if (OpenFiles[indexDesc] == NULL) // Check for existance
  {
    printf("Error, file is not open");
    return HT_ERROR;
  }

  BF_Block *block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  SaveHashtabletoDrive(OpenFiles[indexDesc]->fileDesc, (char*)info->hashtable); // Save table to drive before closing
  free(info->hashtable); // Free from memmory

  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);


  CALL_BF(BF_CloseFile(OpenFiles[indexDesc]->fileDesc));
  free(OpenFiles[indexDesc]->fileName); // Remove from OpenFiles array and free file info that was kept
  free(OpenFiles[indexDesc]);
  OpenFiles[indexDesc] == NULL;
  OpenFilesCount--;

  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record)
{
  if (OpenFiles[indexDesc] == NULL)
  {
    printf("Error, file is not open");
    return HT_ERROR;
  }

  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  int insertIndex = hash_function(record.id, info->global_depth); // Get index of hashtable
  
  // if there is no bucket (bucket is empty)
  if (info->hashtable[insertIndex] == -1)
  {
    CALL_BF(BF_AllocateBlock(OpenFiles[indexDesc]->fileDesc, block)); // Create bucket
    info->hashtableEntriesBlockNum++;                                 // Increase data blocks
    Record* data = (Record*)BF_Block_GetData(block);

    memcpy((void*)data, (void*)&record, sizeof(record));  // Write record to block
    data += info->entries_per_block;                      // Move pointer to HT_Block_info

    ((HT_Block_info*)data)->entries_num = 1;                    // Set HT_Block_info
    ((HT_Block_info*)data)->local_depth = info->global_depth;

    int blockNum;
    CALL_BF(BF_GetBlockCounter(OpenFiles[indexDesc]->fileDesc, &blockNum)); // Get block index
    info->hashtable[insertIndex] = blockNum - 1;                            // Write to hashtable
    info->entries_count++;  // Increase record entries

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    BF_Block_Destroy(&block);
  }
  else  // if bucket is not empty
  {
    CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, info->hashtable[insertIndex], block));
    Record* data = (Record*)BF_Block_GetData(block);

    Record* dataHandler = data;
    dataHandler += info->entries_per_block;
    int entries = ((HT_Block_info*)dataHandler)->entries_num;     // Get block entries
    int local_depth = ((HT_Block_info*)dataHandler)->local_depth; // Get block local depth

    if (entries < info->entries_per_block) // If bucket not full
    {
      data += entries; // Move pointer to free space for new entry
      memcpy((void*)data, (void*)&record, sizeof(record)); // Write entry
      ((HT_Block_info*)dataHandler)->entries_num++; // Update HT_Block_info
      info->entries_count++; // Increase record entries

      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));

      CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
    }
    else // If bucket is full
    {
      if (local_depth == info->global_depth) // Table needs to double
      {
        Record recordsToInsert[info->entries_per_block];
        for (int i = 0; i < info->entries_per_block; i++) // Keep bucket's entries to rehash them
        {
          recordsToInsert[i] = *(data + i);
        }
        info->entries_count -= info->entries_per_block; // Remove their count

        ((HT_Block_info*)dataHandler)->entries_num = 0; // Delete them (already saved above in recordsToInsert array)
        ((HT_Block_info*)dataHandler)->local_depth++;   // Increase local depth

        info->hashtable = DoubleHashTable(info->hashtable, info->global_depth); // Double table

        info->hashtable[(2 * insertIndex) + 1] = -1; // Mark next as empty (we use recursion, so no need to add the bucket now, just create the space)
        info->global_depth++; // Increase global depth, because array is doubled above

        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        for (int i = 0; i < info->entries_per_block; i++) // Rehash removed entries
        {
          HT_InsertEntry(indexDesc, recordsToInsert[i]);
        }
        HT_InsertEntry(indexDesc, record); // Insert record
      }
      else if (local_depth < info->global_depth) // Bucket can be splitted without increasing array
      {
        Record recordsToInsert[info->entries_per_block];
        for (int i = 0; i < info->entries_per_block; i++) // Same as above, save entries to array
        {
          recordsToInsert[i] = *(data + i);
        }
        info->entries_count -= info->entries_per_block; // Remove their count

        ((HT_Block_info*)dataHandler)->entries_num = 0; // Empty bucket
        ((HT_Block_info*)dataHandler)->local_depth++;   // Increase local depth

        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        // Allocate new bucket for the Split (here it is needed because we have to split hashtable block pointing integers between the two buckets).
        CALL_BF(BF_AllocateBlock(OpenFiles[indexDesc]->fileDesc, block));
        info->hashtableEntriesBlockNum++; // Increase data blocks
        Record* data = (Record*)BF_Block_GetData(block);

        data += info->entries_per_block;
        ((HT_Block_info*)data)->entries_num = 0; // Empty bucket
        ((HT_Block_info*)data)->local_depth = local_depth + 1; // 1 more than initial bucket's local depth

        int blockNum;
        CALL_BF(BF_GetBlockCounter(OpenFiles[indexDesc]->fileDesc, &blockNum)); // Get block index

        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        info->hashtable = SplitBucketAndReareange(info->hashtable, insertIndex, info->global_depth, blockNum - 1); // Split buckets, fix block pointing integers and add new bucket

        for (int i = 0; i < info->entries_per_block; i++)
        {
          HT_InsertEntry(indexDesc, recordsToInsert[i]);
        }
        HT_InsertEntry(indexDesc, record);
      }
    }

    BF_Block_Destroy(&block);
  }

  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id)
{
  if (OpenFiles[indexDesc] == NULL)
  {
    printf("Error printing, file is not open");
    return HT_ERROR;
  }

  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  int entriesCount = info->entries_count;

  if (id == NULL) // If print all
  {
    printf("Entries Count: %d\n", entriesCount);
    for (int i = 0; i < entriesCount; i++)
    {
      int index = hash_function(i, info->global_depth); // Find index
      CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, info->hashtable[index], block));
      Record* data = (Record*)BF_Block_GetData(block); // Get block data

      Record* dataHandler = data;
      dataHandler += info->entries_per_block;
      int entries = ((HT_Block_info*)dataHandler)->entries_num;

      for (int j = 0; j < entries; j++) // Print all block entries
      {
        Record rec = *(data + j);
        if (rec.id == i)
        {
          printf("%d %s %s %s \n", rec.id, rec.name, rec.surname, rec.city);
        }
      }
      CALL_BF(BF_UnpinBlock(block));
    }

  }
  else // Print only matching id
  {
    int i = *id;
    int index = hash_function(i, info->global_depth);
    CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, info->hashtable[index], block));
    Record* data = (Record*)BF_Block_GetData(block);

    Record* dataHandler = data;
    dataHandler += info->entries_per_block;
    int entries = ((HT_Block_info*)dataHandler)->entries_num;

    for (int j = 0; j < entries; j++)
    {
      Record rec = *(data + j);
      if (rec.id == i) // Add id check
      {
        printf("%d %s %s %s \n", rec.id, rec.name, rec.surname, rec.city);
      }
    }
    CALL_BF(BF_UnpinBlock(block));
  }
  
  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode HashStatistics(const char* filename)
{
  int indexDesc;
  HT_OpenIndex(filename, &indexDesc);

  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  int lenght = pow(2, info->global_depth);

  printf("\nData Blocks: %d\n", info->hashtableEntriesBlockNum); // We count on insertion, so just print

  double entriesPerBlockAverage = (double)info->entries_count / info->hashtableEntriesBlockNum;
  printf("Entries Per Block Average: %f\n", entriesPerBlockAverage);  // We also count entries so just divide and print 

  int maxEntries = -1;
  int minEntries = INT32_MAX;

  for (int i = 0; i < lenght; i++) // Find max amd min
  {
    if (info->hashtable[i] == -1)
    {
      continue;
    }

    CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, info->hashtable[i], block));
    Record* data = (Record*)BF_Block_GetData(block);

    Record* dataHandler = data;
    dataHandler += info->entries_per_block;
    int entries = ((HT_Block_info*)dataHandler)->entries_num;

    if (entries > maxEntries)
    {
      maxEntries = entries;
    }
    if (entries < minEntries)
    {
      minEntries = entries;
    }

    CALL_BF(BF_UnpinBlock(block));
  }

  printf("Min Entries: %d\n", minEntries);
  printf("Max Entries: %d\n", maxEntries);

  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&block);
  CALL_OR_DIE(HT_CloseFile(indexDesc));
  return HT_OK;
}

// Helper functions

HT_ErrorCode PrintAllBlocks(int indexDesc)
{
  if (OpenFiles[indexDesc] == NULL)
  {
    printf("Error printing, file is not open");
    return HT_ERROR;
  }

  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  int lenght = pow(2, info->global_depth);

  printf("\nGlobal depth: %d\n", info->global_depth);

  for (int i = 0; i < lenght; i++)
  {
    printf("\nIndex %d, Block %d", i, info->hashtable[i]);
    if (info->hashtable[i] == -1)
    {
      printf("\n");
      continue;
    }

    CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, info->hashtable[i], block));
    Record* data = (Record*)BF_Block_GetData(block);

    Record* dataHandler = data;
    dataHandler += info->entries_per_block;
    int entries = ((HT_Block_info*)dataHandler)->entries_num;
    int local_depth = ((HT_Block_info*)dataHandler)->local_depth;

    printf(", Local depth: %d\n", local_depth);
    for (int j = 0; j < entries; j++)
    {
      Record rec = *(data + j);
      printf("%d %s %s %s \n", rec.id, rec.name, rec.surname, rec.city);
    }

    CALL_BF(BF_UnpinBlock(block));
  }

  CALL_BF(BF_GetBlock(OpenFiles[indexDesc]->fileDesc, 0, block));
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&block);
  return HT_OK;
}

int* DoubleHashTable(int* hashTable, int depth)
{
  int newHashTableCount = pow(2, depth + 1); // Calc new size
  int* newHashTable = (int*)realloc(hashTable, sizeof(int) * newHashTableCount); // Reallocate

  if (newHashTable == NULL) // Check for failed realloc
  {
    printf("Error. Realloc failed");
    return NULL;
  }

  // Copy elements to new positions
  for (int i = (newHashTableCount / 2) - 1; i >= 0; i--)
  {
    newHashTable[2 * i] = newHashTable[i];
    newHashTable[(2 * i) + 1] = newHashTable[i];
  }

  return newHashTable;
}

int* SplitBucketAndReareange(int* hashTable, int index, int global_depth, int newblock)
{
  int hashTableCount = pow(2, global_depth); // Calc table count
  int start = index;
  int end = index;

  while (1) // Find first same index
  {
    if (start > 0 && hashTable[start - 1] == hashTable[index])
    {
      start--;
    }
    else
    {
      break;
    }
  }

  while (1) // Find last same index
  {
    if (end < (hashTableCount - 1) && hashTable[end + 1] == hashTable[index])
    {
      end++;
    }
    else
    {
      break;
    }
  }

  int range = end - start + 1; // Calc range
  for (int i = start; i < start + (range / 2); i++) // Split indexes between old and new bucket
  {
    hashTable[i] = hashTable[index];
  }
  for (int i = start + (range / 2); i <= end; i++)
  {
    hashTable[i] = newblock;
  }

  return hashTable;
}

HT_ErrorCode LoadHashtableFromDrive(int filedesc)
{
  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(filedesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  int hashTableSize = pow(2, info->global_depth);

  char* hashtable = (char*)malloc(sizeof(int) * hashTableSize); // Allocate memmory for table

  int next_block = info->hashtableDiscFirstBlock; // First block that contains hashtable on disc
  int bytes_loaded = 0; // Keep track of bytes loaded
  for (int i = 0; i < info->hashtableDiscBlocksNum; i++)
  {
    CALL_BF(BF_GetBlock(filedesc, next_block, block));
    char* data = BF_Block_GetData(block);

    if (i < info->hashtableDiscBlocksNum - 1) // If not last block
    {
      memcpy(hashtable + bytes_loaded, (void*)data, (BF_BLOCK_SIZE - sizeof(int)));
      next_block = *((int*)(data + (BF_BLOCK_SIZE - sizeof(int))));
      bytes_loaded += (BF_BLOCK_SIZE - sizeof(int));
    }
    else // Last block
    {
      memcpy(hashtable + bytes_loaded, (void*)data, (sizeof(int) * hashTableSize) - bytes_loaded);
    }

    CALL_BF(BF_UnpinBlock(block));
  }

  info->hashtable = (int*)hashtable;

  CALL_BF(BF_GetBlock(filedesc, 0, block));
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  BF_Block_Destroy(&block);
  return HT_OK;
}

HT_ErrorCode SaveHashtabletoDrive(int filedesc, char* hashTable)
{
  BF_Block *block;
  BF_Block_Init(&block);

  CALL_BF(BF_GetBlock(filedesc, 0, block));
  HT_info* info = (HT_info*)BF_Block_GetData(block);

  int hashTableSize = pow(2, info->global_depth);
  int blocksAlreadyUsedNum = info->hashtableDiscBlocksNum;

  int hashTableDataSize = sizeof(int) * hashTableSize;
  int blocks_needed = hashTableDataSize / (BF_BLOCK_SIZE - sizeof(int)); // Total blocks needed to save hashtable to drive
  if (hashTableDataSize % (BF_BLOCK_SIZE - sizeof(int)) != 0)
  {
    blocks_needed++;  // if there is remaining data, we need 1 more block
  }
  info->hashtableDiscBlocksNum = blocks_needed; // Update file info

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  int newBlocksNum = blocks_needed - blocksAlreadyUsedNum; // New blocks needed (if array is bigger than when loaded)
  int blocks[newBlocksNum]; // New blocks indexes
  if (blocks_needed > blocksAlreadyUsedNum) // Allocate beforehand so it is easier to link next block at the end of each block
  {
    for (int i = 0; i < newBlocksNum; i++)
    {
      CALL_BF(BF_AllocateBlock(filedesc, block));
      int blockNum;
      CALL_BF(BF_GetBlockCounter(filedesc, &blockNum));
      blocks[i] = blockNum - 1;

      CALL_BF(BF_UnpinBlock(block));
    }
  }

  int bytes_written = 0; // Keep track of bytes written
  int next_block = info->hashtableDiscFirstBlock; // First block (if blocksAlreadyUsedNum == 0 it will not be used, as it is not initiallized)
  for (int i = 0; i < blocksAlreadyUsedNum; i++)  // If there are existing blocks, overwrite them
  {
    CALL_BF(BF_GetBlock(filedesc, next_block, block));
    char* data = BF_Block_GetData(block);

    if (i < blocksAlreadyUsedNum - 1) // If not last existing block
    {
      memcpy((void*)data, hashTable + bytes_written, (BF_BLOCK_SIZE - sizeof(int)));
      next_block = *((int*)(data + (BF_BLOCK_SIZE - sizeof(int))));
      bytes_written += (BF_BLOCK_SIZE - sizeof(int));
    }
    else if (newBlocksNum > 0) // If last existing block more will be needed
    {
      memcpy((void*)data, hashTable + bytes_written, (BF_BLOCK_SIZE - sizeof(int)));
      bytes_written += (BF_BLOCK_SIZE - sizeof(int));
      memcpy((void*)(data + (BF_BLOCK_SIZE - sizeof(int))), (void*)&blocks[0], sizeof(int)); // next block must be linked at the end
    }
    else  // If last existing block and no other we be needed (no need for next to be written)
    {
      memcpy(data, hashTable + bytes_written, hashTableDataSize - bytes_written);
      bytes_written += (BF_BLOCK_SIZE - sizeof(int));
    }

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }

  for (int i = 0; i < newBlocksNum; i++) // If more blocks are needed write them and link them
  {
    CALL_BF(BF_GetBlock(filedesc, blocks[i], block));
    char* data = BF_Block_GetData(block);

    if (i < blocks_needed - 1) // If not last block
    {
      memcpy(data, hashTable + bytes_written, BF_BLOCK_SIZE - sizeof(int));
      bytes_written += BF_BLOCK_SIZE - sizeof(int);
    }
    else
    {
      memcpy(data, hashTable + bytes_written, hashTableDataSize - bytes_written);
    }
    
    if (blocksAlreadyUsedNum == 0 && i == 0) // If first block used link to file info
    {
      info->hashtableDiscFirstBlock = blocks[0];
    }
    if (i < blocks_needed - 1) // If not last block link next
    {
      memcpy(data + (BF_BLOCK_SIZE - sizeof(int)), (void*)&blocks[i + 1], sizeof(int));
    }   

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
  }
  
  BF_Block_Destroy(&block);
  return HT_OK;
}