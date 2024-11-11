#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call){                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {         \
      BF_PrintError(code);    \
      return HP_ERROR;        \
    }                         \
}

int add_to_newBlock(BF_Block *block, int fd, Record record) {
    HP_block_info *block_info = malloc(sizeof(*block_info));
    block_info->next_block = NULL;
    block_info->num_of_records = 1;
    
    void* data;
    CALL_BF(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
    Record* rec = data;
    rec[0] = record;
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    free(block_info);
    return 0;
}

int add_to_existingBlock(BF_Block *block, int fd, Record record, int block_capacity, int block_num) {
    HP_block_info *block_info = malloc(sizeof(*block_info));

    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));
    if(block_info->num_of_records + 1 <= block_capacity) {
        Record* rec = data;
        rec[block_info->num_of_records] = record;
        block_info->num_of_records++;
        memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
        BF_Block_SetDirty(block);
        CALL_BF(BF_UnpinBlock(block));

        free(block_info);
        return block_num;
    }

    // Είμαστε στο τελευταίο block και θα ορίσουμε καιούριο
    int num_of_blocks;
    CALL_BF(BF_GetBlockCounter(fd, &num_of_blocks));
    if((num_of_blocks + 1) > BF_BUFFER_SIZE) {
        printf("Max buffer size reached.. Cannot allocate more blocks");
        free(block_info);
        return -1;
    }
    BF_Block_Init(&(block_info->next_block));

    add_to_newBlock(block_info->next_block, fd, record);
    memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    free(block_info);
    return block_num + 1;
}

int print_records(BF_Block *block, int fd, int id, int block_capacity, int block_num, int blocks_checked) {
    HP_block_info *block_info = malloc(sizeof(*block_info));
    
    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));
    Record* rec = data;
    for(int i = 0; i < block_info->num_of_records; i++) {
        if(rec[i].id == id)
            printRecord(rec[i]);
    }
    CALL_BF(BF_UnpinBlock(block));
    blocks_checked++;
    free(block_info);
    return blocks_checked;
}

int HP_CreateFile(char *fileName){
    HP_info* info = malloc(sizeof(*info));
    BF_Block* block;
    int fd;
    BF_Block_Init(&block);
    void* data;  
    CALL_BF(BF_CreateFile(fileName))
    CALL_BF(BF_OpenFile(fileName, &fd));
    
    CALL_BF(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data, info, sizeof(*info));
    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));

    CALL_BF(BF_CloseFile(fd));

    free(info);
    return 0;
  
}

HP_info* HP_OpenFile(char *fileName){
    HP_info* info = malloc(sizeof(*info));
    int fd;
    void* data;
   
    BF_Block_Init(&info->block);
    CALL_BF(BF_OpenFile(fileName, &(info->fileDesc)));

    CALL_BF(BF_GetBlock(info->fileDesc, 0, info->block));
    data = BF_Block_GetData(info->block);

    info->fileDesc = fd;
    info->block_capacity = (BF_BLOCK_SIZE - sizeof(HP_block_info)) / sizeof(Record);
    info->last_block_id = 0;

    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data, info, sizeof(*info));

    return info ;
}

int HP_CloseFile( HP_info* hp_info ){

    void* data;

    data = BF_Block_GetData(hp_info->block);
    memcpy(data, hp_info, sizeof(*hp_info));
    BF_Block_SetDirty(hp_info->block);
    CALL_BF(BF_UnpinBlock(hp_info->block));

    BF_Block_Destroy(&(hp_info->block));
    CALL_BF(BF_CloseFile(hp_info->fileDesc));

    free(hp_info);

    return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){
    BF_Block* block;
    int block_id;
    BF_Block_Init(&block);
    
    if(hp_info->last_block_id == 0){
        add_to_newBlock(block, hp_info->fileDesc, record);
        hp_info->last_block_id++;
        BF_Block_Destroy(&block);
        return hp_info->last_block_id;
    }
    CALL_BF(BF_GetBlock(hp_info->fileDesc, hp_info->last_block_id, block));
    hp_info->last_block_id = add_to_existingBlock(block, hp_info->fileDesc, record, hp_info->block_capacity, hp_info->last_block_id);

    BF_Block_Destroy(&block);
    return hp_info->last_block_id;
}

int HP_GetAllEntries(HP_info* hp_info, int value){
    BF_Block* block;
    HP_block_info* curr_block;
    BF_Block_Init(&block);
    int* id = value;
    int block_read = 0;
    void* data;

    for(int i=1; i<= hp_info->last_block_id; i++){
        CALL_BF(BF_GetBlock(hp_info->fileDesc, i, block));
        block_read = print_records(block, hp_info->fileDesc, id, hp_info->block_capacity, i, 0);
    }

    BF_Block_Destroy(&block);
   
    return block_read;
}
