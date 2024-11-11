#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call) {     \
    BF_ErrorCode code = call;   \
    if (code != BF_OK) {        \
        BF_PrintError(code);    \
        exit(code);             \
    }                           \
}                               \

int hash(int key, int table_size) {
    int value = 0;

    for (int i = 0; i < key; ++i)
        value = value + i * 37;

    return value % table_size;
}

// Αρχικοποιεί ένα block και του προσθέτει ένα record
int add_record_to_newBlock(BF_Block *block, int fd, Record record) {
    HT_block_info *block_info = malloc(sizeof(*block_info));
    block_info->next_block = NULL;
    block_info->record_count = 1;
    
    void* data;
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
    Record* rec = data;
    rec[0] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    free(block_info);
    return 0;
}

// Προσθέτει ένα record στο block εφόσον χωράει αλλίως το προσθέτει σε block υπερχείλισης (εάν δεν υπάρχει ορίζει ένα)
int add_record_to_existingBlock(BF_Block *block, int fd, Record record, int block_capacity, int block_num) {
    HT_block_info *block_info = malloc(sizeof(*block_info));
    
    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));

    if(block_info->record_count + 1 <= block_capacity) {
        Record* rec = data;
        rec[block_info->record_count] = record;
        block_info->record_count++;
        memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));

        free(block_info);
        return block_num;
    }

    // Άμα δεν έχει χώρο αυτό το block προσθέτουμε το record στο block υπερχείλισης
    if(block_info->next_block != NULL) {
        block_num++;
        block_num = add_record_to_existingBlock(block_info->next_block, fd, record, block_capacity, block_num);
        CALL_OR_DIE(BF_UnpinBlock(block));

        free(block_info);
        return block_num;
    }

    // Άμα δεν έχουμε αναθέσει block υπερχείλισης πρώτα αναθέτουμε το επόμενο block
    int num_of_blocks;
    CALL_OR_DIE(BF_GetBlockCounter(fd, &num_of_blocks));
    if(block_num < num_of_blocks - 1) {
        BF_Block_Init(&(block_info->next_block));
        block_num++;
        CALL_OR_DIE(BF_GetBlock(fd, block_num, block_info->next_block));

        block_num = add_record_to_existingBlock(block_info->next_block, fd, record, block_capacity, block_num);
        memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));

        free(block_info);
        return block_num;
    }

    // Άμα είμαστε στο τελευταίο block θα ορίσουμε καιούριο για block υπερχείλισης
    if((num_of_blocks + 1) > BF_BUFFER_SIZE) {
        printf("Max buffer size reached.. Cannot allocate more blocks");
        free(block_info);
        return -1;
    }
    BF_Block_Init(&(block_info->next_block));

    add_record_to_newBlock(block_info->next_block, fd, record);
    memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    free(block_info);
    return block_num + 1;
}

// Εμφανίζει τα στοιχεία των records με το συγκεκριμένο id 
int print_records(BF_Block *block, int fd, int id, int block_capacity, int block_num, int blocks_checked) {
    HT_block_info *block_info = malloc(sizeof(*block_info));
    
    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));
    Record* rec = data;
    for(int i = 0; i < block_info->record_count; i++) {
        if(rec[i].id == id)
            printRecord(rec[i]);
    }
    CALL_OR_DIE(BF_UnpinBlock(block));
    blocks_checked++;

    // Ελένχουμε και το block υπερχείλισης για records με αυτό το id
    if(block_info->next_block != NULL) {
        block_num++;
        block_num = print_records(block_info->next_block, fd, id, block_capacity, block_num, blocks_checked);
    }

    free(block_info);
    return blocks_checked;
}

int HT_CreateFile(char *fileName, int buckets) {
    if(buckets > BF_BUFFER_SIZE) {
        printf("Cannot have more than %d nuckets\n", BF_BUFFER_SIZE);
        return -1;
    }
    int fd1;
    BF_Block *block;
    BF_Block_Init(&block);

    CALL_OR_DIE(BF_CreateFile(fileName))
    CALL_OR_DIE(BF_OpenFile(fileName, &fd1));

    void *data;
    CALL_OR_DIE(BF_AllocateBlock(fd1, block));
    data = BF_Block_GetData(block);
    int* temp_buckets = data;
    temp_buckets[0] = buckets;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fd1));

    return 0;
}

HT_info *HT_OpenFile(char *fileName) {
    HT_info *info = malloc(sizeof(*info));
    BF_Block_Init(&(info->block));

    CALL_OR_DIE(BF_OpenFile(fileName, &(info->fileDesc)));

    void *data;
    CALL_OR_DIE(BF_GetBlock(info->fileDesc, 0, info->block));
    data = BF_Block_GetData(info->block);
    int* temp_buckets = data;
    info->numBuckets = temp_buckets[0];
    info->hash_table = malloc(sizeof(int) * info->numBuckets);
    for(int i = 0; i < info->numBuckets; i++)
        info->hash_table[i] = 0;
    info->block_capacity = (BF_BLOCK_SIZE - sizeof(HT_block_info)) / sizeof(Record);
    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data, info, sizeof(*info));

    return info;
}

int HT_CloseFile(HT_info *ht_info) {
    // HT_block_info *block_info;
    // BF_Block *block;
    // BF_Block_Init(&block);
    // int blocks_num;
    // CALL_OR_DIE(BF_GetBlockCounter(ht_info->fileDesc, &blocks_num));
    // for(int i = 1; i < blocks_num; i++) {
    //     CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, i, block));
    //     data = BF_Block_GetData(block);
    //     block_info = (HT_block_info*) data + (BF_BLOCK_SIZE - sizeof(HT_block_info));
    //     if(block_info->next_block != NULL)
    //         BF_Block_Destroy(&(block_info->next_block));

    //     CALL_OR_DIE(BF_UnpinBlock(block));
    // }
    // BF_Block_Destroy(&block);

    void* data;
    data = BF_Block_GetData(ht_info->block);
    memcpy(data, ht_info, sizeof(*ht_info));
    BF_Block_SetDirty(ht_info->block);
    CALL_OR_DIE(BF_UnpinBlock(ht_info->block));

    BF_Block_Destroy(&(ht_info->block));
    CALL_OR_DIE(BF_CloseFile(ht_info->fileDesc));

    // free(ht_info->hash_table);
    // free(ht_info);

    return 0;
}

int HT_InsertEntry(HT_info *ht_info, Record record) {
    // printf("so here?  ");
    BF_Block *block;
    BF_Block_Init(&block);
    int block_id;

    int bucket = hash(record.id, ht_info->numBuckets);
    // printf("%d hashes to %d ", record.id, bucket);
    if(ht_info->hash_table[bucket] == 0) {  // Άμα δεν έχει ανατεθεί block σ αυτό το bucket αρχικοποιούμε καινούριο block και το αναθέτουμε 
        CALL_OR_DIE(BF_GetBlockCounter(ht_info->fileDesc, &block_id));
        ht_info->hash_table[bucket] = block_id;
        if((block_id + 1) > ht_info->numBuckets) {
            printf("Max buffer size reached.. Cannot allocate more blocks\n");
            BF_Block_Destroy(&block);
            return -1;
        }
        // printf("bucket has no block attaching new one: %d ", block_id);

        add_record_to_newBlock(block, ht_info->fileDesc, record);
        // printf("block atteched record added\n");
    }
    else {                                  // Άμα υπάρχει ήδη block γι'αυτό το bucket και έχει χώρο προσθέτουμε το record αλλιώς το αποθηκεύουμε στο block υπερχείλισης 
        CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, ht_info->hash_table[bucket], block));
        // printf("bucket with atteched block: %d ", ht_info->hash_table[bucket]);
        block_id = add_record_to_existingBlock(block, ht_info->fileDesc, record, ht_info->block_capacity, ht_info->hash_table[bucket]);
        // printf("record added to block %d\n", block_id);
    }

    BF_Block_Destroy(&block);
    return block_id;
}

int HT_GetAllEntries(HT_info *ht_info, void *value) {
    BF_Block *block;
    BF_Block_Init(&block);
    void* data;
    int blocks_read = 0;
    int *id = value; 
    int bucket = hash(*id, ht_info->numBuckets);

    if(ht_info->hash_table[bucket] == 0)
        printf("No entries with key %d\n", *id);
    else {
        CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, ht_info->hash_table[bucket], block));
        blocks_read = print_records(block, ht_info->fileDesc, *id, ht_info->block_capacity, ht_info->hash_table[bucket], 0);
    }

    BF_Block_Destroy(&block);
    return blocks_read;
}