#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"
#include "statistics.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

void print_statistics(Statistics_info info, char* filename, int numBuckets) {
    printf("\nStatistics of %s:\n", filename);
    printf("block count: %d\n", info.block_count);
    printf("min entries %d\n", info.min_entries);
    printf("average entries %f\n", info.avg_entries);
    printf("max entries %d\n", info.max_entries);
    printf("average blocks per bucket %f\n", info.avg_blocks_perBucket);
    printf("buckets containing overflow blocks %d\n", info.overflow_block_containingBuckets);
    for(int i = 0; i < numBuckets; i++) {
        if(info.overflow_blockNum_perBucket[i] > 0)
            printf("bucket %d has %d overflow block(s)\n", i + 1, info.overflow_blockNum_perBucket[i]);
    }
}

bool overflowBlock_has_rec(int fd, BF_Block *block, HT_block_info *block_info, int hash_result_bucket, int numBuckets) {
    bool ret = false;

    void* data;
    data = BF_Block_GetData(block);
    Record* rec = data;
    for(int i = 0; i < block_info->record_count; i++) {
        if(hash(rec[i].id, numBuckets) == hash_result_bucket)
            ret = true;
    }

    return ret;
}

int how_many_overflowBlocks(int fd, int block_num, int hash_result_bucket, int numBuckets) {
    BF_Block *block;
    BF_Block_Init(&block);
    void* data;
    int count = 0;
    for(int i = block_num; i < numBuckets; i++) {
        CALL_OR_DIE(BF_GetBlock(fd, i, block));
        data = BF_Block_GetData(block);
        HT_block_info *block_info = data + (BF_BLOCK_SIZE - sizeof(*block_info));
        if(block_info->next_block == NULL) {
            CALL_OR_DIE(BF_UnpinBlock(block));
            break;
        }

        if(overflowBlock_has_rec(fd, block, block_info, hash_result_bucket, numBuckets))
            count++;
        else
            break;


        CALL_OR_DIE(BF_UnpinBlock(block));
    }

    BF_Block_Destroy(&block);
    return count;
}

int HashStatistics(char* filename) {
    Statistics_info *stat_info = malloc(sizeof(*stat_info));
    HT_info *info;
    HT_block_info *block_info;
    int fd, i;
    BF_Block *block;
    BF_Block_Init(&block);

    CALL_OR_DIE(BF_OpenFile(filename, &(fd)));

    void *data;
    CALL_OR_DIE(BF_GetBlock(fd, 0, block));
    data = BF_Block_GetData(block);
    info = data;
    
    CALL_OR_DIE(BF_GetBlockCounter(fd, &(stat_info->block_count)));
    
    stat_info->min_entries = info->block_capacity;
    stat_info->avg_entries = 0.0;
    stat_info->max_entries = 0;
    for(i = 1; i < stat_info->block_count; i++) {
        CALL_OR_DIE(BF_GetBlock(fd, i, block));
        data = BF_Block_GetData(block);
        block_info = data + (BF_BLOCK_SIZE - sizeof(*block_info));   
        // printf("%d\n", block_info->record_count);

        if(stat_info->min_entries > block_info->record_count)
            stat_info->min_entries = block_info->record_count;
        stat_info->avg_entries += block_info->record_count;
        if(stat_info->max_entries < block_info->record_count)
            stat_info->max_entries = block_info->record_count;
        
        CALL_OR_DIE(BF_UnpinBlock(block));
    }
    stat_info->avg_entries = stat_info->avg_entries / stat_info->block_count;

    stat_info->avg_blocks_perBucket = 0.0;
    stat_info->overflow_block_containingBuckets = 0;
    stat_info->overflow_blockNum_perBucket = malloc(sizeof(int) * info->numBuckets);
    for(i = 0; i < info->numBuckets; i++) {
        stat_info->overflow_blockNum_perBucket[i] = 0;
        if(info->hash_table[i] == 0)
            continue;

        // printf("%d\n", info->hash_table[i]);
        CALL_OR_DIE(BF_GetBlock(fd, info->hash_table[i], block));
        data = BF_Block_GetData(block);
        block_info = data + (BF_BLOCK_SIZE - sizeof(*block_info));
        if(block_info->next_block != NULL) {
            stat_info->overflow_block_containingBuckets++;
            stat_info->overflow_blockNum_perBucket[i] = how_many_overflowBlocks(fd, info->hash_table[i] + 1, i, info->numBuckets);
            stat_info->avg_blocks_perBucket += stat_info->overflow_blockNum_perBucket[i];
        }

        CALL_OR_DIE(BF_UnpinBlock(block));
    }
    stat_info->avg_blocks_perBucket = (stat_info->avg_blocks_perBucket / stat_info->block_count) + 1.0;

    print_statistics(*stat_info, filename, info->numBuckets);

    CALL_OR_DIE(BF_UnpinBlock(info->block));
    BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fd));
    free(stat_info->overflow_blockNum_perBucket);
    free(stat_info);
    return 0;
}

bool s_overflowBlock_has_rec(int fd, BF_Block *block, SHT_block_info *block_info, int hash_result_bucket, int numBuckets) {
    bool ret = false;

    void* data;
    data = BF_Block_GetData(block);
    Entry* entr = data;
    for(int i = 0; i < block_info->entry_count; i++) {
        if(s_hash(entr[i].name, numBuckets) == hash_result_bucket)
            ret = true;
    }

    return ret;
}

int s_how_many_overflowBlocks(int fd, int block_num, int hash_result_bucket, int numBuckets) {
    BF_Block *block;
    BF_Block_Init(&block);
    void* data;
    int count = 0;
    for(int i = block_num; i < numBuckets; i++) {
        CALL_OR_DIE(BF_GetBlock(fd, i, block));
        data = BF_Block_GetData(block);
        SHT_block_info *block_info = data + (BF_BLOCK_SIZE - sizeof(*block_info));
        if(block_info->next_block == NULL) {
            CALL_OR_DIE(BF_UnpinBlock(block));
            break;
        }

        if(s_overflowBlock_has_rec(fd, block, block_info, hash_result_bucket, numBuckets))
            count++;
        else
            break;


        CALL_OR_DIE(BF_UnpinBlock(block));
    }

    BF_Block_Destroy(&block);
    return count;
}

int SecondaryHashStatistics(char* filename) {
    Statistics_info *stat_info = malloc(sizeof(*stat_info));
    SHT_info *info;
    SHT_block_info *block_info;
    int fd, i;
    BF_Block *block;
    BF_Block_Init(&block);

    CALL_OR_DIE(BF_OpenFile(filename, &(fd)));

    void *data;
    CALL_OR_DIE(BF_GetBlock(fd, 0, block));
    data = BF_Block_GetData(block);
    info = data;
    
    CALL_OR_DIE(BF_GetBlockCounter(fd, &(stat_info->block_count)));
    
    stat_info->min_entries = info->block_capacity;
    stat_info->avg_entries = 0.0;
    stat_info->max_entries = 0;
    for(i = 1; i < stat_info->block_count; i++) {
        CALL_OR_DIE(BF_GetBlock(fd, i, block));
        data = BF_Block_GetData(block);
        block_info = data + (BF_BLOCK_SIZE - sizeof(*block_info));   
        // printf("%d\n", block_info->entry_count);

        if(stat_info->min_entries > block_info->entry_count)
            stat_info->min_entries = block_info->entry_count;
        stat_info->avg_entries += block_info->entry_count;
        if(stat_info->max_entries < block_info->entry_count)
            stat_info->max_entries = block_info->entry_count;
        
        CALL_OR_DIE(BF_UnpinBlock(block));
    }
    stat_info->avg_entries = stat_info->avg_entries / stat_info->block_count;

    stat_info->avg_blocks_perBucket = 0.0;
    stat_info->overflow_block_containingBuckets = 0;
    stat_info->overflow_blockNum_perBucket = malloc(sizeof(int) * info->numBuckets);
    for(i = 0; i < info->numBuckets; i++) {
        stat_info->overflow_blockNum_perBucket[i] = 0;
        if(info->hash_table[i] == 0)
            continue;

        // printf("%d\n", info->hash_table[i]);
        CALL_OR_DIE(BF_GetBlock(fd, info->hash_table[i], block));
        data = BF_Block_GetData(block);
        block_info = data + (BF_BLOCK_SIZE - sizeof(*block_info));
        if(block_info->next_block != NULL) {
            stat_info->overflow_block_containingBuckets++;
            stat_info->overflow_blockNum_perBucket[i] = s_how_many_overflowBlocks(fd, info->hash_table[i] + 1, i, info->numBuckets);
            stat_info->avg_blocks_perBucket += stat_info->overflow_blockNum_perBucket[i];
        }

        CALL_OR_DIE(BF_UnpinBlock(block));
    }
    stat_info->avg_blocks_perBucket = (stat_info->avg_blocks_perBucket / stat_info->block_count) + 1.0;

    print_statistics(*stat_info, filename, info->numBuckets);

    CALL_OR_DIE(BF_UnpinBlock(info->block));
    BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fd));
    free(stat_info->overflow_blockNum_perBucket);
    free(stat_info);
    return 0;
}