#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call) {     \
    BF_ErrorCode code = call;   \
    if (code != BF_OK) {        \
        BF_PrintError(code);    \
        exit(code);             \
    }                           \
}                               \

int s_hash(char *key, int table_size) {
    unsigned long int value = 0;
    int key_len = strlen(key);

    for (int i = 0; i < key_len; i++)
        value = value * 37 + key[i];

    return value % table_size;
}

// Αρχικοποιεί ένα block και του προσθέτει ένα entry
int add_entry_to_newBlock(BF_Block *block, int fd, Entry entry) {
    SHT_block_info *block_info = malloc(sizeof(*block_info));
    block_info->next_block = NULL;
    block_info->entry_count = 1;
    
    void* data;
    CALL_OR_DIE(BF_AllocateBlock(fd, block));
    data = BF_Block_GetData(block);
    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
    Entry* entries = data;
    entries[0] = entry;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    free(block_info);
    return 0;
}

// Προσθέτει ένα entry στο block εφόσον χωράει αλλίως το προσθέτει σε block υπερχείλισης (εάν δεν υπάρχει ορίζει ένα)
int add_entry_to_existingBlock(BF_Block *block, int fd, Entry entry, int block_capacity, int block_num) {
    SHT_block_info *block_info = malloc(sizeof(*block_info));
    
    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));

    if(block_info->entry_count + 1 <= block_capacity) {
        Entry* entries = data;
        entries[block_info->entry_count] = entry;
        block_info->entry_count++;
        memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));

        free(block_info);
        return block_num;
    }

    // Άμα δεν έχει χώρο αυτό το block προσθέτουμε το entry στο block υπερχείλισης
    if(block_info->next_block != NULL) {
        block_num++;
        block_num = add_entry_to_existingBlock(block_info->next_block, fd, entry, block_capacity, block_num);
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

        block_num = add_entry_to_existingBlock(block_info->next_block, fd, entry, block_capacity, block_num);
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

    add_entry_to_newBlock(block_info->next_block, fd, entry);
    memcpy(data + (BF_BLOCK_SIZE - sizeof(*block_info)), block_info, sizeof(*block_info));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    free(block_info);
    return block_num + 1;
}

// Εμφανίζει τo record με το συγκεκριμένο entry.name που βρίσκεται στο entry.block_id block του αρχείου πρωτεύον κατακερματισμού 
int print_record_with_name(HT_info* ht_info, Entry entry) {
	HT_block_info *block_info = malloc(sizeof(*block_info));
	BF_Block *block;
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(ht_info->fileDesc, entry.block_id, block));
    
    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));
	Record* rec = data;
    for(int i = 0; i < block_info->record_count; i++) {
        if(!strcmp(rec[i].name, entry.name))
            printRecord(rec[i]);
    }
    CALL_OR_DIE(BF_UnpinBlock(block));

    BF_Block_Destroy(&block);
	free(block_info);
	return 0;
}

// Εμφανίζει τα στοιχεία των records με το συγκεκριμένο name 
int print_entries(HT_info* ht_info, BF_Block *block, int fd, char* name, int block_capacity, int block_num, int blocks_checked) {
    SHT_block_info *block_info = malloc(sizeof(*block_info));
    
    void* data;
    data = BF_Block_GetData(block);
    memcpy(block_info, data + (BF_BLOCK_SIZE - sizeof(*block_info)), sizeof(*block_info));
    Entry* entries = data;
    for(int i = 0; i < block_info->entry_count; i++) {
        if(strcmp(entries[i].name, name) == 0)
            print_record_with_name(ht_info, entries[i]);
    }
    CALL_OR_DIE(BF_UnpinBlock(block));
    blocks_checked++;

    // Ελένχουμε και το block υπερχείλισης για entries με αυτό το name
    if(block_info->next_block != NULL) {
        block_num++;
        block_num = print_entries(ht_info, block_info->next_block, fd, name, block_capacity, block_num, blocks_checked);
    }

    free(block_info);
    return blocks_checked;
}

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName) {
	if(buckets > BF_BUFFER_SIZE) {
        printf("Cannot have more than %d nuckets\n", BF_BUFFER_SIZE);
        return -1;
    }
    int fd1;
    BF_Block *block;
    BF_Block_Init(&block);
    Entry *temp = malloc(sizeof(*temp));    // χρησιμοποιoύμε μια προσωρινή δομή Entry για να αποθηκεύσουμε
    temp->block_id = buckets;               // τον αριθμό κάδων κατακερματισμού και το όνομα αρχείου πρωτεύοντος ευρετηρίου
    if(strlen(fileName) + 1 > 15) {         // προσωρινά στο πρώτο block για να μπορέσουμε να ενημερώσουμε στην open την sht_info
        printf("fileName can't have more than 14 characters\n");
        BF_Block_Destroy(&block);
        free(temp);
        return -1;
    }
    strcpy(temp->name, fileName);

    CALL_OR_DIE(BF_CreateFile(sfileName))
    CALL_OR_DIE(BF_OpenFile(sfileName, &fd1));

    void *data;
    CALL_OR_DIE(BF_AllocateBlock(fd1, block));
    data = BF_Block_GetData(block);
    memset(data, 0, BF_BLOCK_SIZE);
    memcpy(data, temp, sizeof(*temp));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    BF_Block_Destroy(&block);
    CALL_OR_DIE(BF_CloseFile(fd1));

    free(temp);
    return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName) {
    SHT_info *info = malloc(sizeof(*info));
    BF_Block_Init(&(info->block));

    CALL_OR_DIE(BF_OpenFile(indexName, &(info->fileDesc)));

    void *data;
    CALL_OR_DIE(BF_GetBlock(info->fileDesc, 0, info->block));
    data = BF_Block_GetData(info->block);
    Entry *temp = malloc(sizeof(*temp));
    memcpy(temp, data, sizeof(*temp));
    info->numBuckets = temp->block_id;
    strcpy(info->ht_filename, temp->name);
    info->hash_table = malloc(sizeof(int) * info->numBuckets);
    for(int i = 0; i < info->numBuckets; i++)
        info->hash_table[i] = 0;
    info->block_capacity = (BF_BLOCK_SIZE - sizeof(SHT_block_info)) / sizeof(Entry);
    memcpy(data, info, sizeof(*info));

    free(temp);
    return info;
}

int SHT_CloseSecondaryIndex(SHT_info* sht_info) {
    void* data;
    data = BF_Block_GetData(sht_info->block);
    memcpy(data, sht_info, sizeof(*sht_info));
    BF_Block_SetDirty(sht_info->block);
    CALL_OR_DIE(BF_UnpinBlock(sht_info->block));

    BF_Block_Destroy(&(sht_info->block));
    CALL_OR_DIE(BF_CloseFile(sht_info->fileDesc));

    // free(sht_info->hash_table);
    // free(sht_info);

    return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id) {
	Entry *entry = malloc(sizeof(*entry));
	BF_Block *block;
    BF_Block_Init(&block);
    int block_num;

	entry->block_id = block_id;
	strcpy(entry->name, record.name);
    int bucket = s_hash(record.name, sht_info->numBuckets);
    // printf("%s hashes to %d ", record.name, bucket);
    if(sht_info->hash_table[bucket] == 0) {  // Άμα δεν έχει ανατεθεί block σ αυτό το bucket αρχικοποιούμε καινούριο block και το αναθέτουμε 
        CALL_OR_DIE(BF_GetBlockCounter(sht_info->fileDesc, &block_num));
        sht_info->hash_table[bucket] = block_num;
        if((block_id + 1) > sht_info->numBuckets) {
            printf("Max buffer size reached.. Cannot allocate more blocks\n");
            BF_Block_Destroy(&block);
            return -1;
        }
        // printf("bucket has no block attaching new one: %d ", block_num);

        add_entry_to_newBlock(block, sht_info->fileDesc, *entry);
        // printf("block atteched entry added\n");
    }
    else {                                  // Άμα υπάρχει ήδη block γι'αυτό το bucket και έχει χώρο προσθέτουμε το entry αλλιώς το αποθηκεύουμε στο block υπερχείλισης 
        CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc, sht_info->hash_table[bucket], block));
        // printf("bucket with atteched block: %d ", sht_info->hash_table[bucket]);
        block_id = add_entry_to_existingBlock(block, sht_info->fileDesc, *entry, sht_info->block_capacity, sht_info->hash_table[bucket]);
        // printf("entry added to block %d\n", block_id);
    }

    BF_Block_Destroy(&block);
	free(entry);
    return 0;
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name) {
	BF_Block *block;
    BF_Block_Init(&block);
    void* data;
    int blocks_read = 0;
    int bucket = s_hash(name, sht_info->numBuckets);

    if(sht_info->hash_table[bucket] == 0)
        printf("No entries with name %s\n", name);
    else {
        CALL_OR_DIE(BF_GetBlock(sht_info->fileDesc, sht_info->hash_table[bucket], block));
        blocks_read = print_entries(ht_info, block, sht_info->fileDesc, name, sht_info->block_capacity, sht_info->hash_table[bucket], 0);
    }

    BF_Block_Destroy(&block);
    return blocks_read;
}