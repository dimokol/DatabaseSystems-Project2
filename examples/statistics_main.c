#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"
#include "statistics.h"

#define RECORDS_NUM 30
#define FILE_NAME "data.db"
#define INDEX_NAME "index.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int main() {
    srand(12569874);
    BF_Init(LRU);
    // Αρχικοποιήσεις
    HT_CreateFile(FILE_NAME,10);
    SHT_CreateSecondaryIndex(INDEX_NAME,10,FILE_NAME);
    HT_info* info = HT_OpenFile(FILE_NAME);
    SHT_info* index_info = SHT_OpenSecondaryIndex(INDEX_NAME);
    Record record;

    // Κάνουμε εισαγωγή τυχαίων εγγραφών τόσο στο αρχείο κατακερματισμού τις οποίες προσθέτουμε και στο δευτερεύον ευρετήριο
    printf("Insert Entries\n");
    for (int id = 0; id < RECORDS_NUM; ++id) {
        record = randomRecord();
        int block_id = HT_InsertEntry(info, record);
        SHT_SecondaryInsertEntry(index_info, record, block_id);
    }

    // Κλείνουμε το αρχείο κατακερματισμού και το δευτερεύον ευρετήριο για να ανανεοθούν τα μεταδεδομένα τους
    SHT_CloseSecondaryIndex(index_info);
    HT_CloseFile(info);

    // Καλούμε την statistics για το αρχείο κατακερματισμού και το δευτερεύον ευρετήριο
	// SecondaryHashStatistics(INDEX_NAME);
	HashStatistics(FILE_NAME);

	// Απελευθερώνουμε τη μνήμη
	free(index_info->hash_table);
    free(index_info);
	free(info->hash_table);
	free(info);
    BF_Close();
}