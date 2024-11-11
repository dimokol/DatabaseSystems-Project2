#ifndef STATISTICS_H
#define STATISTICS_H

typedef struct {
    int block_count;    /* πόσα blocks έχει ένα αρχείο */
    int min_entries;    /* ελάχιστο */
    double avg_entries; /* μέσο */
    int max_entries;    /* μέγιστο πλήθος εγγραφών που έχει κάθε bucket ενός αρχείου */
    double avg_blocks_perBucket;        /* μέσος αριθμός των blocks που έχει κάθε bucket */
    int overflow_block_containingBuckets;   /* πλήθος των buckets που έχουν μπλοκ υπερχείλισης */
    int *overflow_blockNum_perBucket;   /* πόσα μπλοκ είναι αυτά για κάθε bucket */
} Statistics_info;

/* Η συνάρτηση διαβάζει το αρχείο ht με όνομα filename και τυπώνει τα στατιστικά που αναφέρθηκαν
προηγουμένως. Σε περίπτωση επιτυχίας επιστρέφει 0, ενώ σε περίπτωση λάθους επιστρέφει -1. */
int HashStatistics(
    char* filename /* όνομα του αρχείου που ενδιαφέρει */ );

/* Η συνάρτηση διαβάζει το αρχείο sht με όνομα filename και τυπώνει τα στατιστικά που αναφέρθηκαν
προηγουμένως. Σε περίπτωση επιτυχίας επιστρέφει 0, ενώ σε περίπτωση λάθους επιστρέφει -1. */
int SecondaryHashStatistics(
    char* filename /* όνομα του αρχείου που ενδιαφέρει */ );

#endif // STATISTICS_FILE_H