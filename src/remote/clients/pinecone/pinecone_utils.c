#include "postgres.h"

ItemPointerData pinecone_id_get_heap_tid(char *id)
{
    ItemPointerData heap_tid;
    int n_matched;
    n_matched = sscanf(id, "%04hx%04hx%04hx", &heap_tid.ip_blkid.bi_hi, &heap_tid.ip_blkid.bi_lo, &heap_tid.ip_posid);
    if (n_matched != 3) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector id"),
                        errhint("Vector id should be a 12-character hexadecimal string")));
    }
    return heap_tid;
}

char* pinecone_id_from_heap_tid(ItemPointerData heap_tid)
{
    char* id = palloc(12 + 1);
    snprintf(id, 12 + 1, "%04hx%04hx%04hx", heap_tid.ip_blkid.bi_hi, heap_tid.ip_blkid.bi_lo, heap_tid.ip_posid);
    return id;
}
