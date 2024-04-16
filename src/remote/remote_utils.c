#include "remote.h"

#include "storage/bufmgr.h"
#include "access/generic_xlog.h"
#include "access/relscan.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

RemoteStaticMetaPageData RemoteSnapshotStaticMeta(Relation index)
{
    Buffer buf;
    Page page;
    RemoteStaticMetaPage metap;
    buf = ReadBuffer(index, REMOTE_STATIC_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = RemotePageGetStaticMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

RemoteBufferMetaPageData RemoteSnapshotBufferMeta(Relation index)
{
    Buffer buf;
    Page page;
    RemoteBufferMetaPage metap;
    buf = ReadBuffer(index, REMOTE_BUFFER_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = RemotePageGetBufferMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

RemoteBufferOpaqueData RemoteSnapshotBufferOpaque(Relation index, BlockNumber blkno)
{
    Buffer buf;
    Page page;
    RemoteBufferOpaque opaque;
    buf = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    opaque = RemotePageGetOpaque(page);
    UnlockReleaseBuffer(buf);
    return *opaque;
}

/*
 * Acquire the buffer's meta page and update its fields.
 */
void set_buffer_meta_page(Relation index, RemoteCheckpoint* ready_checkpoint, RemoteCheckpoint* flush_checkpoint, RemoteCheckpoint* latest_checkpoint, BlockNumber* insert_page, int* n_tuples_since_last_checkpoint) {
    Buffer buffer_meta_buf;
    Page buffer_meta_page;
    RemoteBufferMetaPage buffer_meta;

    // start WAL logging
    GenericXLogState* state = GenericXLogStart(index);

    // get the meta page
    buffer_meta_buf = ReadBuffer(index, REMOTE_BUFFER_METAPAGE_BLKNO);
    LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0); 
    buffer_meta = RemotePageGetBufferMeta(buffer_meta_page);

    // update the buffer meta page
    // checkpoints
    if (ready_checkpoint != NULL) {
        buffer_meta->ready_checkpoint = *ready_checkpoint;
    }
    if (flush_checkpoint != NULL) {
        buffer_meta->flush_checkpoint = *flush_checkpoint;
    }
    if (latest_checkpoint != NULL) {
        buffer_meta->latest_checkpoint = *latest_checkpoint;
    }
    // insert page
    if (insert_page != NULL) {
        buffer_meta->insert_page = *insert_page;
    }
    // n_tuples_since_last_checkpoint
    if (n_tuples_since_last_checkpoint != NULL) {
        buffer_meta->n_tuples_since_last_checkpoint = *n_tuples_since_last_checkpoint;
    }


    // save and release
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buffer_meta_buf);
}

char* checkpoint_to_string(RemoteCheckpoint checkpoint) {
    char* str = palloc(200);
    if (checkpoint.is_checkpoint) {
        snprintf(str, 200, "#%d, blk %d, tid %d:%d, n_prec %d", checkpoint.checkpoint_no, checkpoint.blkno, ItemPointerGetBlockNumber(&checkpoint.tid), ItemPointerGetOffsetNumber(&checkpoint.tid), checkpoint.n_preceding_tuples);
    } else {
        snprintf(str, 200, "invalid");
    }
    return str;
}

char* buffer_meta_to_string(RemoteBufferMetaPageData buffer_meta) {
    char* str = palloc(200);
    // show reach of ready, flush and latest checkpoints on a separate line
    // show insert page and n_tuples_since_last_checkpoint
    snprintf(str, 200, "ready: %s\nflush: %s\nlatest: %s\ninsert page: %d\nn_since_check: %d", 
        checkpoint_to_string(buffer_meta.ready_checkpoint), checkpoint_to_string(buffer_meta.flush_checkpoint), checkpoint_to_string(buffer_meta.latest_checkpoint), buffer_meta.insert_page, buffer_meta.n_tuples_since_last_checkpoint);
    return str;
}

char* buffer_opaque_to_string(RemoteBufferOpaqueData buffer_opaque) {
    char* str = palloc(200);
    snprintf(str, 200, "next: %d, prev_check: %d, check: %s", buffer_opaque.nextblkno, buffer_opaque.prev_checkpoint_blkno, checkpoint_to_string(buffer_opaque.checkpoint));
    return str;
}   

void remote_print_relation(Relation index) {
    // print each page of the relation for debugging

    // print the static meta page and the buffer meta page
    RemoteStaticMetaPageData static_meta = RemoteSnapshotStaticMeta(index);
    RemoteBufferMetaPageData buffer_meta = RemoteSnapshotBufferMeta(index);
    elog(INFO, "\n\nStatic Meta Page:\n%d dimensions, %d metric, %s host",
         static_meta.dimensions, static_meta.metric, static_meta.host);
    elog(INFO, "\n\nBuffer Meta Page:\n%s", buffer_meta_to_string(buffer_meta));

    // print the buffer opaque data for each page
    for (BlockNumber blkno = REMOTE_BUFFER_HEAD_BLKNO; blkno < RelationGetNumberOfBlocks(index); blkno++) {
        RemoteBufferOpaqueData buffer_opaque = RemoteSnapshotBufferOpaque(index, blkno);
        elog(INFO, "\nBuffer Opaque Page %d: %s", blkno, buffer_opaque_to_string(buffer_opaque));
    }
}


/* text_array_get_json */
cJSON* text_array_get_json(Datum value) {
    ArrayType *array = DatumGetArrayTypeP(value);
    int nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    Datum* elems;
    bool* nulls;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    Oid elmtype = ARR_ELEMTYPE(array);
    cJSON *json_array = cJSON_CreateArray();

    // get array element type info
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

    // deconstruct array
    deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);

    // copy array elements to json array
    for (int j = 0; j < nelems; j++) {
        if (!nulls[j]) {
            Datum elem = elems[j];
            char* cstr = TextDatumGetCString(elem);
            cJSON_AddItemToArray(json_array, cJSON_CreateString(cstr));
        }
    }
    return json_array;
}