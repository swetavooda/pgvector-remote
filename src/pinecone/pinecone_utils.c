#include "pinecone.h"

#include "storage/bufmgr.h"
#include "access/generic_xlog.h"
#include "access/relscan.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

cJSON* tuple_get_pinecone_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id)
{
    cJSON *json_vector = cJSON_CreateObject();
    cJSON *metadata = cJSON_CreateObject();
    Vector *vector;
    cJSON *json_values;
    bool isNonZero;

    vector = DatumGetVector(values[0]);
    isNonZero = validate_vector_nonzero(vector);
    if(!isNonZero) return NULL;
    json_values = cJSON_CreateFloatArray(vector->x, vector->dim);
    // prepare metadata
    for (int i = 1; i < tup_desc->natts; i++) // skip the first column which is the vector
    {
        // todo: we should validate that all the columns have the desired types when the index is built
        FormData_pg_attribute* td = TupleDescAttr(tup_desc, i);
        switch (td->atttypid) {
            case BOOLOID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateBool(DatumGetBool(values[i])));
                break;
            case FLOAT8OID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateNumber(DatumGetFloat8(values[i])));
                break;
            case INT4OID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateNumber(DatumGetInt32(values[i])));
                break;
            case TEXTOID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateString(text_to_cstring(DatumGetTextP(values[i]))));
                break;
            case TEXTARRAYOID:
                {
                    cJSON* json_array = text_array_get_json(values[i]);
                    cJSON_AddItemToObject(metadata, NameStr(td->attname), json_array);
                }
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Invalid column type when decoding tuple."),
                                errhint("Pinecone index only supports boolean, float8, text, and textarray columns")));
        }
    }
    // add to vector object
    cJSON_AddItemToObject(json_vector, "id", cJSON_CreateString(vector_id));
    cJSON_AddItemToObject(json_vector, "values", json_values);
    cJSON_AddItemToObject(json_vector, "metadata", metadata);
    return json_vector;
}


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



PineconeStaticMetaPageData PineconeSnapshotStaticMeta(Relation index)
{
    Buffer buf;
    Page page;
    PineconeStaticMetaPage metap;
    buf = ReadBuffer(index, PINECONE_STATIC_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetStaticMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

PineconeBufferMetaPageData PineconeSnapshotBufferMeta(Relation index)
{
    Buffer buf;
    Page page;
    PineconeBufferMetaPage metap;
    buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetBufferMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

PineconeBufferOpaqueData PineconeSnapshotBufferOpaque(Relation index, BlockNumber blkno)
{
    Buffer buf;
    Page page;
    PineconeBufferOpaque opaque;
    buf = ReadBuffer(index, blkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    opaque = PineconePageGetOpaque(page);
    UnlockReleaseBuffer(buf);
    return *opaque;
}

/*
 * Acquire the buffer's meta page and update its fields.
 */
void set_buffer_meta_page(Relation index, PineconeCheckpoint* ready_checkpoint, PineconeCheckpoint* flush_checkpoint, PineconeCheckpoint* latest_checkpoint, BlockNumber* insert_page, int* n_tuples_since_last_checkpoint) {
    Buffer buffer_meta_buf;
    Page buffer_meta_page;
    PineconeBufferMetaPage buffer_meta;

    // start WAL logging
    GenericXLogState* state = GenericXLogStart(index);

    // get the meta page
    buffer_meta_buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO);
    LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0); 
    buffer_meta = PineconePageGetBufferMeta(buffer_meta_page);

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

char* checkpoint_to_string(PineconeCheckpoint checkpoint) {
    char* str = palloc(200);
    if (checkpoint.is_checkpoint) {
        snprintf(str, 200, "#%d, blk %d, tid %s, n_prec %d", checkpoint.checkpoint_no, checkpoint.blkno, pinecone_id_from_heap_tid(checkpoint.tid), checkpoint.n_preceding_tuples);
    } else {
        snprintf(str, 200, "invalid");
    }
    return str;
}

char* buffer_meta_to_string(PineconeBufferMetaPageData buffer_meta) {
    char* str = palloc(200);
    // show reach of ready, flush and latest checkpoints on a separate line
    // show insert page and n_tuples_since_last_checkpoint
    snprintf(str, 200, "ready: %s\nflush: %s\nlatest: %s\ninsert page: %d\nn_since_check: %d", 
        checkpoint_to_string(buffer_meta.ready_checkpoint), checkpoint_to_string(buffer_meta.flush_checkpoint), checkpoint_to_string(buffer_meta.latest_checkpoint), buffer_meta.insert_page, buffer_meta.n_tuples_since_last_checkpoint);
    return str;
}

char* buffer_opaque_to_string(PineconeBufferOpaqueData buffer_opaque) {
    char* str = palloc(200);
    snprintf(str, 200, "next: %d, prev_check: %d, check: %s", buffer_opaque.nextblkno, buffer_opaque.prev_checkpoint_blkno, checkpoint_to_string(buffer_opaque.checkpoint));
    return str;
}   

void pinecone_print_relation(Relation index) {
    // print each page of the relation for debugging

    // print the static meta page and the buffer meta page
    PineconeStaticMetaPageData static_meta = PineconeSnapshotStaticMeta(index);
    PineconeBufferMetaPageData buffer_meta = PineconeSnapshotBufferMeta(index);
    elog(INFO, "\n\nStatic Meta Page:\n%d dimensions, %s metric, %s host, %s index name",
         static_meta.dimensions, vector_metric_to_pinecone_metric[static_meta.metric], static_meta.host, static_meta.pinecone_index_name);
    elog(INFO, "\n\nBuffer Meta Page:\n%s", buffer_meta_to_string(buffer_meta));

    // print the buffer opaque data for each page
    for (BlockNumber blkno = PINECONE_BUFFER_HEAD_BLKNO; blkno < RelationGetNumberOfBlocks(index); blkno++) {
        PineconeBufferOpaqueData buffer_opaque = PineconeSnapshotBufferOpaque(index, blkno);
        elog(INFO, "\nBuffer Opaque Page %d: %s", blkno, buffer_opaque_to_string(buffer_opaque));
    }
}


// murmur hash lifted from hnswutils.c
uint64
murmurhash64(uint64 data)
{
	uint64		h = data;

	h ^= h >> 33;
	h *= 0xff51afd7ed558ccd;
	h ^= h >> 33;
	h *= 0xc4ceb9fe1a85ec53;
	h ^= h >> 33;

	return h;
}

/* TID hash table */
uint32
hash_tid(ItemPointerData tid, int seed)
{
	union
	{
		uint64		i;
		ItemPointerData tid;
	}			x;

	/* Initialize unused bytes */
	x.i = 0;
	x.tid = tid;

	return murmurhash64(x.i + seed);
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