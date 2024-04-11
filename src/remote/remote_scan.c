#include "src/remote/clients/pinecone/remote_api.h"
#include "remote.h"

#include <storage/bufmgr.h>
#include "catalog/pg_operator_d.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include <time.h>
#include "common/hashfn.h"

#include <catalog/index.h>
#include <access/heapam.h>
#include <access/tableam.h>

#include <math.h>

RemoteCheckpoint* get_checkpoints_to_fetch(Relation index, int* n_checkpoints_return) {
    // starting at the current remote page, create a list of each checkpoint page's checkpoint (blkno, tid, checkpt_no)
    RemoteBufferMetaPageData buffer_meta = RemoteSnapshotBufferMeta(index);
    int n_checkpoints = buffer_meta.flush_checkpoint.checkpoint_no - buffer_meta.ready_checkpoint.checkpoint_no - 1;
    RemoteCheckpoint* checkpoints;
    BlockNumber currentblkno = buffer_meta.flush_checkpoint.blkno;
    RemoteBufferOpaqueData opaque = RemoteSnapshotBufferOpaque(index, currentblkno);

    // don't fetch more than remote_max_fetched_vectors_for_liveness_check vectors
    if (n_checkpoints > remote_max_fetched_vectors_for_liveness_check) {
        elog(WARNING, "Remote's internal indexing is more than %d batches behind what you have send to remote (flushed). This means remote is not keeping up with the rate of insertion.", n_checkpoints);
        n_checkpoints = remote_max_fetched_vectors_for_liveness_check;
    } else if (n_checkpoints < 0) {
        n_checkpoints = 0;
    }

    checkpoints = palloc((n_checkpoints) * sizeof(RemoteCheckpoint));

    // traverse from the flushed checkpoint back to the live checkpoint and append each checkpoint to the list
    for (int i = 0; i < n_checkpoints; i++) {
        // move to the previous checkpoint (i.e. omit the latest flush checkpoint because we have no way to know if it is live or not)
        currentblkno = opaque.prev_checkpoint_blkno;
        opaque = RemoteSnapshotBufferOpaque(index, currentblkno);
        checkpoints[i] = opaque.checkpoint;
    }

    *n_checkpoints_return = n_checkpoints;
    return checkpoints;
}

RemoteCheckpoint get_best_fetched_checkpoint(Relation index, RemoteCheckpoint* checkpoints, cJSON* fetch_results) {
    // find the latest checkpoint that has was fetched (i.e. is in fetch_results)
    // todo: add timestamping so that we can assume that if the remote page is sufficiently old, we can assume it is live. (simple)

    // preprocess the results from a json object to a list of ItemPointerData
    RemoteCheckpoint invalid_checkpoint = {INVALID_CHECKPOINT_NUMBER, InvalidBlockNumber, {{0, 0},0}, 0, false};
    cJSON* vectors = cJSON_GetObjectItemCaseSensitive(fetch_results, "vectors");
    cJSON* vector;
    int n_fetched = cJSON_GetArraySize(vectors);
    ItemPointerData* fetched_tids = palloc(sizeof(ItemPointerData) * n_fetched);
    int k = 0;

    cJSON_ArrayForEach(vector, vectors) {
        char* id_str = vector->string;
        fetched_tids[k++] = remote_id_get_heap_tid(id_str);
    }
    // log fetched tids
    for (int i = 0; i < n_fetched; i++) {
        elog(DEBUG1, "fetched tid: %s", remote_id_from_heap_tid(fetched_tids[i]));
    }

    // the checkpoints are listed in reverse chronological order, so we can return the first checkpoint that is in fetch_results
    for (int i = 0; checkpoints[i].is_checkpoint; i++) {
        // search for the checkpoint in the fetched tids
        for (int j = 0; j < n_fetched; j++) {
            if (ItemPointerEquals(&checkpoints[i].tid, &fetched_tids[j])) {
                return checkpoints[i];
            }
        }
    }
    return invalid_checkpoint;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc remote_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
    RemoteScanOpaque so;
    AttrNumber attNums[] = {1, 2};
	Oid			sortOperators[] = {Float8LessOperator, TIDLessOperator};
	Oid			sortCollations[] = {InvalidOid, InvalidOid};
	bool		nullsFirstFlags[] = {false, false};
	scan = RelationGetIndexScan(index, nkeys, norderbys);
    so = (RemoteScanOpaque) palloc(sizeof(RemoteScanOpaqueData));

    // set support functions
    so->procinfo = index_getprocinfo(index, 1, 1); // lookup the first support function in the opclass for the first attribute

    // create tuple description for sorting
    so->tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);

    // prep sort
    // allocate 6MB for the heapsort
    so->sortstate = tuplesort_begin_heap(so->tupdesc, 2, attNums, sortOperators, sortCollations, nullsFirstFlags, 6000, NULL, false);
    so->slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
    
    scan->opaque = so;
    return scan;
}


cJSON* remote_build_filter(Relation index, ScanKey keys, int nkeys) {
    cJSON *filter = cJSON_CreateObject();
    cJSON *and_list = cJSON_CreateArray();
    const char* remote_filter_operators[] = {"$lt", "$lte", "$eq", "$gte", "$gt", "$ne", "$in"};
    for (int i = 0; i < nkeys; i++)
    {
        cJSON *key_filter = cJSON_CreateObject();
        cJSON *condition = cJSON_CreateObject();
        cJSON *condition_value = NULL;
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, keys[i].sk_attno - 1);

        if (td->atttypid == TEXTARRAYOID && keys[i].sk_strategy == REMOTE_STRATEGY_ARRAY_CONTAINS) {
            // contains (list_of_strings @> ARRAY[tag1, tag2])
            // $and: [ {list_of_strings: {$in: [tag1]}}, {list_of_strings: {$in: [tag2]}} ]
            cJSON* tags = text_array_get_json(keys[i].sk_argument);
            cJSON* tag;
            cJSON_ArrayForEach(tag, tags) {
                cJSON* condition_contains_tag = cJSON_CreateObject(); // list_of_strings: {$in: [tag1]}
                cJSON* predicate_contains_tag = cJSON_CreateObject(); // {$in: [tag1]}
                cJSON* single_tag_list = cJSON_CreateArray(); // [tag1]
                cJSON_AddItemToArray(single_tag_list, cJSON_Duplicate(tag, true)); // [tag1]
                cJSON_AddItemToObject(predicate_contains_tag, "$in", single_tag_list); // {$in: [tag1]}
                cJSON_AddItemToObject(condition_contains_tag, td->attname.data, predicate_contains_tag); // list_of_strings: {$in: [tag1]}
                cJSON_AddItemToArray(and_list, condition_contains_tag);
            }
            // cJSON_Delete(tags);
        } else {
            switch (td->atttypid)
            {
                case BOOLOID:
                    condition_value = cJSON_CreateBool(DatumGetBool(keys[i].sk_argument));
                    break;
                case FLOAT8OID:
                    condition_value = cJSON_CreateNumber(DatumGetFloat8(keys[i].sk_argument));
                    break;
                case TEXTOID:
                    condition_value = cJSON_CreateString(text_to_cstring(DatumGetTextP(keys[i].sk_argument)));
                    break;
                case TEXTARRAYOID:
                    // overlap
                    if (keys[i].sk_strategy != REMOTE_STRATEGY_ARRAY_OVERLAP) {
                        ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("Unsupported operator for text[] datatype. Must be && (overlap)")));
                    }
                    condition_value = text_array_get_json(keys[i].sk_argument);
                    break;
                    // contains (list_of_strings @> ARRAY[tag1, tag2])
                    // $and: [ {list_of_strings: {$in: [tag1]}}, {list_of_strings: {$in: [tag2]}} ]
                default:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("Unsupported datatype for remote index scan. Must be one of bool, float8, text, text[]")));
                    break;
            }
            // this only works if all datatypes use the same strategy naming convention. todo: document this
            cJSON_AddItemToObject(condition, remote_filter_operators[keys[i].sk_strategy - 1], condition_value);
            cJSON_AddItemToObject(key_filter, td->attname.data, condition);
            cJSON_AddItemToArray(and_list, key_filter);
        }
    }
    cJSON_AddItemToObject(filter, "$and", and_list);
    return filter;
}


/*
 * Start or restart an index scan
 */
void remote_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	Vector * vec;
	// cJSON *remote_response;
    RemoteCheckpoint* fetch_checkpoints;
    RemoteCheckpoint best_checkpoint;
    Datum query_datum;
    RemoteStaticMetaPageData remote_metadata = RemoteSnapshotStaticMeta(scan->indexRelation);
    RemoteScanOpaque so = (RemoteScanOpaque) scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation); // used for accessing
    //
    Relation index = scan->indexRelation;
    RemoteOptions *options = (RemoteOptions *) index->rd_options;
    RemoteIndexInterface* remote_index_interface = remote_index_interfaces[options->provider];
    PreparedQuery query;
    int n_checkpoints;
    ItemPointerData* remote_tids = palloc(sizeof(ItemPointerData) * remote_top_k); // remote top-k ctids

    // check that the ORDER BY is on the first column (which is assumed to be a column on vectors)
    if (scan->numberOfOrderBys == 0 || orderbys[0].sk_attno != 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Index must be ordered by the first column")));
    }
    
	// get the query vector
    query_datum = orderbys[0].sk_argument;
    vec = DatumGetVector(query_datum);
    fetch_checkpoints = get_checkpoints_to_fetch(scan->indexRelation, &n_checkpoints);
    query = remote_index_interface->prepare_query(scan->indexRelation, keys, nkeys, vec);
    remote_tids = remote_index_interface->query_with_fetch(remote_metadata.host, remote_top_k, query, true, fetch_checkpoints, n_checkpoints, &best_checkpoint);

    // set the remote_ready_page to the best checkpoint
    if (best_checkpoint.is_checkpoint) {
        set_buffer_meta_page(scan->indexRelation, &best_checkpoint, NULL, NULL, NULL, NULL);
    }

    // copy metric
    so->metric = remote_metadata.metric;

    /* Requires MVCC-compliant snapshot as not able to pin during sorting */
    /* https://www.postgresql.org/docs/current/index-locking.html */
    if (!IsMVCCSnapshot(scan->xs_snapshot))
        elog(ERROR, "non-MVCC snapshots are not supported with remote");

    // get the ctids from the buffer
    ItemPointerData* buffer_tids = buffer_get_tids(scan->indexRelation);


    // locally scan the buffer and add them to the sort state
    load_buffer_into_sort(scan->indexRelation, so, query_datum, tupdesc);

    // TODO: add the remote results to the sort state
    
    // allocate for xs_orderbyvals (*Datum)
    scan->xs_orderbyvals = palloc(sizeof(Datum)); // assumes only one ORDER BY
    scan->xs_orderbynulls = palloc(sizeof(bool)); // TODO: assumes only one ORDER BY

}

// todo: save stats from inserting from base table into the meta

void load_tids_into_sort(Relation index, RemoteScanOpaque so, Datum query_datum, TupleDesc index_tupdesc, ItemPointerData* tids, int n_tids) {
    // todo: make sure that this is just as fast as pgvector's flatscan e.g. using vectorized operations
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);

    // index info
    IndexInfo *indexInfo = BuildIndexInfo(index);
    Datum* index_values = palloc(sizeof(Datum) * indexInfo->ii_NumIndexAttrs);
    bool* index_isnull = palloc(sizeof(bool) * indexInfo->ii_NumIndexAttrs);

    // get the base table
    Oid baseTableOid = index->rd_index->indrelid;
    Relation baseTableRel = RelationIdGetRelation(baseTableOid);
    Snapshot snapshot = GetActiveSnapshot();

    // begin the index fetch (this the preferred way for an index to request tuples from its base table)
    IndexFetchTableData *fetchData = baseTableRel->rd_tableam->index_fetch_begin(baseTableRel);
    TupleTableSlot *base_table_slot = MakeSingleTupleTableSlot(baseTableRel->rd_att, &TTSOpsBufferHeapTuple);
    bool call_again, all_dead, found;

    for (int i = 0; i < n_tids; i++) {
        // fetch the vector from the base table
        ItemPointerData tid = tids[i];
        found = baseTableRel->rd_tableam->index_fetch_tuple(fetchData, &tid, snapshot, base_table_slot, &call_again, &all_dead);
        if (!found) {
            elog(WARNING, "could not find tuple in base table");
            continue; // do not add the tuple to the sortstate
        }

        // extract the indexed columns
        FormIndexDatum(indexInfo, base_table_slot, NULL, index_values, index_isnull);
        if (index_isnull[0]) elog(ERROR, "vector is null");
        
        // add the tuple
        ExecClearTuple(slot);
        slot->tts_values[0] = FunctionCall2(so->procinfo, index_values[0], query_datum); // compute distance between entry and query
        slot->tts_isnull[0] = false;
        slot->tts_values[1] = TransactionIdGetDatum(&tid);
        slot->tts_isnull[1] = false;
        ExecStoreVirtualTuple(slot);

        tuplesort_puttupleslot(so->sortstate, slot);
    }

    // end the index fetch
    ExecDropSingleTupleTableSlot(base_table_slot);
    baseTableRel->rd_tableam->index_fetch_end(fetchData);
    // close the base table
    RelationClose(baseTableRel);

    tuplesort_performsort(so->sortstate);
    
    // get the first tuple from the sortstate
    so->more_buffer_tuples = tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL);
}

ItemPointerData* buffer_get_tids(Relation index)
{
    RemoteBufferMetaPageData buffer_meta = RemoteSnapshotBufferMeta(index);
    BlockNumber currentblkno = buffer_meta.ready_checkpoint.blkno;
    int n_tuples = buffer_meta.latest_checkpoint.n_preceding_tuples + buffer_meta.n_tuples_since_last_checkpoint;
    int unflushed_tuples = n_tuples - buffer_meta.flush_checkpoint.n_preceding_tuples;
    int unready_tuples = n_tuples - buffer_meta.ready_checkpoint.n_preceding_tuples;
    int n_tids = 0;

    ItemPointerData* buffer_tids = palloc(sizeof(ItemPointerData) * remote_max_buffer_scan);

    // check H - T > max_local_scan
    if (unready_tuples > remote_max_buffer_scan) ereport(NOTICE, (errcode(ERRCODE_INSUFFICIENT_RESOURCES), errmsg("Buffer is too large"), errhint("There are %d tuples in the buffer that have not yet been flushed to remote and %d tuples in remote that are not yet live. You may want to consider flushing the buffer.", unflushed_tuples, unready_tuples - unflushed_tuples)));
    

    // add tuples to buffer_tids
    while (BlockNumberIsValid(currentblkno)) {
        Buffer buf;
        Page page;


        // access the page
        buf = ReadBuffer(index, currentblkno); // todo bulkread access method
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        // add all tuples on the page to the sortstate
        for (OffsetNumber offno = FirstOffsetNumber; offno <= PageGetMaxOffsetNumber(page); offno = OffsetNumberNext(offno)) {
            // get the tid
            RemoteBufferTuple buffer_tup;
            ItemId itemid = PageGetItemId(page, offno);
            Item item = PageGetItem(page, itemid);
            buffer_tup = *((RemoteBufferTuple*) item);
            buffer_tids[n_tids++] = buffer_tup.tid;
        }

        // move to the next page
        currentblkno = RemotePageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);

        // stop if we have added enough tuples to the sortstate
        if (n_tids >= remote_max_buffer_scan) {
            elog(NOTICE, "Reached max local scan");
            break;
        }
    }

    return buffer_tids;
}

/*
 * Fetch the next tuple in the given scan
 */
bool remote_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	// interpret scan->opaque as a cJSON object
	char *id_str;
	ItemPointerData match_heaptid;
    RemoteScanOpaque so = (RemoteScanOpaque) scan->opaque;
    cJSON *match = so->remote_results;
    double remote_best_dist, buffer_best_dist, dist, dist_lower_bound;
    bool isnull;
    float rel_tol = 0.15; // relative tolerance for distance recheck; TODO: this should depend on the metric; the inaccuracy arises from remote using half precision floats

    elog(DEBUG1, "✓ remote_best_dist: %f, buffer_best_dist: %f", remote_best_dist, buffer_best_dist);
    // merge the results from the buffer and the remote index
    if (match == NULL && !so->more_buffer_tuples) {
        return false;
    }
    else if (buffer_best_dist < remote_best_dist) {
        // use the buffer tuple
        Datum blkno_datum = slot_getattr(so->slot, 2, &isnull);
        Datum offset_datum = slot_getattr(so->slot, 3, &isnull);
        dist = buffer_best_dist;
        ItemPointerSetBlockNumber(&match_heaptid, blkno_datum);
        ItemPointerSetOffsetNumber(&match_heaptid, offset_datum);
        scan->xs_heaptid = match_heaptid;
        scan->xs_recheck = true;
        // get the next tuple from the sortstate
        so->more_buffer_tuples = tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL);
    }
    else {
        dist = remote_best_dist;
        // get the id of the match // interpret the id as a string
        id_str = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(match, "id"));
        match_heaptid = remote_id_get_heap_tid(id_str);
        scan->xs_heaptid = match_heaptid;
        // TODO: create a datum out of the distance and retrun it to xs_orderbyvals
        // NEXT
        so->remote_results = so->remote_results->next;
    }
    // The recheck is going to compute vector<->query i.e. l2_distance, whereas for sorting we have been using l2_squared_distance
    // we need to provide xs_recheck a lower bound on the l2_distance
    dist_lower_bound = dist > 0 ? dist * (1 - rel_tol) : dist * (1 + rel_tol);
    dist_lower_bound = sqrt(dist_lower_bound);
    scan->xs_recheckorderby = true; // remote returns an approximate distance which we need to recheck.
    scan->xs_orderbyvals[0] = Float8GetDatum((float8) dist_lower_bound);
    scan->xs_orderbynulls[0] = false;
    elog(DEBUG1, "dist: %f, dist_lower_bound: %f", dist, dist_lower_bound);
    return true;
}

void no_endscan(IndexScanDesc scan) {};
