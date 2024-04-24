#include <math.h>
#include <float.h>
#include <time.h>

#include "src/remote/remote.h"

#include <storage/bufmgr.h>
#include "catalog/pg_operator_d.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "common/hashfn.h"
#include <catalog/index.h>
#include <access/heapam.h>
#include <access/tableam.h>


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

/*
 * Prepare for an index scan
 */
IndexScanDesc remote_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
    RemoteScanOpaque so;
    AttrNumber attNums[] = {1};
	Oid			sortOperators[] = {TIDLessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};
	scan = RelationGetIndexScan(index, nkeys, norderbys);
    so = (RemoteScanOpaque) palloc(sizeof(RemoteScanOpaqueData));

    // create tuple description for sorting
    so->sort_tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(so->sort_tupdesc, (AttrNumber) 1, "heaptid", TIDOID, -1, 0);
    TupleDescInitEntry(so->sort_tupdesc, (AttrNumber) 2, "is_local", BOOLOID, -1, 0);

    // allocate 600KB for the heapsort max 20K tuples each of 20 bytes is 400KB (each tuple has two datums = 8B + header)
    so->tid_sortstate = tuplesort_begin_heap(so->sort_tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, 600, NULL, false);
    // these slots hold the top result from the sortstate
    so->tid_sortslot = MakeSingleTupleTableSlot(so->sort_tupdesc, &TTSOpsMinimalTuple);
    scan->opaque = so;
    return scan;
}

/*
 * Start or restart an index scan
 */
void remote_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	Vector * vec;
    RemoteCheckpoint* fetch_checkpoints;
    RemoteCheckpoint best_checkpoint;
    Datum query_datum;
    RemoteStaticMetaPageData remote_metadata = RemoteSnapshotStaticMeta(scan->indexRelation);
    RemoteScanOpaque so = (RemoteScanOpaque) scan->opaque;
    //
    Relation index = scan->indexRelation;
    RemoteOptions *options = (RemoteOptions *) index->rd_options;
    RemoteIndexInterface* remote_index_interface = remote_index_interfaces[options->provider];
    PreparedQuery query;
    int n_checkpoints;
    int n_local_tids, n_remote_tids;

    // check that the ORDER BY is on the first column (which is assumed to be a column on vectors)
    if (scan->numberOfOrderBys == 0 || orderbys[0].sk_attno != 1) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Index must be ordered by the first column")));
    }

	// get the query vector
    query_datum = orderbys[0].sk_argument;
    vec = DatumGetVector(query_datum);

    
    // query the vector index and update the liveness pointer
    fetch_checkpoints = get_checkpoints_to_fetch(scan->indexRelation, &n_checkpoints);
    query = remote_index_interface->prepare_query(scan->indexRelation, keys, nkeys, vec, remote_top_k);
    best_checkpoint.is_checkpoint = false;
    so->remote_tids = remote_index_interface->query_with_fetch(remote_metadata.host, remote_top_k, query, true, fetch_checkpoints, n_checkpoints, &best_checkpoint, &n_remote_tids);

    // set the remote_ready_page to the best checkpoint
    if (best_checkpoint.is_checkpoint) set_buffer_meta_page(scan->indexRelation, &best_checkpoint, NULL, NULL, NULL, NULL);

    /* Requires MVCC-compliant snapshot as not able to pin during sorting */
    /* https://www.postgresql.org/docs/current/index-locking.html */
    if (!IsMVCCSnapshot(scan->xs_snapshot))
        elog(ERROR, "non-MVCC snapshots are not supported with remote");


    // get the local tids
    so->local_tids = buffer_get_tids(scan->indexRelation, &n_local_tids);

    // sort the tids
    load_tids_into_sort(index, so->tid_sortstate, so, query_datum, so->local_tids, n_local_tids, true);
    load_tids_into_sort(index, so->tid_sortstate, so, query_datum, so->remote_tids, n_remote_tids, false);
    tuplesort_performsort(so->tid_sortstate);

    // get the first tuple
    so->more_tuples = tuplesort_gettupleslot(so->tid_sortstate, true, false, so->tid_sortslot, NULL);

    // allocate for xs_orderbyvals (*Datum)
    scan->xs_orderbyvals = palloc(sizeof(Datum)); // assumes only one ORDER BY
    scan->xs_orderbynulls = palloc(sizeof(bool)); // TODO: assumes only one ORDER BY

}

// todo: save stats from inserting from base table into the meta

void load_tids_into_sort(Relation index, Tuplesortstate *sortstate, RemoteScanOpaque so, Datum query_datum, ItemPointerData* tids, int n_tids, bool is_local) {
    // todo: make sure that this is just as fast as pgvector's flatscan e.g. using vectorized operations
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->sort_tupdesc, &TTSOpsVirtual);

    for (int i = 0; i < n_tids; i++) {
        // fetch the vector from the base table
        ItemPointer tid = &tids[i]; // these point to so->local_tids or so->remote_tids
        
        // add the tuple
        ExecClearTuple(slot);
        slot->tts_values[0] = ItemPointerGetDatum(tid);
        slot->tts_isnull[0] = false;
        slot->tts_values[1] = BoolGetDatum(is_local);
        slot->tts_isnull[1] = false;
        ExecStoreVirtualTuple(slot);

        tuplesort_puttupleslot(sortstate, slot);
    }
}

ItemPointerData* buffer_get_tids(Relation index, int* return_n_tids)
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
        buf = ReadBuffer(index, currentblkno); 
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
    *return_n_tids = n_tids;    
    return buffer_tids;
}

/*
 * Fetch the next tuple in the given scan
 */
bool remote_gettuple(IndexScanDesc scan, ScanDirection dir)
{
    RemoteScanOpaque so = (RemoteScanOpaque) scan->opaque;
    ItemPointer tid;
    int is_local;
    bool isnull;

    if (!so->more_tuples) {
        return false;
    }

    // merge the results from the buffer and the remote index
    tid = (ItemPointer) DatumGetPointer(slot_getattr(so->tid_sortslot, 1, &isnull));
    is_local = DatumGetBool(slot_getattr(so->tid_sortslot, 2, &isnull));

    scan->xs_heaptid = *tid; // ItemPointerData
    scan->xs_recheck = is_local; // we can only apply predicates on the remote index, so predicates on tuples in the local buffer need to be rechecked
    so->more_tuples = tuplesort_gettupleslot(so->tid_sortstate, true, false, so->tid_sortslot, NULL);
    if (so->more_tuples) {
        ItemPointer new_tid = (ItemPointer) DatumGetPointer(slot_getattr(so->tid_sortslot, 1, &isnull));
        if (ItemPointerCompare(tid, new_tid) == 0) {
            // if the next tuple is the same as the current tuple, then we need to skip
            // N.B. we don't need a loop because the same tid appears at most twice in the sortstate
            elog(DEBUG1, "Skipping duplicate tuple, found in both local buffer and remote index");
            so->more_tuples = tuplesort_gettupleslot(so->tid_sortstate, true, false, so->tid_sortslot, NULL);
        }
    }

    // TODO: we could simplify by setting xs_recheckorderby to make PG reorder all, say, 20K tuples
    // This way we avoid looking up and computing the distance for each tuple twice
    scan->xs_recheckorderby = true; // remote returns an approximate distance which we need to recheck.
    // so set this lower bound to -inf
    scan->xs_orderbyvals[0] = Float8GetDatum(-DBL_MAX);
    scan->xs_orderbynulls[0] = false;

    return true;
}

void no_endscan(IndexScanDesc scan) {};
