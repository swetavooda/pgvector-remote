#include "src/remote/remote.h"

#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include "utils/memutils.h"
#include "storage/lmgr.h"
#include "miscadmin.h" // MyDatabaseId
#include <catalog/index.h>

#include <access/heapam.h>
#include <access/tableam.h>

#define REMOTE_FLUSH_LOCK_IDENTIFIER 1969841813 // random number, uniquely identifies the remote insertion lock
#define REMOTE_APPEND_LOCK_IDENTIFIER 1969841814 // random number, uniquely identifies the remote append lock

#define SET_LOCKTAG_FLUSH(lock, index)  SET_LOCKTAG_ADVISORY(lock, MyDatabaseId, (uint32) index->rd_id, REMOTE_FLUSH_LOCK_IDENTIFIER, 0)
#define SET_LOCKTAG_APPEND(lock, index) SET_LOCKTAG_ADVISORY(lock, MyDatabaseId, (uint32) index->rd_id, REMOTE_APPEND_LOCK_IDENTIFIER, 0)

void RemotePageInit(Page page, Size pageSize)
{
    RemoteBufferOpaque opaque;
    PageInit(page, pageSize, sizeof(RemoteBufferOpaqueData));
    opaque = RemotePageGetOpaque(page);
    opaque->nextblkno = InvalidBlockNumber;
    opaque->prev_checkpoint_blkno = InvalidBlockNumber;
    opaque->checkpoint.is_checkpoint = false;
    // checkpoint
    // ItemPointerSetInvalid
}

/* 
 * add a tuple to the end of the buffer
 * return true if a new page was created
 */
bool AppendBufferTuple(Relation index, ItemPointer heap_tid)
{
    // IndexTuple itup;
    GenericXLogState *state;
    Buffer buffer_meta_buf, insert_buf, newbuf = InvalidBuffer;
    Page buffer_meta_page, insert_page, newpage;
    RemoteBufferOpaque  new_opaque;
    RemoteBufferMetaPage buffer_meta;
    BlockNumber newblkno;
    LOCKTAG remote_append_lock;
    Size itemsz;
    RemoteBufferMetaPageData meta_snapshot;
    bool full;
    bool create_checkpoint = false;
    RemoteOptions *opts = (RemoteOptions *) index->rd_options;
    
    // prepare the index tuple
    // itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    // itup->t_tid = *heap_tid;
    // itemsz = MAXALIGN(IndexTupleSize(itup));
    
    //
    RemoteBufferTuple buffer_tid;
    buffer_tid.tid = *heap_tid;
    itemsz = MAXALIGN(sizeof(RemoteBufferTuple));

    /* LOCKING STRATEGY FOR INSERTION
     * acquire append lock
     * read a snapshot of meta
     * acquire meta.insert_page
     * if insert_page is not full:
     *   add item to insert_page:
     * if insert_page is full:
     *   acquire meta
     *   acquire & create newpage
     *   add item to newpage:
     *     insert_page.nextblkno = newpage.blkno
     *     meta.n_unflushed_tuples += (tuples on old page)
     *     meta.insert_page = newpage.blkno
     *   if this qualifies as a checkpoint:
     *     newpage.prev_checkpoint = meta.latest_checkpoint
     *     meta.latest_checkpoint = newpage.blkno
     *     newpage.representative_vector_heap_tid = itup.t_tid
     *   release newpage, meta
     * release insert_page, newpage, meta
     * release append lock
     * (if it was full and threshold met, we will next try to advance remote head)
     */

    // acquire append lock
    SET_LOCKTAG_APPEND(remote_append_lock, index); LockAcquire(&remote_append_lock, ExclusiveLock, false, false);
    // start WAL logging
    state = GenericXLogStart(index);
    // read a snapshot of the buffer meta
    meta_snapshot = RemoteSnapshotBufferMeta(index);
    // acquire the insert page
    insert_buf = ReadBuffer(index, meta_snapshot.insert_page); LockBuffer(insert_buf, BUFFER_LOCK_EXCLUSIVE);
    insert_page = GenericXLogRegisterBuffer(state, insert_buf, 0);
    // check if the page is full and if we want to create a new checkpoint
    full = PageGetFreeSpace(insert_page) < itemsz;
    create_checkpoint = meta_snapshot.n_tuples_since_last_checkpoint + PageGetMaxOffsetNumber(insert_page) >= opts->batch_size;

    // add item to insert page
    if (!full && !create_checkpoint) {
        // PageAddItem(insert_page, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
        PageAddItem(insert_page, (Item) &buffer_tid, itemsz, InvalidOffsetNumber, false, false);

        // log the number of items on this page MaxOffsetNumber
        elog(DEBUG1, "No new page! Page has %lu items", (unsigned long)PageGetMaxOffsetNumber(insert_page));

        // release insert_page
        GenericXLogFinish(state);
        UnlockReleaseBuffer(insert_buf);
    } else {
        // acquire the meta
        buffer_meta_buf = ReadBuffer(index, REMOTE_BUFFER_METAPAGE_BLKNO); LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
        buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0);
        buffer_meta = RemotePageGetBufferMeta(buffer_meta_page);
        // acquire and create a new page
        LockRelationForExtension(index, ExclusiveLock); // acquire a lock to let us add pages to the relation (this isn't really necessary since we will always have the append lock anyway)
        newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
        UnlockRelationForExtension(index, ExclusiveLock);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
        RemotePageInit(newpage, BufferGetPageSize(newbuf));
        // check that there is room on the new page
        if (PageGetFreeSpace(newpage) < itemsz) elog(ERROR, "A new page was created, but it doesn't have enough space for the new tuple");
        // add item to new page
        // PageAddItem(newpage, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
        PageAddItem(newpage, (Item) &buffer_tid, itemsz, InvalidOffsetNumber, false, false);
        // update insert_page nextblkno
        newblkno = BufferGetBlockNumber(newbuf);
        RemotePageGetOpaque(insert_page)->nextblkno = newblkno;
        // update meta
        buffer_meta->insert_page = newblkno;
        buffer_meta->n_tuples_since_last_checkpoint += PageGetMaxOffsetNumber(insert_page);
        // if this qualifies as a checkpoint, set this page as the latest head checkpoint
        if (create_checkpoint) {
            // create a checkpoint on the opaque of the new page
            new_opaque = RemotePageGetOpaque(newpage);
            new_opaque->prev_checkpoint_blkno = buffer_meta->latest_checkpoint.blkno;
            new_opaque->checkpoint = buffer_meta->latest_checkpoint;
            new_opaque->checkpoint.tid = *heap_tid; // we will assume we have inserted up to this point if we see this in remote
            new_opaque->checkpoint.blkno = newblkno;
            new_opaque->checkpoint.checkpoint_no += 1;
            new_opaque->checkpoint.n_preceding_tuples += buffer_meta->n_tuples_since_last_checkpoint;
            // set this page as the latest head checkpoint
            buffer_meta->latest_checkpoint = new_opaque->checkpoint;
            buffer_meta->n_tuples_since_last_checkpoint = 0;

        }
        // release insert_page, newpage, meta
        GenericXLogFinish(state);
        UnlockReleaseBuffer(insert_buf); UnlockReleaseBuffer(newbuf); UnlockReleaseBuffer(buffer_meta_buf);
    }
    // release append lock
    LockRelease(&remote_append_lock, ExclusiveLock, false);
    return create_checkpoint;
}

/*
 * Insert a tuple into the index
 */
bool remote_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique, 
#if PG_VERSION_NUM >= 140000
                     bool indexUnchanged, 
#endif
                     IndexInfo *indexInfo)
{
    bool checkpoint_created;

    // add a tuple to the buffer (we don't need to use a memory context like the original implementation because we don't need to allocate an index tuple)
    checkpoint_created = AppendBufferTuple(index, heap_tid);

    // if there are enough tuples in the buffer, advance the remote tail
    if (checkpoint_created) {
        elog(DEBUG1, "Checkpoint created. Flushing to Remote");
        FlushToRemote(index);
    }

    // log the state of the relation for debugging

    return false;
}

/*
 * Upload batches of vectors to remote.
 */
void FlushToRemote(Relation index)
{
    Buffer buf, buffer_meta_buf;
    Page page, buffer_meta_page;
    BlockNumber currentblkno = REMOTE_BUFFER_HEAD_BLKNO;
    bool success;
    // get the remote interface
    RemoteOptions *opts = (RemoteOptions *) index->rd_options;
    RemoteIndexInterface *interface = remote_index_interfaces[opts->provider];

    // take a snapshot of the buffer meta
    // we don't need to worry about another transaction advancing the remote tail because we have the remote insertion lock
    RemoteStaticMetaPageData static_meta = RemoteSnapshotStaticMeta(index);
    RemoteBufferMetaPageData buffer_meta = RemoteSnapshotBufferMeta(index);


    // index info
    IndexInfo *indexInfo = BuildIndexInfo(index);
    Datum* index_values = (Datum*) palloc(sizeof(Datum) * indexInfo->ii_NumIndexAttrs);
    bool* index_isnull = (bool*) palloc(sizeof(bool) * indexInfo->ii_NumIndexAttrs);
    // get the base table
    Oid baseTableOid = index->rd_index->indrelid;
    Relation baseTableRel = RelationIdGetRelation(baseTableOid);
    Snapshot snapshot = GetActiveSnapshot();
    // begin the index fetch (this the preferred way for an index to request tuples from its base table)
    IndexFetchTableData *fetchData = baseTableRel->rd_tableam->index_fetch_begin(baseTableRel);
    TupleTableSlot *slot = MakeSingleTupleTableSlot(baseTableRel->rd_att, &TTSOpsBufferHeapTuple);
    bool call_again = false;
    bool all_dead = false;
    bool found = false;

    // hold
    PreparedBulkInsert prepared_tuples = interface->begin_prepare_bulk_insert(index);
    int n_prepared_tuples = 0;

    // acquire the remote insertion lock
    LOCKTAG remote_flush_lock;
    SET_LOCKTAG_FLUSH(remote_flush_lock, index);
    success = LockAcquire(&remote_flush_lock, ExclusiveLock, false, true);
    if (!success) {
        ereport(NOTICE, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("Remote insertion lock not available"),
                        errhint("The remote insertion lock is currently held by another transaction. This is likely because the buffer is being advanced by another transaction. This is not an error, but it may cause a delay in the insertion of new vectors.")));
        return;
    }


    // get the first page
    buf = ReadBuffer(index, buffer_meta.flush_checkpoint.blkno);
    if (BufferIsInvalid(buf)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Remote buffer page not found")));
    }
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    // iterate through the pages
    while (true)
    {
        // Add all tuples on the page.
        for (int i = 1; i <= PageGetMaxOffsetNumber(page); i++)
        {
            ItemId itemid = PageGetItemId(page, i);
            Item item = PageGetItem(page, itemid);
            RemoteBufferTuple buffer_tup = *((RemoteBufferTuple*) item);
            if (!ItemIdIsUsed(itemid)) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                                                      errmsg("Item is not used")));
            if (item == NULL) ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                                              errmsg("Item is null")));

            // log the tid of the index tuple
            elog(DEBUG1, "Flushing tuple with tid %d:%d", ItemPointerGetBlockNumber(&buffer_tup.tid), ItemPointerGetOffsetNumber(&buffer_tup.tid));

            // fetch the tuple from the base table
            found = baseTableRel->rd_tableam->index_fetch_tuple(fetchData, &buffer_tup.tid, snapshot, slot, &call_again, &all_dead);

            // print the tuple
            if (!found) {
                ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
                                errmsg("Tuple not found in heap")));
            } else {
                // extract the indexed columns
                FormIndexDatum(indexInfo, slot, NULL, index_values, index_isnull);
                // we know in advance how many vectors we want to send, correct?
                // each vector is individually prepared and we pass an array

                interface->append_prepare_bulk_insert(prepared_tuples, index->rd_att, index_values, index_isnull, &buffer_tup.tid);
                n_prepared_tuples++;
            }
        }

        // Move to the next page. Stop if there are no more pages.
        // This linked list is probably unnecessary. We could could just use blockno++
        currentblkno = RemotePageGetOpaque(page)->nextblkno;
        if (!BlockNumberIsValid(currentblkno)) break; 

        // release the current buffer and get the next buffer
        UnlockReleaseBuffer(buf);
        buf = ReadBuffer(index, currentblkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        // If we have reached a checkpoint, push them to the remote index and update the remote checkpoint with a representative vector heap tid
        if (RemotePageGetOpaque(page)->checkpoint.is_checkpoint) {
            GenericXLogState *state = GenericXLogStart(index); // start a new WAL record
 
            interface->end_prepare_bulk_insert(prepared_tuples);
            interface->bulk_upsert(static_meta.host, prepared_tuples, n_prepared_tuples);
            interface->delete_prepared_bulk_insert(prepared_tuples);
            prepared_tuples = interface->begin_prepare_bulk_insert(index);
            n_prepared_tuples = 0;

            // lock the buffer meta page
            buffer_meta_buf = ReadBuffer(index, REMOTE_BUFFER_METAPAGE_BLKNO);
            LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
            buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0);

            // update the buffer meta page
            RemotePageGetBufferMeta(buffer_meta_page)->flush_checkpoint = RemotePageGetOpaque(page)->checkpoint;

            // save and release
            GenericXLogFinish(state);
            UnlockReleaseBuffer(buffer_meta_buf);

            // stop if we don't expect to have another batch because we have reached the last checkpoint
            if (buffer_meta.latest_checkpoint.blkno == currentblkno) break;
        }
    }
    // free the prepared tuples
    interface->end_prepare_bulk_insert(prepared_tuples);
    interface->delete_prepared_bulk_insert(prepared_tuples);
    UnlockReleaseBuffer(buf); // release the last buffer

    // end the index fetch
    ExecDropSingleTupleTableSlot(slot);
    baseTableRel->rd_tableam->index_fetch_end(fetchData);
    // close the base table
    RelationClose(baseTableRel);

    // release the lock
    LockRelease(&remote_flush_lock, ExclusiveLock, false);
}
