#include "remote.h"

#include "remote/remote.h"

#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include <access/reloptions.h>

#include <unistd.h>
#include <access/tableam.h>
// LockRelationForExtension in lmgr.h
#include <storage/lmgr.h>


void generateRandomAlphanumeric(char *s, const int length) {
    char charset[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    if (length) {
        // Seed the random number generator
        srand((unsigned int)time(NULL));
        for (int i = 0; i < length; i++) {
            int key = rand() % (int)(sizeof(charset) - 1);
            *s++ = charset[key];
        }
        *s = '\0'; // Null-terminate the string
    }
}


char* get_remote_index_name(Relation index) {
    // TODO: there is no reason to keep the remote index name
    char* remote_index_name = palloc(REMOTE_NAME_MAX_LENGTH + 1); // remote's maximum index name length is 45
    char* index_name;
    char random_postfix[5];
    int name_length;
    // create the remote_index_name like pgvector-{oid}-{index_name}-{random_postfix}
    index_name = NameStr(index->rd_rel->relname);
    generateRandomAlphanumeric(random_postfix, 4);
    name_length = snprintf(remote_index_name, REMOTE_NAME_MAX_LENGTH+1, "pgvector-%u-%s-%s", index->rd_id, index_name, random_postfix);
    if (name_length > REMOTE_NAME_MAX_LENGTH) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("Remote index name too long"), errhint("The remote index name is %s... and is %d characters long. The maximum length is 45 characters.", remote_index_name, name_length)));
    }
    // check that all chars are alphanumeric or hyphen
    for (int i = 0; i < name_length; i++) {
        if (!isalnum(remote_index_name[i]) && remote_index_name[i] != '-') {
            elog(DEBUG1, "Invalid character: %c", remote_index_name[i]);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Remote index name (%s) contains invalid character %c", remote_index_name, remote_index_name[i]), errhint("The remote index name can only contain alphanumeric characters and hyphens.")));
        }
    }
    return remote_index_name;
}


IndexBuildResult *remote_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    RemoteOptions *opts = (RemoteOptions *) index->rd_options;
    IndexBuildResult *result = palloc(sizeof(IndexBuildResult));
    VectorMetric metric = get_opclass_metric(index);
    char* spec = GET_STRING_RELOPTION(opts, spec);
    int dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;
    char* remote_index_name = get_remote_index_name(index);
    char* host = GET_STRING_RELOPTION(opts, host);
    // provider
    RemoteIndexInterface* remote_index_interface = remote_index_interfaces[opts->provider];

    // check credentials
    if (remote_index_interface->check_credentials != NULL) {
        remote_index_interface->check_credentials();
    }

    // The typical arrangement is that the user must provide exactly one of host and spec.

    // if spec is provided
    if (spec == NULL) {
        // overwrite
        if (opts->overwrite) {
            remote_index_interface->delete_all(host);
        }
        // verify that the host is empty (unless we are skipping the build)
        if (!opts->skip_build) {
            bool empty = remote_index_interface->host_is_empty(host);
            if (!empty) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Host is not empty"), errhint("You must provide an empty host if you are not skipping the build.")));
            }
        }
    } else {
        // create a remote index from the spec
        host = remote_index_interface->create_host_from_spec(dimensions, metric, remote_index_name, spec);
    }

    // init the index pages: static meta, buffer meta, and buffer head
    InitIndexPages(index, metric, dimensions, remote_index_name, host);

    // iterate through the base table and upsert the vectors to the remote index
    result->heap_tuples = 0;
    result->index_tuples = 0;
    if (!opts->skip_build) {
        InsertBaseTable(heap, index, indexInfo, host, result, remote_index_interface);
        // wait for the remote index to finish processing the vectors
        // i.e. describe stats is equal to result->index_tuples
        remote_index_interface->wait_for_index(host, result->index_tuples);
    }
    return result;
}

void InsertBaseTable(Relation heap, Relation index, IndexInfo *indexInfo, char* host, IndexBuildResult *result, RemoteIndexInterface* remote_index_interface) {
    RemoteBuildState buildstate;
    int reltuples;
    // initialize the buildstate
    buildstate.indtuples = 0;
    buildstate.prepared_tuples = palloc(REMOTE_BATCH_SIZE * sizeof(PreparedTuple));
    buildstate.n_prepared_tuples = 0;
    buildstate.remote_index_interface = remote_index_interface;
    strcpy(buildstate.host, host);
    // iterate through the base table and upsert the vectors to the remote index
    reltuples = table_index_build_scan(heap, index, indexInfo, true, true, remote_build_callback, (void *) &buildstate, NULL);
    if (buildstate.n_prepared_tuples > 0) {
        remote_index_interface->bulk_upsert(host, buildstate.prepared_tuples, remote_vectors_per_request, buildstate.n_prepared_tuples);
    }
    pfree(buildstate.prepared_tuples);
    // stats
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
}

void remote_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    RemoteBuildState *buildstate = (RemoteBuildState *) state;
    TupleDesc itup_desc = index->rd_att;
    // prepare the tuple and add it to the prepared_tuples array
    PreparedTuple prepared_tuple = buildstate->remote_index_interface->prepare_tuple_for_bulk_insert(itup_desc, values, isnull, tid);
    buildstate->prepared_tuples[buildstate->n_prepared_tuples] = prepared_tuple;
    buildstate->n_prepared_tuples++;
    // if full, flush
    if (buildstate->n_prepared_tuples == REMOTE_BATCH_SIZE) {
        buildstate->remote_index_interface->bulk_upsert(buildstate->host, buildstate->prepared_tuples, remote_vectors_per_request, buildstate->n_prepared_tuples);
        buildstate->n_prepared_tuples = 0;
    }
    buildstate->indtuples++;
}


/*
 * Create the static meta page
 * Create the buffer meta page
 * Create the buffer head
 */
void InitIndexPages(Relation index, VectorMetric metric, int dimensions, char *remote_index_name, char *host) {
    Buffer meta_buf, buffer_meta_buf, buffer_head_buf;
    Page meta_page, buffer_meta_page, buffer_head_page;
    RemoteStaticMetaPage remote_static_meta_page;
    RemoteBufferMetaPage remote_buffer_meta_page;
    RemoteBufferOpaque buffer_head_opaque;
    RemoteCheckpoint default_checkpoint;
    GenericXLogState *state = GenericXLogStart(index);
    int forkNum = MAIN_FORKNUM;

    // init default checkpoint
    default_checkpoint.blkno = REMOTE_BUFFER_HEAD_BLKNO;
    default_checkpoint.checkpoint_no = 0;
    default_checkpoint.is_checkpoint = true;
    default_checkpoint.n_preceding_tuples = 0;

    // Lock the relation for extension, not really necessary since this is called exactly once in build_index
    LockRelationForExtension(index, ExclusiveLock); 

    // CREATE THE STATIC META PAGE
    meta_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL); LockBuffer(meta_buf, BUFFER_LOCK_EXCLUSIVE);
    if (BufferGetBlockNumber(meta_buf) != REMOTE_STATIC_METAPAGE_BLKNO) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Remote static meta page block number mismatch")));
    }
    meta_page = GenericXLogRegisterBuffer(state, meta_buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(meta_page, BufferGetPageSize(meta_buf), 0); // format as a page
    remote_static_meta_page = RemotePageGetStaticMeta(meta_page);
    remote_static_meta_page->metric = metric;
    remote_static_meta_page->dimensions = dimensions;
    // You must set pd_lower because GenericXLog ignores any changes in the free space between pd_lower and pd_upper
    ((PageHeader) meta_page)->pd_lower = ((char *) remote_static_meta_page - (char *) meta_page) + sizeof(RemoteStaticMetaPageData);

    // copy host and remote_index_name, checking for length
    if (strlcpy(remote_static_meta_page->host, host, REMOTE_HOST_MAX_LENGTH) > REMOTE_HOST_MAX_LENGTH) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("Host name too long"),
                        errhint("The host name is %s... and is %d characters long. The maximum length is %d characters.",
                                host, (int) strlen(host), REMOTE_HOST_MAX_LENGTH)));
    }
    if (strlcpy(remote_static_meta_page->remote_index_name, remote_index_name, REMOTE_NAME_MAX_LENGTH) > REMOTE_NAME_MAX_LENGTH) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("Remote index name too long"),
                        errhint("The remote index name is %s... and is %d characters long. The maximum length is %d characters.",
                                remote_index_name, (int) strlen(remote_index_name), REMOTE_NAME_MAX_LENGTH)));
    }

    // CREATE THE BUFFER META PAGE
    buffer_meta_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL); LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
    Assert(BufferGetBlockNumber(buffer_meta_buf) == REMOTE_BUFFER_METAPAGE_BLKNO);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(buffer_meta_page, BufferGetPageSize(buffer_meta_buf), sizeof(RemoteBufferMetaPageData)); // format as a page
    remote_buffer_meta_page = RemotePageGetBufferMeta(buffer_meta_page);
    // set head, remote_tail, and live_tail to START
    remote_buffer_meta_page->ready_checkpoint = default_checkpoint;
    remote_buffer_meta_page->flush_checkpoint = default_checkpoint;
    remote_buffer_meta_page->latest_checkpoint = default_checkpoint;
    remote_buffer_meta_page->insert_page = REMOTE_BUFFER_HEAD_BLKNO;
    remote_buffer_meta_page->n_tuples_since_last_checkpoint = 0;
    // adjust pd_lower 
    ((PageHeader) buffer_meta_page)->pd_lower = ((char *) remote_buffer_meta_page - (char *) buffer_meta_page) + sizeof(RemoteBufferMetaPageData);

    // CREATE THE BUFFER HEAD
    buffer_head_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL); LockBuffer(buffer_head_buf, BUFFER_LOCK_EXCLUSIVE);
    Assert(BufferGetBlockNumber(buffer_head_buf) == REMOTE_BUFFER_HEAD_BLKNO);
    buffer_head_page = GenericXLogRegisterBuffer(state, buffer_head_buf, GENERIC_XLOG_FULL_IMAGE);
    RemotePageInit(buffer_head_page, BufferGetPageSize(buffer_head_buf));
    buffer_head_opaque = RemotePageGetOpaque(buffer_head_page);
    buffer_head_opaque->checkpoint = default_checkpoint;

    // cleanup
    GenericXLogFinish(state);
    UnlockReleaseBuffer(meta_buf);
    UnlockReleaseBuffer(buffer_meta_buf);
    UnlockReleaseBuffer(buffer_head_buf);
    UnlockRelationForExtension(index, ExclusiveLock);


}


void remote_buildempty(Relation index) {}

void no_buildempty(Relation index){}; // for some reason this is never called even when the base table is empty


VectorMetric get_opclass_metric(Relation index)
{
    FmgrInfo *procinfo = index_getprocinfo(index, 1, 2); // lookup the second support function in the opclass for the first attribute
    Oid collation = index->rd_indcollation[0]; // get the collation of the first attribute
    Datum datum = FunctionCall0Coll(procinfo, collation); // call the support function
    return (VectorMetric) DatumGetInt32(datum);
}