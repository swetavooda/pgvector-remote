#ifndef REMOTE_INDEX_AM_H
#define REMOTE_INDEX_AM_H

#include "remote_api.h"
#include "postgres.h"

// todo: get these out of the header
#include "access/amapi.h"
#include "src/vector.h"

#include "remote/remote.h"

#include "src/cJSON.h"
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/array.h>
#include "access/relscan.h"
#include "storage/block.h"

#define REMOTE_DEFAULT_BUFFER_THRESHOLD 2000
#define REMOTE_MIN_BUFFER_THRESHOLD 1
#define REMOTE_MAX_BUFFER_THRESHOLD 10000

#define REMOTE_STATIC_METAPAGE_BLKNO 0
#define REMOTE_BUFFER_METAPAGE_BLKNO 1
#define REMOTE_BUFFER_HEAD_BLKNO 2

#define INVALID_CHECKPOINT_NUMBER -1

#define RemotePageGetOpaque(page)	((RemoteBufferOpaque) PageGetSpecialPointer(page))
#define RemotePageGetStaticMeta(page)	((RemoteStaticMetaPage) PageGetContents(page))
#define RemotePageGetBufferMeta(page)    ((RemoteBufferMetaPage) PageGetContents(page))

// remote specific limits

#define REMOTE_NAME_MAX_LENGTH 45
#define REMOTE_HOST_MAX_LENGTH 100

#define DEFAULT_SPEC "{}"
#define DEFAULT_HOST ""

// strategy numbers
#define REMOTE_STRATEGY_ARRAY_OVERLAP 7
#define REMOTE_STRATEGY_ARRAY_CONTAINS 2

// structs
typedef struct RemoteScanOpaqueData
{
    int dimensions;
    VectorMetric metric;
    bool first;

    // sorting
    Tuplesortstate *sortstate;
    TupleDesc tupdesc;
    TupleTableSlot *slot; // TODO ??
    bool isnull;
    bool more_buffer_tuples;
    char* bloom_filter;
    size_t bloom_filter_size;

    // support functions
    FmgrInfo *procinfo;

    // results
    cJSON* remote_results;

} RemoteScanOpaqueData;
typedef RemoteScanOpaqueData *RemoteScanOpaque;

extern const char* vector_metric_to_remote_metric[VECTOR_METRIC_COUNT];

typedef struct RemoteStaticMetaPageData
{
    int dimensions;
    char host[REMOTE_HOST_MAX_LENGTH + 1];
    char remote_index_name[REMOTE_NAME_MAX_LENGTH + 1];
    VectorMetric metric;
} RemoteStaticMetaPageData;
typedef RemoteStaticMetaPageData *RemoteStaticMetaPage;
typedef struct RemoteBuildState
{
    int64 indtuples; // total number of tuples indexed
    cJSON *json_vectors; // array of json vectors
    char host[100];
} RemoteBuildState;

typedef struct RemoteOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
    int         spec; // spec is a string; this is its offset in the rd_options
    Provider    provider;
    int         host;
    bool        overwrite; // todo: should this be int?
    bool        skip_build;
}			RemoteOptions;

typedef struct RemoteCheckpoint
{
    int checkpoint_no; // unused by fetch_ids
    BlockNumber blkno; // unused in the page's opaque data
    ItemPointerData tid; // unused in the buffer meta
    int n_preceding_tuples; 
    bool is_checkpoint;
} RemoteCheckpoint;

typedef struct RemoteBufferMetaPageData
{
    // FIFO pointers
    RemoteCheckpoint ready_checkpoint;
    RemoteCheckpoint flush_checkpoint;
    RemoteCheckpoint latest_checkpoint;

    // INSERT PAGE
    BlockNumber insert_page;
    int n_tuples_since_last_checkpoint; // (does not include the tuples on the insert page)
} RemoteBufferMetaPageData;
typedef RemoteBufferMetaPageData *RemoteBufferMetaPage;


typedef struct RemoteBufferOpaqueData
{
    BlockNumber nextblkno;
    BlockNumber prev_checkpoint_blkno;

    // checkpoints for uploading and checking liveness
    RemoteCheckpoint checkpoint;
} RemoteBufferOpaqueData;
typedef RemoteBufferOpaqueData *RemoteBufferOpaque;


typedef struct RemoteBufferTuple
{
    ItemPointerData tid;
    int16 flags;
} RemoteBufferTuple;
#define REMOTE_BUFFER_TUPLE_VACUUMED 1 << 0

// GUC variables
extern char* remote_api_key;
extern int remote_top_k;
extern int remote_vectors_per_request;
extern int remote_requests_per_batch;
extern int remote_max_buffer_scan;
extern int remote_max_fetched_vectors_for_liveness_check;
#define REMOTE_BATCH_SIZE remote_vectors_per_request * remote_requests_per_batch
// GUC variables for testing
#ifdef REMOTE_MOCK
extern bool remote_use_mock_response;
#endif

// function declarations

// remote.c
Datum remotehandler(PG_FUNCTION_ARGS); // handler
void RemoteInit(void); // GUC and Index Options
bytea * remote_options(Datum reloptions, bool validate);
void no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages);

// build
void generateRandomAlphanumeric(char *s, const int length);
char* get_remote_index_name(Relation index);
IndexBuildResult *remote_build(Relation heap, Relation index, IndexInfo *indexInfo);
char* CreateRemoteIndexAndWait(Relation index, cJSON* spec_json, VectorMetric metric, char* remote_index_name, int dimensions);
void InsertBaseTable(Relation heap, Relation index, IndexInfo *indexInfo, char* host, IndexBuildResult *result);
void remote_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
void InitIndexPages(Relation index, VectorMetric metric, int dimensions, char *remote_index_name, char *host, int forkNum);
void remote_buildempty(Relation index);
void no_buildempty(Relation index); // for some reason this is never called even when the base table is empty
VectorMetric get_opclass_metric(Relation index);

// insert
bool AppendBufferTupleInCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo);
void RemotePageInit(Page page, Size pageSize);
bool AppendBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel);
bool remote_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique, 
#if PG_VERSION_NUM >= 140000
                     bool indexUnchanged, 
#endif
                     IndexInfo *indexInfo);
void FlushToRemote(Relation index);

// scan
IndexScanDesc remote_beginscan(Relation index, int nkeys, int norderbys);
cJSON* remote_build_filter(Relation index, ScanKey keys, int nkeys);
void remote_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
void load_buffer_into_sort(Relation index, RemoteScanOpaque so, Datum query_datum, TupleDesc index_tupdesc);
bool remote_gettuple(IndexScanDesc scan, ScanDirection dir);
void no_endscan(IndexScanDesc scan);
RemoteCheckpoint* get_checkpoints_to_fetch(Relation index);
RemoteCheckpoint get_best_fetched_checkpoint(Relation index, RemoteCheckpoint* checkpoints, cJSON* fetch_results);
cJSON *fetch_ids_from_checkpoints(RemoteCheckpoint *checkpoints);
#define BUFFER_BLOOM_K 20 // bloom filter k 



// vacuum
IndexBulkDeleteResult *remote_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

// validate
void remote_spec_validator(const char *spec);
void remote_host_validator(const char *spec);
void validate_api_key(void);
void validate_vector_nonzero(Vector* vector);
bool no_validate(Oid opclassoid);

// utils
// converting between postgres tuples and json vectors
cJSON* tuple_get_remote_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id);
cJSON* index_tuple_get_remote_vector(Relation index, IndexTuple itup);
cJSON* heap_tuple_get_remote_vector(Relation heap, HeapTuple htup);
char* remote_id_from_heap_tid(ItemPointerData heap_tid);
ItemPointerData remote_id_get_heap_tid(char *id);
cJSON* text_array_get_json(Datum value);
// read and write meta pages
RemoteStaticMetaPageData RemoteSnapshotStaticMeta(Relation index);
RemoteBufferMetaPageData RemoteSnapshotBufferMeta(Relation index);
RemoteBufferOpaqueData RemoteSnapshotBufferOpaque(Relation index, BlockNumber blkno);
void set_buffer_meta_page(Relation index, RemoteCheckpoint* ready_checkpoint, RemoteCheckpoint* flush_checkpoint, RemoteCheckpoint* latest_checkpoint, BlockNumber* insert_page, int* n_tuples_since_last_checkpoint);
char* checkpoint_to_string(RemoteCheckpoint checkpoint);
char* buffer_meta_to_string(RemoteBufferMetaPageData buffer_meta);
char* buffer_opaque_to_string(RemoteBufferOpaqueData buffer_opaque);
void remote_print_relation(Relation index);
// hashing and bloom filters
uint64 murmurhash64(uint64 data);
uint32 hash_tid(ItemPointerData tid, int seed);

// helpers
Oid get_index_oid_from_name(char* index_name);
void lookup_mock_response(CURL* hnd, ResponseData* response_data, CURLcode* curl_code);


// misc.

#endif // REMOTE_INDEX_AM_H