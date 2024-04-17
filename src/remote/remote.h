#ifndef REMOTE_INDEX_AM_H
#define REMOTE_INDEX_AM_H

#include "postgres.h"
// curl
#include <curl/curl.h>
#include "src/remote/curl_utils.h"

// todo: get these out of the header
#include "access/amapi.h"
#include "src/vector.h"

#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/array.h>
#include "access/relscan.h"
#include "storage/block.h"
#include <access/reloptions.h>

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

#define DEFAULT_SPEC ""
#define DEFAULT_HOST ""

// strategy numbers
#define REMOTE_STRATEGY_ARRAY_OVERLAP 7
#define REMOTE_STRATEGY_ARRAY_CONTAINS 2

typedef char* PreparedBulkInsert;
typedef char* PreparedQuery;

// providers
extern relopt_enum_elt_def provider_enum_options[];

// structs
typedef struct RemoteScanOpaqueData
{
    int dimensions;
    bool first;

    // sorting
    ItemPointerData *local_tids, *remote_tids;
    FmgrInfo *procinfo; // distance function
    TupleDesc sort_tupdesc; // tuple descriptor for sorting (distance float4, tid TID)
    Tuplesortstate *local_sortstate, *remote_sortstate;
    TupleTableSlot *local_sortslot, *remote_sortslot;
    bool more_local_tuples, more_remote_tuples;

} RemoteScanOpaqueData;
typedef RemoteScanOpaqueData *RemoteScanOpaque;

#define REMOTE_HOST_MAX_LENGTH 100
typedef struct RemoteStaticMetaPageData
{
    int dimensions;
    char host[REMOTE_HOST_MAX_LENGTH + 1];
    VectorMetric metric;
} RemoteStaticMetaPageData;
typedef RemoteStaticMetaPageData *RemoteStaticMetaPage;

typedef enum Provider
{
    PINECONE_PROVIDER,
    MILVUS_PROVIDER,
    NUM_PROVIDERS
} Provider;

typedef struct RemoteOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
    int         spec; // spec is a string; this is its offset in the rd_options
    Provider    provider;
    int         host;
    bool        overwrite; // todo: should this be int?
    bool        skip_build;
    int         batch_size;
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


// create
typedef char* (*ri_create_host_from_spec_function)(int dimensions, VectorMetric metric, char* spec, Relation index);
typedef void (*ri_validate_host_schema_function)(char* host, int dimensions, VectorMetric metric, Relation index);
typedef void (*ri_delete_all_function)(char* host);
typedef int (*ri_count_live_function)(char* host);
typedef int (*ri_est_network_cost_function)(void);
typedef void (*ri_spec_validator_function)(const char *spec);
// insert
typedef PreparedBulkInsert (*ri_begin_prepare_bulk_insert_function)(Relation index);
typedef void (*ri_append_prepare_bulk_insert_function)(PreparedBulkInsert prepared_vectors, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid);
typedef void (*ri_end_prepare_bulk_insert_function)(PreparedBulkInsert prepared_vectors);
typedef void (*ri_delete_prepared_bulk_insert_function)(PreparedBulkInsert prepared_vectors);
typedef bool (*ri_bulk_upsert_function)(char* host, PreparedBulkInsert prepared_vectors, int n_prepared_tuples);
// query
typedef PreparedQuery (*ri_prepare_query_function)(Relation index, ScanKey keys, int nkeys, Vector* vector, int top_k);
typedef ItemPointerData* (*ri_query_with_fetch_function)(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids);

typedef struct
{
    // create index
    ri_create_host_from_spec_function create_host_from_spec;
    ri_validate_host_schema_function validate_host_schema;
    ri_delete_all_function delete_all;
    ri_count_live_function count_live;
    ri_est_network_cost_function est_network_cost;
    ri_spec_validator_function spec_validator;
    // insert
    ri_begin_prepare_bulk_insert_function begin_prepare_bulk_insert;
    ri_append_prepare_bulk_insert_function append_prepare_bulk_insert;
    ri_end_prepare_bulk_insert_function end_prepare_bulk_insert;
    ri_delete_prepared_bulk_insert_function delete_prepared_bulk_insert;
    ri_bulk_upsert_function bulk_upsert; /* bulk_upsert is responsible for freeing each PreparedBulkInsert */
    // query
    ri_prepare_query_function prepare_query;
    ri_query_with_fetch_function query_with_fetch;
} RemoteIndexInterface;

extern RemoteIndexInterface* remote_index_interfaces[NUM_PROVIDERS];

typedef struct RemoteBuildState
{
    int64 indtuples; // total number of tuples indexed
    PreparedBulkInsert prepared_tuples; // array of prepared tuples
    int n_prepared_tuples; // number of prepared tuples
    char host[100];
    RemoteIndexInterface* remote_index_interface;
    int batch_size;
} RemoteBuildState;


// GUC variables
extern int remote_top_k;
extern int remote_requests_per_batch;
extern int remote_max_buffer_scan;
extern int remote_max_fetched_vectors_for_liveness_check;
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
void InsertBaseTable(Relation heap, Relation index, IndexInfo *indexInfo, char* host, IndexBuildResult *result, RemoteIndexInterface* remote_index_interface);
void remote_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
void InitIndexPages(Relation index, VectorMetric metric, int dimensions,  char *host);
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
void remote_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
ItemPointerData* buffer_get_tids(Relation index, int* n_local_tids);
void load_tids_into_sort(Relation index, Tuplesortstate *sortstate, RemoteScanOpaque so, Datum query_datum, ItemPointerData* tids, int n_tids);
bool remote_gettuple(IndexScanDesc scan, ScanDirection dir);
void no_endscan(IndexScanDesc scan);
RemoteCheckpoint* get_checkpoints_to_fetch(Relation index, int* n_checkpoints);


// vacuum
IndexBulkDeleteResult *remote_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

// validate
bool no_validate(Oid opclassoid);

// utils
// converting between postgres tuples and json vectors
// read and write meta pages
RemoteStaticMetaPageData RemoteSnapshotStaticMeta(Relation index);
RemoteBufferMetaPageData RemoteSnapshotBufferMeta(Relation index);
RemoteBufferOpaqueData RemoteSnapshotBufferOpaque(Relation index, BlockNumber blkno);
void set_buffer_meta_page(Relation index, RemoteCheckpoint* ready_checkpoint, RemoteCheckpoint* flush_checkpoint, RemoteCheckpoint* latest_checkpoint, BlockNumber* insert_page, int* n_tuples_since_last_checkpoint);
char* checkpoint_to_string(RemoteCheckpoint checkpoint);
char* buffer_meta_to_string(RemoteBufferMetaPageData buffer_meta);
char* buffer_opaque_to_string(RemoteBufferOpaqueData buffer_opaque);
void remote_print_relation(Relation index);

// helpers
Oid get_index_oid_from_name(char* index_name);
void lookup_mock_response(CURL* hnd, ResponseData* response_data, CURLcode* curl_code);


// misc.
void initialize_remote_index_interfaces(void);

#endif // REMOTE_INDEX_AM_H