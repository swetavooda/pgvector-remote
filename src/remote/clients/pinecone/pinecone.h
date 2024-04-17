// pinecone/clients/pinecone/pinecone.h
#ifndef PINECONE_CLIENT_H
#define PINECONE_CLIENT_H

#include "src/remote/cJSON.h"
#include "src/remote/remote.h"  
#include "src/vector.h"
#include "postgres.h" // bool

#define PINECONE_NAME_MAX_LENGTH 45
#define PINECONE_HOST_MAX_LENGTH 100

// GUC
extern char* pinecone_api_key;
extern int pinecone_vectors_per_request; // number of vectors to send in a single request (there can be concurrent requests)
extern int pinecone_network_cost;


extern RemoteIndexInterface pinecone_remote_index_interface;

cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys);
cJSON* checkpoints_get_pinecone_ids(RemoteCheckpoint* checkpoints, int n_checkpoints);
ItemPointerData* pinecone_extract_ctids_from_fetch_response(cJSON* fetch_response, int* n_results);

// interface
// create
char* pinecone_create_host_from_spec(int dimensions, VectorMetric metric, char* spec, Relation index);
void pinecone_validate_host_schema(char* host, int dimensions, VectorMetric metric, Relation index);
int pinecone_count_live(char* host);
int pinecone_est_network_cost(void);
// bulk insert
PreparedBulkInsert pinecone_begin_prepare_bulk_insert(Relation index);
void pinecone_append_prepare_bulk_insert(PreparedBulkInsert prepared_vectors, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid);
void pinecone_end_prepare_bulk_insert(PreparedBulkInsert prepared_vectors);
void pinecone_delete_prepared_bulk_insert(PreparedBulkInsert prepared_vectors);
bool pinecone_bulk_upsert(char* host, PreparedBulkInsert prepared_vectors, int n_prepared_tuples);
// query
PreparedQuery pinecone_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k);
ItemPointerData* pinecone_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids);

// utils
cJSON* text_array_get_json(Datum value);
ItemPointerData pinecone_id_get_heap_tid(char *id);
char* pinecone_id_from_heap_tid(ItemPointerData heap_tid);
cJSON* tuple_get_remote_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id);

#endif // PINECONE_CLIENT_H