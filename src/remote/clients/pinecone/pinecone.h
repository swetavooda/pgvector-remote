// pinecone/clients/pinecone/pinecone.h
#ifndef PINECONE_CLIENT_H
#define PINECONE_CLIENT_H

#include "src/remote/rremote.h"

extern RemoteIndexInterface pinecone_remote_index_interface;

cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys);
cJSON* checkpoints_get_pinecone_ids(RemoteCheckpoint* checkpoints, int n_checkpoints);
ItemPointerData* pinecone_extract_ctids_from_fetch_response(cJSON* fetch_response, int* n_results);

// interface
char* pinecone_create_host_from_spec(int dimensions, VectorMetric metric, char* remote_index_name, char* spec);
void pinecone_wait_for_index(char* host, int n_vectors);
void pinecone_check_credentials(void);
bool pinecone_bulk_upsert(char* host, PreparedTuple* prepared_vectors, int remote_vectors_per_request, int n_prepared_tuples);
ItemPointerData* pinecone_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids);
PreparedTuple pinecone_prepare_tuple_for_bulk_insert(TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid);
PreparedQuery pinecone_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k);

// utils
cJSON* text_array_get_json(Datum value);
ItemPointerData pinecone_id_get_heap_tid(char *id);
char* pinecone_id_from_heap_tid(ItemPointerData heap_tid);
cJSON* tuple_get_remote_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id);

#endif // PINECONE_CLIENT_H