// milvus/clients/pinecone/pinecone.h
#ifndef MILVUS_CLIENT_H
#define MILVUS_CLIENT_H

#include "src/remote/remote.h"  
#include "src/vector.h"
#include "postgres.h" // bool

// GUC
extern char* milvus_api_key;

// interface
// create
char* milvus_create_host_from_spec(int dimensions, VectorMetric metric, char* remote_index_name, char* spec);
void milvus_check_credentials(void);
int milvus_count_live(char* host);
// bulk insert
PreparedBulkInsert milvus_begin_prepare_bulk_insert(TupleDesc tupdesc);
void milvus_append_prepare_bulk_insert(PreparedBulkInsert prepared_vectors, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid);
void milvus_end_prepare_bulk_insert(PreparedBulkInsert prepared_vectors);
void milvus_delete_prepared_bulk_insert(PreparedBulkInsert prepared_vectors);
bool milvus_bulk_upsert(char* host, PreparedBulkInsert prepared_vectors, int remote_vectors_per_request, int n_prepared_tuples);
// query
PreparedQuery milvus_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k);
ItemPointerData* milvus_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids);

extern RemoteIndexInterface milvus_remote_index_interface;

int triangle_num(int n);


#endif // MILVUS_CLIENT_H