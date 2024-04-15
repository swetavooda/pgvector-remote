// pinecone/clients/pinecone/pinecone.h
#ifndef PINECONE_CLIENT_H
#define PINECONE_CLIENT_H

#include "src/remote/rremote.h"

extern RemoteIndexInterface pinecone_remote_index_interface;

char* pinecone_create_host_from_spec(int dimensions, VectorMetric metric, char* remote_index_name, char* spec);
void pinecone_wait_for_index(char* host, int n_vectors);
void pinecone_check_credentials(void);
bool pinecone_bulk_upsert(char* host, PreparedTuple* prepared_vectors, int remote_vectors_per_request, int n_prepared_tuples);
cJSON** pinecone_query_with_fetch(char* host, int top_k, cJSON* query_vector_values, cJSON* filter, bool include_vector_ids, cJSON* fetch_ids);
PreparedTuple pinecone_prepare_tuple_for_bulk_insert(TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid);

// utils

ItemPointerData pinecone_id_get_heap_tid(char *id);
char* pinecone_id_from_heap_tid(ItemPointerData heap_tid);

#endif // PINECONE_CLIENT_H