#include "remote/clients/milvus/milvus.h"

#include "postgres.h"


#include <stdio.h>
#include <stdlib.h>

// CREATE
void milvus_check_credentials(void) {
    return;
}

// UPSERT
void milvus_end_prepare_bulk_insert(PreparedBulkInsert prepared_vectors) {
    return;
}
void milvus_delete_prepared_bulk_insert(PreparedBulkInsert prepared_vectors) {
    return;
}


RemoteIndexInterface milvus_remote_index_interface = {
    // create index
    .check_credentials = milvus_check_credentials,
    .create_host_from_spec = milvus_create_host_from_spec,
    .count_live = milvus_count_live,
    // upsert
    .begin_prepare_bulk_insert = milvus_begin_prepare_bulk_insert,
    .append_prepare_bulk_insert = milvus_append_prepare_bulk_insert,
    .end_prepare_bulk_insert = milvus_end_prepare_bulk_insert,
    .delete_prepared_bulk_insert = milvus_delete_prepared_bulk_insert,
    .bulk_upsert = milvus_bulk_upsert,
    // query
    .prepare_query = milvus_prepare_query,
    .query_with_fetch = milvus_query_with_fetch,
};
