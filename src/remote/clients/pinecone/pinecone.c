#include "remote/remote.h"
#include "remote/rremote.h"
#include "remote/clients/pinecone/pinecone.h"
#include "remote/clients/pinecone/remote_api.h"
#include "postgres.h"


#include <stdio.h>
#include <stdlib.h>

const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT] = {
    "",
    "euclidean",
    "cosine",
    "dotproduct"
};

// char* CreateRemoteIndexAndWait(Relation index, cJSON* spec_json, VectorMetric metric, char* remote_index_name, int dimensions) {
        // host = remote_index_interface->create_host_from_spec(dimensions, metric, remote_index_name, spec);
char* pinecone_create_host_from_spec(int dimensions, VectorMetric metric, char* remote_index_name, char* spec) {
    char* host = palloc(100);
    const char* remote_metric_name = vector_metric_to_pinecone_metric[metric];
    cJSON* spec_json = cJSON_Parse(spec);
    cJSON* create_response = remote_create_index(pinecone_api_key, remote_index_name, dimensions, remote_metric_name, spec_json);
    host = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(create_response, "host"));
    return host;
    // now we wait until the remote index is done initializing
    // todo: timeout and error handling
    // while (!cJSON_IsTrue(cJSON_GetObjectItem(cJSON_GetObjectItem(create_response, "status"), "ready")))
    // {
        // elog(DEBUG1, "Waiting for remote index to initialize...");
        // sleep(1);
        // create_response = describe_index(pinecone_api_key, remote_index_name);
    // }
    // sleep(2); // TODO: ping the host with get_index_stats instead of pinging remote.io with describe_index
    // return host;
}

void pinecone_wait_for_index(char* host, int n_vectors) {
    elog(NOTICE, "Waiting for Pinecone index to initialize");
}

void pinecone_check_credentials(void) {
    elog(NOTICE, "Checking Pinecone credentials");
    if (pinecone_api_key == NULL || strlen(pinecone_api_key) == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Pinecone API key not set"),
                 errhint("Set the Pinecone API key using the remote.pinecone_api_key GUC. E.g. ALTER SYSTEM SET remote.pinecone_api_key TO 'your-api-key'")));
    }
}

bool pinecone_bulk_upsert(char* host, PreparedTuple* prepared_vectors, int remote_vectors_per_request, int n_prepared_tuples) {
    cJSON *json_vectors, *response;
    json_vectors = cJSON_CreateArray();
    for (int i = 0; i < n_prepared_tuples; i++) {
        cJSON* json_vector = (cJSON*) prepared_vectors[i];
        cJSON_AddItemToArray(json_vectors, json_vector);
    }
    response = remote_bulk_upsert(pinecone_api_key, host, json_vectors, remote_vectors_per_request);
    cJSON_Delete(json_vectors);
    elog(DEBUG1, "Pinecone bulk upsert response: %s", cJSON_Print(response));
    if (response != NULL && cJSON_GetObjectItemCaseSensitive(response, "upsertedCount") != NULL) {
        return true;
    }
    return false;
}

cJSON** pinecone_query_with_fetch(char* host, int top_k, cJSON* query_vector_values, cJSON* filter, bool include_vector_ids, cJSON* fetch_ids) {
    cJSON** responses = remote_query_with_fetch(pinecone_api_key, host, top_k, query_vector_values, filter, include_vector_ids, fetch_ids);
    return responses;
}

PreparedTuple pinecone_prepare_tuple_for_bulk_insert(TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid) {
    char* vector_id = remote_id_from_heap_tid(*ctid);
    cJSON* json_vector = tuple_get_remote_vector(tupdesc, values, nulls, vector_id);
    return (PreparedTuple) json_vector;
}

RemoteIndexInterface pinecone_remote_index_interface = {
    .check_credentials = pinecone_check_credentials,
    .create_host_from_spec = pinecone_create_host_from_spec,
    .wait_for_index = pinecone_wait_for_index,
    .bulk_upsert = pinecone_bulk_upsert,
    .query_with_fetch = pinecone_query_with_fetch,
    .prepare_tuple_for_bulk_insert = pinecone_prepare_tuple_for_bulk_insert
};
