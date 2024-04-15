#include "remote/remote.h"
#include "remote/rremote.h"
#include "remote/clients/pinecone/pinecone.h"
#include "remote/clients/pinecone/pinecone_api.h"
#include "postgres.h"

#include "access/tupdesc.h"


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


PreparedQuery pinecone_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k) {
    cJSON* filter = pinecone_build_filter(index, keys, nkeys);
    cJSON* query_vector_values = cJSON_CreateFloatArray(vec->x, vec->dim);
    cJSON* request_body = cJSON_CreateObject();
    cJSON_AddItemToObject(request_body, "topK", cJSON_CreateNumber(top_k));
    cJSON_AddItemToObject(request_body, "vector", query_vector_values);
    cJSON_AddItemToObject(request_body, "filter", filter);
    cJSON_AddItemToObject(request_body, "includeValues", cJSON_CreateFalse());
    cJSON_AddItemToObject(request_body, "includeMetadata", cJSON_CreateFalse());
    return (PreparedQuery) request_body;
}

cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys) {
    cJSON *filter = cJSON_CreateObject();
    cJSON *and_list = cJSON_CreateArray();
    const char* remote_filter_operators[] = {"$lt", "$lte", "$eq", "$gte", "$gt", "$ne", "$in"};
    for (int i = 0; i < nkeys; i++)
    {
        cJSON *key_filter = cJSON_CreateObject();
        cJSON *condition = cJSON_CreateObject();
        cJSON *condition_value = NULL;
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, keys[i].sk_attno - 1);

        if (td->atttypid == TEXTARRAYOID && keys[i].sk_strategy == REMOTE_STRATEGY_ARRAY_CONTAINS) {
            // contains (list_of_strings @> ARRAY[tag1, tag2])
            // $and: [ {list_of_strings: {$in: [tag1]}}, {list_of_strings: {$in: [tag2]}} ]
            cJSON* tags = text_array_get_json(keys[i].sk_argument);
            cJSON* tag;
            cJSON_ArrayForEach(tag, tags) {
                cJSON* condition_contains_tag = cJSON_CreateObject(); // list_of_strings: {$in: [tag1]}
                cJSON* predicate_contains_tag = cJSON_CreateObject(); // {$in: [tag1]}
                cJSON* single_tag_list = cJSON_CreateArray(); // [tag1]
                cJSON_AddItemToArray(single_tag_list, cJSON_Duplicate(tag, true)); // [tag1]
                cJSON_AddItemToObject(predicate_contains_tag, "$in", single_tag_list); // {$in: [tag1]}
                cJSON_AddItemToObject(condition_contains_tag, td->attname.data, predicate_contains_tag); // list_of_strings: {$in: [tag1]}
                cJSON_AddItemToArray(and_list, condition_contains_tag);
            }
            // cJSON_Delete(tags);
        } else {
            switch (td->atttypid)
            {
                case BOOLOID:
                    condition_value = cJSON_CreateBool(DatumGetBool(keys[i].sk_argument));
                    break;
                case FLOAT8OID:
                    condition_value = cJSON_CreateNumber(DatumGetFloat8(keys[i].sk_argument));
                    break;
                case TEXTOID:
                    condition_value = cJSON_CreateString(text_to_cstring(DatumGetTextP(keys[i].sk_argument)));
                    break;
                case TEXTARRAYOID:
                    // overlap
                    if (keys[i].sk_strategy != REMOTE_STRATEGY_ARRAY_OVERLAP) {
                        ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                 errmsg("Unsupported operator for text[] datatype. Must be && (overlap)")));
                    }
                    condition_value = text_array_get_json(keys[i].sk_argument);
                    break;
                    // contains (list_of_strings @> ARRAY[tag1, tag2])
                    // $and: [ {list_of_strings: {$in: [tag1]}}, {list_of_strings: {$in: [tag2]}} ]
                default:
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("Unsupported datatype for remote index scan. Must be one of bool, float8, text, text[]")));
                    break;
            }
            // this only works if all datatypes use the same strategy naming convention. todo: document this
            cJSON_AddItemToObject(condition, remote_filter_operators[keys[i].sk_strategy - 1], condition_value);
            cJSON_AddItemToObject(key_filter, td->attname.data, condition);
            cJSON_AddItemToArray(and_list, key_filter);
        }
    }
    cJSON_AddItemToObject(filter, "$and", and_list);
    return filter;
}


// prepare query

ItemPointerData* pinecone_query_with_fetch(char* host, int top_k, PreparedQuery query, int top_k, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return) {
    cJSON* request_body = (cJSON*) query;
    cJSON** responses = remote_query_with_fetch(pinecone_api_key, host, top_k, request_body, include_vector_ids, fetch_ids);
    ItemPointerData *results = palloc0(sizeof(ItemPointerData) * top_k); // 0 is an invalid ItemPointerData
    // find the best checkpoint
    cJSON* fetch_response = responses[1];
    *best_checkpoint_return = checkpoints[0]; // TODO: get the actual best checkpoint // it might be easier to just return an arr of known visible cpoints
    // extract the ctids from the response
    cJSON* query_response = responses[0];
    cJSON* matches = cJSON_GetObjectItemCaseSensitive(query_response, "matches");
    cJSON* match = matches->child;
    int n = 0;
    while (match != NULL) {
        cJSON* vector_id = cJSON_GetObjectItemCaseSensitive(match, "id");
        char* remote_id = cJSON_GetStringValue(vector_id);
        ItemPointerData ctid = pinecone_id_get_heap_tid(remote_id);
        results[n++] = ctid;
        match = match->next;
    }
    return results;
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
