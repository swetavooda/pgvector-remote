#include "remote/remote.h"
#include "remote/rremote.h"
#include "remote/clients/pinecone/pinecone.h"
#include "remote/clients/pinecone/pinecone_api.h"
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/index.h"

#include <stdio.h>
#include <stdlib.h>

#include <storage/bufmgr.h>
#include "catalog/pg_operator_d.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include <time.h>
#include "common/hashfn.h"

#include <catalog/index.h>
#include <access/heapam.h>
#include <access/tableam.h>

#include <math.h>

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
    // cJSON* index_stats_response;
    // index_stats_response = remote_get_index_stats(pinecone_api_key, host);
    // while (cJSON_GetObjectItemCaseSensitive(index_stats_response, "totalVectorCount")->valueint < result->index_tuples) {
        // sleep(1);
        // index_stats_response = remote_get_index_stats(pinecone_api_key, host);
    // }
}

void pinecone_check_credentials(void) {
    if (pinecone_api_key == NULL || strlen(pinecone_api_key) == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Pinecone API key not set"),
                 errhint("Set the Pinecone API key using the remote.pinecone_api_key GUC. E.g. ALTER SYSTEM SET remote.pinecone_api_key TO 'your-api-key'")));
    }
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


ItemPointerData* pinecone_extract_ctids_from_fetch_response(cJSON* fetch_response, int* n_results) {
    cJSON* vectors = cJSON_GetObjectItemCaseSensitive(fetch_response, "vectors");
    cJSON* vector;
    ItemPointerData *results;
    int k = 0;
    *n_results = cJSON_GetArraySize(vectors);
    results = palloc0(sizeof(ItemPointerData) * *n_results); // 0 is an invalid ItemPointerData
    cJSON_ArrayForEach(vector, vectors) {
        char* id_str = vector->string;
        ItemPointerData ctid = pinecone_id_get_heap_tid(id_str);
        results[k++] = ctid;
    }
    return results;
}

cJSON* checkpoints_get_pinecone_ids(RemoteCheckpoint* checkpoints, int n_checkpoints) {
    cJSON* ids = cJSON_CreateArray();
    for (int i = 0; i < n_checkpoints; i++) {
        cJSON* id = cJSON_CreateString(pinecone_id_from_heap_tid(checkpoints[i].tid));
        cJSON_AddItemToArray(ids, id);
    }
    return ids;
}


ItemPointerData* pinecone_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids) {
    cJSON* request_body = (cJSON*) query;
    cJSON* ids = checkpoints_get_pinecone_ids(checkpoints, n_checkpoints);
    cJSON** responses = remote_query_with_fetch(pinecone_api_key, host, request_body, include_vector_ids, ids);
    ItemPointerData *results = palloc0(sizeof(ItemPointerData) * top_k); // 0 is an invalid ItemPointerData
    // find the best checkpoint
    // 1. extract the ctids from the fetch response
    cJSON* fetch_response = responses[1];
    int n_results;
    ItemPointerData* fetched_ctids = pinecone_extract_ctids_from_fetch_response(fetch_response, &n_results);
    // 2. return the best (first) checkpoint which is among the fetched
    for (int i = 0; i < n_checkpoints; i++) {
        RemoteCheckpoint checkpoint = checkpoints[i];
        for (int j = 0; j < n_results; j++) {
            if (ItemPointerEquals(&checkpoint.tid, &fetched_ctids[j])) {
                *best_checkpoint_return = checkpoint;
                break;
            }
        }
    }
    // extract the ctids from the response
    {
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
        *n_remote_tids = n;
        return results;
    }
}

// prepare_bulk_insert
PreparedBulkInsert pinecone_begin_prepare_bulk_insert(TupleDesc tupdesc) {
    cJSON* json_vectors = cJSON_CreateArray();
    return (PreparedBulkInsert) json_vectors;
}
void pinecone_append_prepare_bulk_insert(PreparedBulkInsert prepared_vectors, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid) {
    cJSON* json_vectors = (cJSON*) prepared_vectors;
    char* vector_id = pinecone_id_from_heap_tid(*ctid);
    cJSON* json_vector = tuple_get_remote_vector(tupdesc, values, nulls, vector_id);
    cJSON_AddItemToArray(json_vectors, json_vector);
}
void pinecone_end_prepare_bulk_insert(PreparedBulkInsert prepared_vectors) {
    return;
}
void pinecone_delete_prepared_bulk_insert(PreparedBulkInsert prepared_vectors) {
    cJSON_Delete((cJSON*) prepared_vectors);
}

bool pinecone_bulk_upsert(char* host, PreparedBulkInsert prepared_vectors, int remote_vectors_per_request, int n_prepared_tuples) {
    cJSON *json_vectors = (cJSON*) prepared_vectors;
    cJSON *response;
    response = remote_bulk_upsert(pinecone_api_key, host, json_vectors, remote_vectors_per_request);
    elog(DEBUG1, "Pinecone bulk upsert response: %s", cJSON_Print(response));
    if (response != NULL && cJSON_GetObjectItemCaseSensitive(response, "upsertedCount") != NULL) {
        return true;
    }
    return false;
}


RemoteIndexInterface pinecone_remote_index_interface = {
    // create index
    .check_credentials = pinecone_check_credentials,
    .create_host_from_spec = pinecone_create_host_from_spec,
    .wait_for_index = pinecone_wait_for_index,
    // upsert
    .begin_prepare_bulk_insert = pinecone_begin_prepare_bulk_insert,
    .append_prepare_bulk_insert = pinecone_append_prepare_bulk_insert,
    .end_prepare_bulk_insert = pinecone_end_prepare_bulk_insert,
    .delete_prepared_bulk_insert = pinecone_delete_prepared_bulk_insert,
    .bulk_upsert = pinecone_bulk_upsert,
    // query
    .prepare_query = pinecone_prepare_query,
    .query_with_fetch = pinecone_query_with_fetch,
};
