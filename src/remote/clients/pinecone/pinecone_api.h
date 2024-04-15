#ifndef REMOTE_API_H
#define REMOTE_API_H

#include <curl/curl.h>
#include "src/remote/cJSON.h"
#include "src/remote/curl_utils.h"

#define bool _Bool

typedef CURL** CURLHandleList;

size_t write_callback(char *contents, size_t size, size_t nmemb, void *userdata);
struct curl_slist *create_common_headers(const char *api_key);
void set_curl_options(CURL *hnd, const char *api_key, const char *url, const char *method, ResponseData *response_data);
cJSON* generic_remote_request(const char *api_key, const char *url, const char *method, cJSON *body, bool expect_json_response);
cJSON* describe_index(const char *api_key, const char *index_name);
cJSON* remote_get_index_stats(const char *api_key, const char *index_host);
cJSON* list_indexes(const char *api_key);
cJSON* remote_delete_vectors(const char *api_key, const char *index_host, cJSON *ids);
cJSON* remote_delete_index(const char *api_key, const char *index_name);
cJSON* remote_delete_all(const char *api_key, const char *index_host);
cJSON* remote_list_vectors(const char *api_key, const char *index_host, int limit, char* pagination_token);
cJSON* remote_create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, cJSON *spec);
cJSON** remote_query_with_fetch(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter, bool with_fetch, cJSON* fetch_ids);
cJSON* remote_bulk_upsert(const char *api_key, const char *index_host, cJSON *vectors, int batch_size);
CURL* get_remote_query_handle(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter, ResponseData* response_data);
CURL* get_remote_upsert_handle(const char *api_key, const char *index_host, cJSON *vectors, ResponseData* response_data);
CURL* get_remote_fetch_handle(const char *api_key, const char *index_host, cJSON* ids, ResponseData* response_data);
cJSON* batch_vectors(cJSON *vectors, int batch_size);
#ifdef REMOTE_MOCK
void mock_netcall(const char *url, const char *method, cJSON *body, ResponseData *response_data, CURLcode *ret);
#endif

#endif // REMOTE_API_H