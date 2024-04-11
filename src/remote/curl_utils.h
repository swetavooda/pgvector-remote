#ifndef CURL_UTILS_H
#define CURL_UTILS_H


#include <curl/curl.h>

typedef struct {
    char message[256];
    char *request_body;
    char *data;
    size_t length;
    char method[10]; // GET, POST, DELETE, etc.
} ResponseData;

#endif // CURL_UTILS_H