
#include "remote/remote.h"
#include "remote/clients/pinecone/pinecone.h"
#include "remote/clients/milvus/milvus.h"
#include "utils/guc.h"

RemoteIndexInterface* remote_index_interfaces[NUM_PROVIDERS] = {0};

char* pinecone_api_key = NULL; // declared in pinecone.h
char* milvus_api_key = NULL; // declared in milvus.h
char* milvus_cluster_host = NULL; // declared in milvus.h

// declared in remote.h, used in remote.c
relopt_enum_elt_def provider_enum_options[] = {
    {"pinecone", PINECONE_PROVIDER},
    {"milvus", MILVUS_PROVIDER},
    {NULL, 0}
};

void initialize_remote_index_interfaces() {
    DefineCustomStringVariable("remote.pinecone_api_key", "Pinecone API key", "Pinecone API key",
                              &pinecone_api_key, "", 
                              PGC_USERSET, 
                              0, NULL, NULL, NULL); 
    DefineCustomStringVariable("remote.milvus_api_key", "milvus API key", "milvus API key",
                              &milvus_api_key, "", 
                              PGC_USERSET, 
                              0, NULL, NULL, NULL); 
    DefineCustomStringVariable("remote.milvus_cluster_host", "milvus cluster host", "milvus cluster host",
                              &milvus_cluster_host, "", 
                              PGC_USERSET, 
                              0, NULL, NULL, NULL); 
    remote_index_interfaces[PINECONE_PROVIDER] = &pinecone_remote_index_interface;
    remote_index_interfaces[MILVUS_PROVIDER] = &milvus_remote_index_interface;
}