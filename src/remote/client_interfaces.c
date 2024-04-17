
#include "remote/remote.h"
#include "remote/clients/pinecone/pinecone.h"
#include "remote/clients/milvus/milvus.h"
#include "utils/guc.h"

RemoteIndexInterface* remote_index_interfaces[NUM_PROVIDERS] = {0};

// credentials
char* pinecone_api_key = NULL; 
char* milvus_api_key = NULL;
// network costs
int pinecone_network_cost = 0;
int milvus_network_cost = 0;
// unique pinecone GUC
int pinecone_vectors_per_request = 100; // declared in pinecone.h
// unique milvus GUC
char* milvus_cluster_host = NULL; // declared in milvus.h

// declared in remote.h, used in remote.c
relopt_enum_elt_def provider_enum_options[] = {
    {"pinecone", PINECONE_PROVIDER},
    {"milvus", MILVUS_PROVIDER},
    {NULL, 0}
};

void initialize_remote_index_interfaces() {
    // credentials
    DefineCustomStringVariable("remote.pinecone_api_key", "Pinecone API key", "Pinecone API key",
                              &pinecone_api_key, "", 
                              PGC_USERSET, 
                              0, NULL, NULL, NULL);
    DefineCustomStringVariable("remote.milvus_api_key", "milvus API key", "milvus API key",
                              &milvus_api_key, "", 
                              PGC_USERSET, 
                              0, NULL, NULL, NULL); 
    // network costs
    DefineCustomIntVariable("remote.pinecone_network_cost", "Pinecone network cost", "Pinecone network cost",
                            &pinecone_network_cost, 10000, 0, 1000000000, PGC_USERSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("remote.milvus_network_cost", "milvus network cost", "milvus network cost",
                            &milvus_network_cost, 10000, 0, 1000000000, PGC_USERSET, 0, NULL, NULL, NULL);
    // unique pinecone GUC
    DefineCustomIntVariable("remote.pinecone_vectors_per_request", "Number of vectors to send in a single request", "Number of vectors to send in a single request",
                            &pinecone_vectors_per_request, 100, 1, 10000, PGC_USERSET, 0, NULL, NULL, NULL);
    // unique milvus GUC
    DefineCustomStringVariable("remote.milvus_cluster_host", "milvus cluster host", "milvus cluster host",
                              &milvus_cluster_host, "", 
                              PGC_USERSET, 
                              0, NULL, NULL, NULL); 
    remote_index_interfaces[PINECONE_PROVIDER] = &pinecone_remote_index_interface;
    remote_index_interfaces[MILVUS_PROVIDER] = &milvus_remote_index_interface;
}