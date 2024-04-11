
#include "pinecone/remote.h"
#include "pinecone/clients/pinecone/pinecone.h"
#include "pinecone/clients/milvus/milvus.h"

RemoteIndexInterface* remote_index_interfaces[NUM_PROVIDERS] = {0};

void initialize_remote_index_interfaces() {
    remote_index_interfaces[PINECONE_PROVIDER] = &pinecone_remote_index_interface;
    remote_index_interfaces[MILVUS_PROVIDER] = &milvus_remote_index_interface;
}