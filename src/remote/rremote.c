
#include "remote/remote.h"
#include "remote/clients/remote/remote.h"
#include "remote/clients/milvus/milvus.h"

RemoteIndexInterface* remote_index_interfaces[NUM_PROVIDERS] = {0};

void initialize_remote_index_interfaces() {
    remote_index_interfaces[REMOTE_PROVIDER] = &remote_remote_index_interface;
    remote_index_interfaces[MILVUS_PROVIDER] = &milvus_remote_index_interface;
}