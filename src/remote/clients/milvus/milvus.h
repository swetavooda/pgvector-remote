// pinecone/clients/pinecone/pinecone.h
#ifndef MILVUS_CLIENT_H
#define MILVUS_CLIENT_H

#include "pinecone/remote.h"

int mv_int_to_int(int x);
char* mv_int_to_string(int x);

extern RemoteIndexInterface milvus_remote_index_interface;

#endif // MILVUS_CLIENT_H