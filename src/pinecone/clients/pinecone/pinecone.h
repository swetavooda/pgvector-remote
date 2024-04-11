// pinecone/clients/pinecone/pinecone.h
#ifndef PINECONE_CLIENT_H
#define PINECONE_CLIENT_H

#include "pinecone/remote.h"

int pn_int_to_int(int x);
char* pn_int_to_string(int x);

extern RemoteIndexInterface pinecone_remote_index_interface;

#endif // PINECONE_CLIENT_H