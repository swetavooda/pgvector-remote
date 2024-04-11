// define a struct of functions where the first function maps an int to an int
// and the second function maps an int to a string

// remote.h
#ifndef REMOTE_H
#define REMOTE_H

typedef enum Provider
{
    PINECONE_PROVIDER,
    MILVUS_PROVIDER,
    NUM_PROVIDERS
} Provider;

typedef struct
{
    int (*int_to_int)(int);
    char* (*int_to_string)(int);
} RemoteIndexInterface;

extern RemoteIndexInterface* remote_index_interfaces[NUM_PROVIDERS];


void initialize_remote_index_interfaces(void);

#endif // REMOTE_H



