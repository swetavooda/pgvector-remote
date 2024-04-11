#include "pinecone/remote.h"
#include "pinecone/clients/milvus/milvus.h"


#include <stdio.h>
#include <stdlib.h>


int mv_int_to_int(int x) {
    return x + 1;

}

char* mv_int_to_string(int x) {
    char* str = (char*) malloc(100);
    x += 2;
    sprintf(str, "%d", x);
    return str;
}

RemoteIndexInterface milvus_remote_index_interface = {
    .int_to_int = mv_int_to_int,
    .int_to_string = mv_int_to_string
};
