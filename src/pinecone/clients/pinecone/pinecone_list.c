#include "pinecone/remote.h"
#include "pinecone/clients/pinecone/pinecone.h"


#include <stdio.h>
#include <stdlib.h>


int pn_int_to_int(int x) {
    return x + 1;
}

char* pn_int_to_string(int x) {
    char* str = (char*) malloc(100);
    sprintf(str, "%d", x);
    return str;
}

RemoteIndexInterface pinecone_remote_index_interface = {
    .int_to_int = pn_int_to_int,
    .int_to_string = pn_int_to_string
};
