#include "postgres.h"
#include "src/remote/cJSON.h"
#include "storage/itemptr.h"    
#include "access/tupdesc.h"
#include "catalog/pg_type_d.h" // TEXTARRAYOID
#include "src/vector.h" // Vector
#include "src/remote/clients/pinecone/pinecone.h"

#include "utils/builtins.h" // text_to_cstring

ItemPointerData pinecone_id_get_heap_tid(char *id)
{
    ItemPointerData heap_tid;
    int n_matched;
    n_matched = sscanf(id, "%04hx%04hx%04hx", &heap_tid.ip_blkid.bi_hi, &heap_tid.ip_blkid.bi_lo, &heap_tid.ip_posid);
    if (n_matched != 3) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector id"),
                        errhint("Vector id should be a 12-character hexadecimal string")));
    }
    return heap_tid;
}

char* pinecone_id_from_heap_tid(ItemPointerData heap_tid)
{
    char* id = palloc(12 + 1);
    snprintf(id, 12 + 1, "%04hx%04hx%04hx", heap_tid.ip_blkid.bi_hi, heap_tid.ip_blkid.bi_lo, heap_tid.ip_posid);
    return id;
}

cJSON* tuple_get_remote_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id)
{
    cJSON *json_vector = cJSON_CreateObject();
    cJSON *metadata = cJSON_CreateObject();
    Vector *vector;
    cJSON *json_values;
    if(isnull[0]){
        elog(DEBUG1, "Cannot insert NULL vectors to pinecone.");
        return NULL;
    }
    vector = DatumGetVector(values[0]);
    if(vector_eq_zero_internal(vector)) {
            elog(DEBUG1, "Cannot insert zero vectors to pinecone.");
        return NULL;
    }
    json_values = cJSON_CreateFloatArray(vector->x, vector->dim);
    // prepare metadata
    for (int i = 1; i < tup_desc->natts; i++) // skip the first column which is the vector
    {
        // todo: we should validate that all the columns have the desired types when the index is built
        FormData_pg_attribute* td = TupleDescAttr(tup_desc, i);
        switch (td->atttypid) {
            case BOOLOID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateBool(DatumGetBool(values[i])));
                break;
            case FLOAT8OID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateNumber(DatumGetFloat8(values[i])));
                break;
            case INT4OID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateNumber(DatumGetInt32(values[i])));
                break;
            case TEXTOID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateString(text_to_cstring(DatumGetTextP(values[i]))));
                break;
            case TEXTARRAYOID:
                {
                    cJSON* json_array = text_array_get_json(values[i]);
                    cJSON_AddItemToObject(metadata, NameStr(td->attname), json_array);
                }
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Invalid column type when decoding tuple."),
                                errhint("Remote index only supports boolean, float8, text, and textarray columns")));
        }
    }
    // add to vector object
    cJSON_AddItemToObject(json_vector, "id", cJSON_CreateString(vector_id));
    cJSON_AddItemToObject(json_vector, "values", json_values);
    cJSON_AddItemToObject(json_vector, "metadata", metadata);
    return json_vector;
}


