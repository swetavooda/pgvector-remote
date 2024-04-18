
#include <stdlib.h>
#include <vector>

#include "milvus/MilvusClient.h"
#include "milvus/types/CollectionSchema.h"

extern "C" {
    #include "postgres.h"
    #include "fmgr.h"
    #include "milvus.h"
    #include "utils/rel.h" // index
    #include "utils/lsyscache.h" // elmbyval
}


// MilvusPreparedBulkInsert struct
typedef struct MilvusPreparedBulkInsert {
    milvus::CollectionSchema* schema;
    std::vector<milvus::FieldDataPtr> fields_data;
} MilvusPreparedBulkInsert;
// N.B. we have to include this after including postgres.h so that postgres.h will get C linkage
// #include "milvus.h"

void
CheckStatus(std::string&& prefix, const milvus::Status& status) {
    if (!status.IsOk()) {
        elog(ERROR, "%s %s", prefix.c_str(), status.Message().c_str());
    }
}
ItemPointerData milvus_id_get_heap_tid(int64_t id) {
    ItemPointerData heap_tid;
    int blockno = id >> 16;
    int offsetno = id & 0xFFFF;
    heap_tid.ip_blkid.bi_hi = blockno >> 16;
    heap_tid.ip_blkid.bi_lo = blockno & 0xFFFF;
    heap_tid.ip_posid = offsetno;
    return heap_tid;
}
std::shared_ptr<milvus::MilvusClient> get_client() {
    static std::shared_ptr<milvus::MilvusClient> client = nullptr;
    if (!client) {
        client = milvus::MilvusClient::Create();
        if (strcmp(milvus_cluster_host, "") == 0) {
            elog(ERROR, "remote.milvus_cluster_host is not set");
        }
        milvus::ConnectParam connect_param(milvus_cluster_host, 443);
        if (strcmp(milvus_api_key, "") == 0) {
            elog(ERROR, "remote.milvus_api_key is not set");
        }
        connect_param.SetApiKey(milvus_api_key);
        connect_param.EnableTls();
        auto status = client->Connect(connect_param);
        if (!status.IsOk()) {
            elog(WARNING, "failed to connect, retrying...");
            status = client->Connect(connect_param);
        }
        CheckStatus("Failed to connect milvus server:", status);
        elog(NOTICE, "Connect to milvus server.");
    }
    return client;
}
milvus::CollectionSchema* index_get_milvus_schema(Relation index) {
    // create collection_name as pgvr_{Oid}_{indexname}
    char* remote_index_name = (char*) palloc(REMOTE_HOST_MAX_LENGTH);
    int n = snprintf(remote_index_name, REMOTE_HOST_MAX_LENGTH, "pgvr_%u_%s", index->rd_id, NameStr(index->rd_rel->relname));
    if (n >= REMOTE_HOST_MAX_LENGTH) {
        elog(ERROR, "Remote index name is too long");
    }
    // validate that every character is alphanumeric or underscore
    for (int i = 0; i < n; i++) {
        if (!isalnum(remote_index_name[i]) && remote_index_name[i] != '_') {
            elog(ERROR, "Milvus index name must be alphanumeric or underscore, but got %s", remote_index_name);
        }
    }
    std::string collection_name = remote_index_name;
    elog(NOTICE, "Creating collection %s", collection_name.c_str());
    milvus::CollectionSchema* collection_schema = new milvus::CollectionSchema(collection_name);
    collection_schema->AddField({"tuple_id", milvus::DataType::INT64, "tuple identifier", true, false});  // id field, primary key, no autoid
    // add metadata fields
    for (int i = 0; i < index->rd_att->natts; i++) { 
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, i);
        milvus::FieldSchema arr_schema = {};
        std::map<std::string, std::string> type_params = {};
        switch (td->atttypid) {
            case BOOLOID:
                collection_schema->AddField(milvus::FieldSchema(NameStr(td->attname), milvus::DataType::BOOL, ""));
                break;
            case FLOAT8OID:
                collection_schema->AddField(milvus::FieldSchema(NameStr(td->attname), milvus::DataType::FLOAT, ""));
                break;
            case INT8ARRAYOID:
                arr_schema.SetName(NameStr(td->attname));
                arr_schema.SetDataType(milvus::DataType::ARRAY);
                type_params["max_capacity"] = "1024";
                arr_schema.SetElementType(milvus::DataType::INT64);
                arr_schema.SetTypeParams(type_params);
                collection_schema->AddField(arr_schema);
                break;
            default:
                if (i == 0) { // assume first col is vector
                    collection_schema->AddField(milvus::FieldSchema(NameStr(td->attname), milvus::DataType::FLOAT_VECTOR, "first column")
                        .WithDimension(td->atttypmod));
                    break;
                }
                elog(ERROR, "Unsupported data type. Milvus can only index metadata fields of type bool, float8, and float vector");
                break;
        }
    }
    return collection_schema;
}
// TODO: support other array datatypes
std::vector<int64_t> long_array_get_vector(Datum value) {
    std::vector<int64_t> ret;
    ArrayType *array = DatumGetArrayTypeP(value);
    int nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    Datum* elems;
    bool* nulls;
    int16 elmlen;
    bool elmbyval;
    char elmalign;
    Oid elmtype = ARR_ELEMTYPE(array);

    // get array element type info
    get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

    // deconstruct array
    deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);

    // copy array elements to json array
    for (int j = 0; j < nelems; j++) {
        if (!nulls[j]) {
            Datum elem = elems[j];
            ret.push_back(DatumGetInt64(elem));
        }
    }
    return ret;
}
// expressions
std::string milvus_expression_from_scankey(ScanKey key, Relation index) {
    // COL OP ARG
    // get the attribute number
    int attnum = key->sk_attno - 1;
    // get the attribute name
    FormData_pg_attribute* td = TupleDescAttr(index->rd_att, attnum);
    std::string field_name = NameStr(td->attname);
    // get the argument
    Datum arg = key->sk_argument;
    std::string arg_str;
    switch (td->atttypid) {
        case BOOLOID:
            arg_str = DatumGetBool(arg) ? "true" : "false";
            break;
        case FLOAT8OID:
            arg_str = std::to_string(DatumGetFloat8(arg));
            break;
        case INT8ARRAYOID:
            {
                std::vector<int64_t> arg_vector = long_array_get_vector(arg);
                arg_str = "[";
                for (size_t i = 0; i < arg_vector.size(); i++) {
                    arg_str += std::to_string(arg_vector[i]);
                    if (i < arg_vector.size() - 1) {
                        arg_str += ", ";
                    }
                }
                arg_str += "]";
            }
            break;
        default:
            elog(ERROR, "Unsupported data type. Milvus can only filter metadata fields of type bool, float8, and float vector");
            break;
    }
    bool is_array = td->atttypid == INT8ARRAYOID;
    if (is_array) {
        if (key->sk_strategy == REMOTE_STRATEGY_ARRAY_OVERLAP) {
            return "array_contains_any(" + field_name + ", " + arg_str + ")";
        } else if (key->sk_strategy == REMOTE_STRATEGY_ARRAY_CONTAINS) {
            return "array_contains_all(" + field_name + ", " + arg_str + ")";
        } else {
            elog(ERROR, "Unsupported array strategy");
        }
    }
    // get the operator
    const char* milvus_filter_operators[] = {"<", "<=", "=", ">=", ">", "!="};
    std::string op = milvus_filter_operators[key->sk_strategy - 1];
    return "( " + field_name + " " + op + " " + arg_str + " )";
}
std::string milvus_expression_from_scankeys(ScanKey keys, int nkeys, Relation index) {
    std::string expression = "";
    for (int i = 0; i < nkeys; i++) {
        expression += milvus_expression_from_scankey(&keys[i], index);
        if (i < nkeys - 1) {
            expression += " and ";
        }
    }
    return expression;
}

// CREATE
extern "C" void milvus_validate_host_schema(char* host, int dimensions, VectorMetric metric, Relation index) {
    // get the schema
    auto client = get_client();
    std::string collection_name = host;
    bool has_collection = false;
    auto status = client->HasCollection(collection_name, has_collection);
    CheckStatus("Failed to check collection:", status);
    if (!has_collection) {
        elog(ERROR, "Collection %s does not exist", collection_name.c_str());
    }
    // validate the schema
    // get the schema
    milvus::CollectionDesc collection_desc;
    status = client->DescribeCollection(collection_name, collection_desc);
    CheckStatus("Failed to describe collection:", status);
    const milvus::CollectionSchema& schema = collection_desc.Schema();
    const milvus::CollectionSchema& expected_schema = *index_get_milvus_schema(index);
    // verify that the schemas have the same number of fields
    if (schema.Fields().size() != expected_schema.Fields().size()) elog(ERROR, "The number of fields in the remote index does not match the number of fields in the local index. Expected %d, but got %d", (int) expected_schema.Fields().size(), (int) schema.Fields().size());
    // for each field in the schema, verify that the field name and data type match
    for (size_t i = 0; i < schema.Fields().size(); i++) {
        const milvus::FieldSchema& remote_field = schema.Fields()[i];
        const milvus::FieldSchema& expected_field = expected_schema.Fields()[i];
        // verify that the primary key flag is the same
        if (remote_field.Name() != expected_field.Name()) elog(ERROR, "The column names do not match. Remote: %s, expected: %s", remote_field.Name().c_str(), expected_field.Name().c_str());
        if (remote_field.IsPrimaryKey() != expected_field.IsPrimaryKey()) elog(ERROR, "The primary key flag of field %s does not match. Remote: %d, expected: %d", remote_field.Name().c_str(), remote_field.IsPrimaryKey(), expected_field.IsPrimaryKey());
        if (remote_field.FieldDataType() != expected_field.FieldDataType()) elog(ERROR, "The data type of field %s does not match. Remote: %d, expected: %d", remote_field.Name().c_str(), (int) remote_field.FieldDataType(), (int) expected_field.FieldDataType());
        if (remote_field.Dimension() != expected_field.Dimension()) elog(ERROR, "The dimension of field %s does not match. Remote: %d, expected: %d", remote_field.Name().c_str(), (int) remote_field.Dimension(), (int) expected_field.Dimension());
    }
}
extern "C" int milvus_count_live(char* host) {
    auto client = get_client();
    std::string collection_name = host;
    bool has_collection = false;
    auto status = client->HasCollection(collection_name, has_collection);
    CheckStatus("Failed to check collection:", status);
    if (!has_collection) {
        elog(ERROR, "Collection %s does not exist", collection_name.c_str());
    }
    status = client->LoadCollection(collection_name);
    CheckStatus("Failed to load collection:", status);
    // count vecs in partition
    milvus::PartitionStat part_stat;
    std::string partition_name = "_default";
    status = client->GetPartitionStatistics(collection_name, partition_name, part_stat);
    CheckStatus("Failed to get partition statistics:", status);
    return part_stat.RowCount();
}
extern "C" char* milvus_create_host_from_spec(int dimensions, VectorMetric metric, char* spec, Relation index) {
    milvus::CollectionSchema* collection_schema = index_get_milvus_schema(index);
    // VECTOR INDEX
    milvus::MetricType milvus_metric = metric == EUCLIDEAN_METRIC ? milvus::MetricType::L2 :
                                       metric == INNER_PRODUCT_METRIC ? milvus::MetricType::IP :
                                        milvus::MetricType::INVALID;
    if (milvus_metric == milvus::MetricType::INVALID) elog(ERROR, "Unsupported metric type. Milvus only supports euclidean and inner product");
    milvus::IndexType milvus_index_type = milvus::IndexType::FLAT; // TODO, allow the user to specify other index types
    // get the name of the first field in the index
    std::string vector_field_name = collection_schema->Fields()[1].Name();
    milvus::IndexDesc index_desc(vector_field_name, "pgvr_primary_vector_index", milvus_index_type, milvus_metric, 0);
    // SEND TO MILVUS
    auto client = get_client();
    auto status = client->CreateCollection(*collection_schema);
    CheckStatus("Failed to create collection:", status);
    status = client->CreateIndex(collection_schema->Name(), index_desc);
    CheckStatus("Failed to create index:", status);
    // return the collection schema's name
    return (char*) collection_schema->Name().c_str();
}

// UPSERT
extern "C" PreparedBulkInsert milvus_begin_prepare_bulk_insert(Relation index) {
    MilvusPreparedBulkInsert* prepared_vectors = new MilvusPreparedBulkInsert();
    prepared_vectors->schema = index_get_milvus_schema(index);
    prepared_vectors->fields_data = std::vector<milvus::FieldDataPtr>();
    // for each field in schema, add a field data to fields_data
    for (const milvus::FieldSchema& field : prepared_vectors->schema->Fields()) {
        if (field.Name() == "tuple_id") {
            prepared_vectors->fields_data.push_back(std::make_shared<milvus::Int64FieldData>(field.Name()));
        } else if (field.FieldDataType() == milvus::DataType::FLOAT_VECTOR) {
            prepared_vectors->fields_data.push_back(std::make_shared<milvus::FloatVecFieldData>(field.Name()));
        } else if (field.FieldDataType() == milvus::DataType::BOOL) {
            prepared_vectors->fields_data.push_back(std::make_shared<milvus::BoolFieldData>(field.Name()));
        } else if (field.FieldDataType() == milvus::DataType::FLOAT) {
            prepared_vectors->fields_data.push_back(std::make_shared<milvus::FloatFieldData>(field.Name()));
        } else if (field.FieldDataType() == milvus::DataType::ARRAY) {
            prepared_vectors->fields_data.push_back(std::make_shared<milvus::ArrayFieldData>(field.Name()));
        } else {
            elog(ERROR, "Unsupported data type in schema");
        }
    }
    return (PreparedBulkInsert) prepared_vectors;
}
extern "C" void milvus_append_prepare_bulk_insert(PreparedBulkInsert prepared_vectors_, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid) {
    MilvusPreparedBulkInsert* prepared_vectors = (MilvusPreparedBulkInsert*) prepared_vectors_;
    int64_t id = ItemPointerGetBlockNumber(ctid) << 16 | ItemPointerGetOffsetNumber(ctid);
    ((milvus::Int64FieldData*) prepared_vectors->fields_data[0].get())->Add(id);
    for (int i = 0; i < tupdesc->natts; i++) {
        if (nulls[i]) {
            elog(ERROR, "Null values are not supported");
        }
        FormData_pg_attribute* td = TupleDescAttr(tupdesc, i);
        switch (td->atttypid) {
            case BOOLOID:
                ((milvus::BoolFieldData*) prepared_vectors->fields_data[i + 1].get())->Add(DatumGetBool(values[i]));
                break;
            case FLOAT8OID:
                ((milvus::FloatFieldData*) prepared_vectors->fields_data[i + 1].get())->Add(DatumGetFloat8(values[i]));
                break;
            case INT8ARRAYOID:
                elog(NOTICE, "Adding array field");
                ((milvus::ArrayFieldData*) prepared_vectors->fields_data[i + 1].get())->Add(long_array_get_vector(values[i]));
                break;
            default:
                if (i == 0) { // assume first col is vector
                    Vector* pg_vector = DatumGetVector(values[i]);
                    std::vector<float> vector(pg_vector->x, pg_vector->x + pg_vector->dim);
                    ((milvus::FloatVecFieldData*) prepared_vectors->fields_data[i + 1].get())->Add(vector);
                    break;
                }
                elog(WARNING, "Unsupported data type. This is an internal error. We should have already validated the schema");
                break;
        }
    }
    return;
}
extern "C" bool milvus_bulk_upsert(char* host, PreparedBulkInsert prepared_vectors_,  int n_vectors) {
    MilvusPreparedBulkInsert* prepared_vectors = (MilvusPreparedBulkInsert*) prepared_vectors_;
    std::string collection_name = host;
    std::string partition_name = "_default"; // Milvus' default partition name
    auto client = get_client();
    // tell server prepare to load collection
    auto status = client->LoadCollection(collection_name);
    CheckStatus("Failed to load collection:", status);
    // insert
    milvus::DmlResults dml_results;
    status = client->Insert(collection_name, partition_name, prepared_vectors->fields_data, dml_results);
    CheckStatus("Failed to insert:", status);
    elog(NOTICE, "Successfully insert %ld rows.", dml_results.IdArray().IntIDArray().size());
    return true;
}

// QUERY
extern "C" PreparedQuery milvus_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k) {
    std::vector<float> q_vector(vec->x, vec->x + vec->dim);
    std::string vector_field_name = NameStr(TupleDescAttr(index->rd_att, 0)->attname);
    // construct search arguments
    milvus::SearchArguments* arguments = new milvus::SearchArguments();
    arguments->AddPartitionName("_default");
    arguments->SetMetricType(milvus::MetricType::L2);
    arguments->SetTopK(top_k);
    std::string expression = milvus_expression_from_scankeys(keys, nkeys, index);
    elog(NOTICE, "Query expression: %s", expression.c_str());
    arguments->SetExpression(expression);
    arguments->SetGuaranteeTimestamp(milvus::GuaranteeStrongTs()); // ensure that the search is executed after the inserted data is persisted
    arguments->AddTargetVector(vector_field_name, std::move(q_vector));
    return (PreparedQuery) arguments;
}
extern "C" ItemPointerData* milvus_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids) {
    auto client = get_client(); // "in03-07cf0782f144f07.api.gcp-us-west1.zillizcloud.com"
    milvus::SearchResults search_results{};
    milvus::SearchArguments* arguments = (milvus::SearchArguments*) query;
    arguments->SetCollectionName(host);
    auto status = client->Search(*arguments, search_results);
    CheckStatus("Failed to search:", status);
    milvus::SingleResult result = search_results.Results()[0]; // we only have one query vector
    std::vector<int64_t> ids = result.Ids().IntIDArray();
    *n_remote_tids = ids.size();
    ItemPointerData* remote_tids = (ItemPointerData*) palloc(sizeof(ItemPointerData) * ids.size());
    for (size_t i = 0; i < ids.size(); i++) {
        remote_tids[i] = milvus_id_get_heap_tid(ids[i]); // convert int64 id to ItemPointerData (tuple identifiers)
    }
    return remote_tids;
}
