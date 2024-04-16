
#include <stdio.h>
#include <stdlib.h>

// cpp vector
#include <vector>

#include "milvus/MilvusClient.h"
#include "milvus/types/CollectionSchema.h"

extern "C" {
    #include "postgres.h"
    #include "fmgr.h"
    #include "milvus.h"
    // index
    #include "utils/rel.h"
}


// MilvusPreparedBulkInsert struct
typedef struct MilvusPreparedBulkInsert {
    // ids
    std::string id_field_name;
    std::vector<int64_t> insert_ids;
    // vectors
    int n_vectors;
    std::string vector_field_name;
    std::vector<std::vector<float>> vectors;
    // metadata
    int n_metadata_attrs;
    std::vector<std::string> field_names;
    std::vector<std::string> field_types;
    void** field_data;
} MilvusPreparedBulkInsert;
// N.B. we have to include this after including postgres.h so that postgres.h will get C linkage
// #include "milvus.h"

void
CheckStatus(std::string&& prefix, const milvus::Status& status) {
    if (!status.IsOk()) {
        // std::cout << prefix << " " << status.Message() << std::endl;
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
    // verify that the schema has the correct number of fields (it has one extra field for the id field)
    if ((int) schema.Fields().size() - 1 != index->rd_att->natts) elog(ERROR, "The number of non-primary fields in the remote index does not match the number of fields in the local index. Expected %d, but got %d", index->rd_att->natts, (int) schema.Fields().size() - 1);
    // verify that the first field is (1) named tuple_id (2) is an int64 (3) is the primary key
    const milvus::FieldSchema& id_field = schema.Fields()[0]; 
    if (id_field.Name() != "tuple_id") elog(ERROR, "The first field in the remote index must be named tuple_id, but got %s", id_field.Name().c_str());
    if (!id_field.IsPrimaryKey()) elog(ERROR, "The tuple_id field in the remote index must be the primary key");
    if (id_field.FieldDataType() != milvus::DataType::INT64) elog(ERROR, "The tuple_id field in the remote index must be of type INT64");
    // verify that the second field is (1) named vector (2) is a float vector with (3) the correct dimension
    const milvus::FieldSchema& vector_field = schema.Fields()[1]; 
    if (vector_field.Name() != "vector") elog(ERROR, "The second field in the remote index must be named vector, but got %s", vector_field.Name().c_str());
    if (vector_field.FieldDataType() != milvus::DataType::FLOAT_VECTOR) elog(ERROR, "The vector field in the remote index must be of type FLOAT_VECTOR");
    if ((int) vector_field.Dimension() != dimensions) elog(ERROR, "The vector field in the remote index must have dimension %d, but got %d", dimensions, (int) vector_field.Dimension());
    // verify that the rest of the fields are the same as the local index
    for (size_t i = 2; i < schema.Fields().size(); i++) { // skip the first two fields which we've already checked
        const milvus::FieldSchema& remote_field = schema.Fields()[i];
        int local_field_index = i - 1; // the remote index has one extra field (tuple_id)
        FormData_pg_attribute* local_td = TupleDescAttr(index->rd_att, local_field_index);
        // verify that the field name matches
        if (remote_field.Name() != NameStr(local_td->attname)) elog(ERROR, "The column names do not match. Remote: %s, Local: %s", remote_field.Name().c_str(), NameStr(local_td->attname));
        // verify that the field data type matches
        milvus::DataType milvus_field_datatype = remote_field.FieldDataType();
        bool matches;
        switch (milvus_field_datatype) {
            case milvus::DataType::BOOL:
                matches = local_td->atttypid == BOOLOID;
                break;
            case milvus::DataType::FLOAT:
                matches = local_td->atttypid == FLOAT8OID;
                break;
            default:
                elog(WARNING, "Unsupported remote data type");
                matches = false;
                break;
        }
        if (!matches) elog(ERROR, "The data type of field %s does not match. Remote: %d, Local: %d", remote_field.Name().c_str(), (int) milvus_field_datatype, local_td->atttypid);
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
    // COLLECTION SCHEMA
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
    // define the schema
    milvus::CollectionSchema collection_schema(collection_name); // TODO: each provider specifies its name its own way
    collection_schema.AddField({"tuple_id", milvus::DataType::INT64, "tuple identifier", true, false});  // id field, primary key, no autoid
    collection_schema.AddField(milvus::FieldSchema("vector", milvus::DataType::FLOAT_VECTOR, "first column").WithDimension(dimensions));
    // add metadata fields
    for (int i = 1; i < index->rd_att->natts; i++) { // start at 1 to exclude the vector field
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, i);
        switch (td->atttypid) {
            case BOOLOID:
                collection_schema.AddField(milvus::FieldSchema(NameStr(td->attname), milvus::DataType::BOOL, ""));
                break;
            case FLOAT8OID:
                collection_schema.AddField(milvus::FieldSchema(NameStr(td->attname), milvus::DataType::FLOAT, ""));
                break;
            default:
                elog(WARNING, "Unsupported data type");
                break;
        }
    }
    // VECTOR INDEX
    milvus::MetricType milvus_metric;
    switch (metric) {
        case EUCLIDEAN_METRIC:
            milvus_metric = milvus::MetricType::L2;
            break;
        case INNER_PRODUCT_METRIC:
            milvus_metric = milvus::MetricType::IP;
            break;
        default:
            elog(ERROR, "Currently the Milvus-cpp-sdk only supports euclidean and inner product metrics.");
            break;
    }
    milvus::IndexType milvus_index_type = milvus::IndexType::FLAT; // TODO, allow the user to specify other index types
    milvus::IndexDesc index_desc("vector", "pgvr_primary_vector_index", milvus_index_type, milvus_metric, 0);
    // SEND TO MILVUS
    auto client = get_client();
    auto status = client->CreateCollection(collection_schema);
    CheckStatus("Failed to create collection:", status);
    status = client->CreateIndex(collection_name, index_desc);
    CheckStatus("Failed to create index:", status);
    return remote_index_name;
}

// UPSERT
extern "C" PreparedBulkInsert milvus_begin_prepare_bulk_insert(TupleDesc tupdesc) {
    MilvusPreparedBulkInsert* prepared_vectors = new MilvusPreparedBulkInsert();
    prepared_vectors->n_vectors = 0;
    prepared_vectors->n_metadata_attrs = tupdesc->natts - 1;
    prepared_vectors->id_field_name = std::string("tuple_id");
    prepared_vectors->vector_field_name = std::string("vector");
    for (int i = 1; i < tupdesc->natts; i++) {
        FormData_pg_attribute* td = TupleDescAttr(tupdesc, i);
        prepared_vectors->field_names.push_back(NameStr(td->attname));
        switch (td->atttypid) {
            case BOOLOID:
                prepared_vectors->field_types.push_back("bool");
                break;
            case FLOAT8OID:
                prepared_vectors->field_types.push_back("float");
                break;
            default:
                elog(WARNING, "Unsupported data type");
                break;
        }
    }
    prepared_vectors->vectors = std::vector<std::vector<float>>();
    return (PreparedBulkInsert) prepared_vectors;
}
extern "C" void milvus_append_prepare_bulk_insert(PreparedBulkInsert prepared_vectors_, TupleDesc tupdesc, Datum* values, bool* nulls, ItemPointer ctid) {
    MilvusPreparedBulkInsert* prepared_vectors = (MilvusPreparedBulkInsert*) prepared_vectors_;
    int64_t id = ItemPointerGetBlockNumber(ctid) << 16 | ItemPointerGetOffsetNumber(ctid);
    prepared_vectors->insert_ids.push_back(id);
    Vector* pg_vector = DatumGetVector(values[0]);
    std::vector<float> vector(pg_vector->x, pg_vector->x + pg_vector->dim);
    prepared_vectors->vectors.push_back(vector);
    return;
}
extern "C" bool milvus_bulk_upsert(char* host, PreparedBulkInsert prepared_vectors_, int vectors_per_request, int n_vectors) {
    MilvusPreparedBulkInsert* prepared_vectors = (MilvusPreparedBulkInsert*) prepared_vectors_;
    std::string collection_name = host;
    std::string partition_name = "_default"; // Milvus' default partition name
    // TODO: factor out this client connection logic 
    auto client = get_client();
    // tell server prepare to load collection
    auto status = client->LoadCollection(collection_name);
    CheckStatus("Failed to load collection:", status);
    // insert
    std::vector<milvus::FieldDataPtr> fields_data{
        std::make_shared<milvus::Int64FieldData>(prepared_vectors->id_field_name, prepared_vectors->insert_ids),
        // std::make_shared<milvus::Int8FieldData>(field_age_name, insert_ages),
        std::make_shared<milvus::FloatVecFieldData>(prepared_vectors->vector_field_name, prepared_vectors->vectors)
    };
    milvus::DmlResults dml_results;
    status = client->Insert(collection_name, partition_name, fields_data, dml_results);
    CheckStatus("Failed to insert:", status);
    elog(NOTICE, "Successfully insert %ld rows.", dml_results.IdArray().IntIDArray().size());
    return true;
}

// QUERY
extern "C" PreparedQuery milvus_prepare_query(Relation index, ScanKey keys, int nkeys, Vector* vec, int top_k) {
    std::vector<float> q_vector(vec->x, vec->x + vec->dim);
    std::string vector_field_name = "vector";
    // construct search arguments
    milvus::SearchArguments* arguments = new milvus::SearchArguments();
    arguments->AddPartitionName("_default");
    arguments->SetMetricType(milvus::MetricType::L2);
    arguments->SetTopK(top_k);
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
