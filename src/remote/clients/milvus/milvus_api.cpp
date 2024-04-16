
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

std::shared_ptr<milvus::MilvusClient> get_client(const char* cluster_host) {
    static std::shared_ptr<milvus::MilvusClient> client = nullptr;
    if (!client) {
        client = milvus::MilvusClient::Create();
        milvus::ConnectParam connect_param(cluster_host, 443);
        connect_param.SetAuthorizations("db_07cf0782f144f07", "Vr9$E9HX)9(U]!BP");
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
extern "C" char* milvus_create_host_from_spec(int dimensions, VectorMetric metric, char* remote_index_name, char* spec) {
    // elog(NOTICE, "milvus_create_host_from_spec(%d, %d, %s, %s)", dimensions, metric, remote_index_name, spec);
    // auto client = milvus::MilvusClient::Create();
    // milvus::ConnectParam connect_param("in03-07cf0782f144f07.api.gcp-us-west1.zillizcloud.com", 443);
    // connect_param.SetAuthorizations("db_07cf0782f144f07", "Vr9$E9HX)9(U]!BP");
    // connect_param.EnableTls();
    // auto status = client->Connect(connect_param);
    // CheckStatus("Failed to connect milvus server:", status);
    // elog(NOTICE, "Connect to milvus server.");
    char* result = (char*) malloc(100);
    snprintf(result, 100, "milvus://in03-07cf0782f144f07.api.gcp-us-west1.zillizcloud.com:443");
    return result;
}

// UPSERT
extern "C" PreparedBulkInsert milvus_begin_prepare_bulk_insert(TupleDesc tupdesc) {
    MilvusPreparedBulkInsert* prepared_vectors = new MilvusPreparedBulkInsert();
    prepared_vectors->n_vectors = 0;
    prepared_vectors->n_metadata_attrs = tupdesc->natts - 1;
    prepared_vectors->id_field_name = std::string("id");
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
    std::string collection_name = "c0";
    std::string partition_name = "_default"; // Milvus' default partition name
    // TODO: factor out this client connection logic 
    auto client = get_client("in03-07cf0782f144f07.api.gcp-us-west1.zillizcloud.com");
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
    arguments->SetCollectionName("c0");
    arguments->AddPartitionName("_default");
    arguments->SetMetricType(milvus::MetricType::L2);
    arguments->SetTopK(1);
    arguments->SetGuaranteeTimestamp(milvus::GuaranteeStrongTs()); // ensure that the search is executed after the inserted data is persisted
    arguments->AddTargetVector(vector_field_name, std::move(q_vector));
    return (PreparedQuery) arguments;
}
extern "C" ItemPointerData* milvus_query_with_fetch(char* host, int top_k, PreparedQuery query, bool include_vector_ids, RemoteCheckpoint* checkpoints, int n_checkpoints, RemoteCheckpoint* best_checkpoint_return, int* n_remote_tids) {
    auto client = get_client("in03-07cf0782f144f07.api.gcp-us-west1.zillizcloud.com");
    milvus::SearchResults search_results{};
    milvus::SearchArguments* arguments = (milvus::SearchArguments*) query;
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
