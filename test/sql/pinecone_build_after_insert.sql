delete from pinecone_mock;
SET client_min_messages = 'notice';
-- flush each vector individually
SET pinecone.vectors_per_request = 1;
SET pinecone.requests_per_batch = 1;
-- disable flat scan to force use of the index
SET enable_seqscan = off;
-- CREATE TABLE
CREATE TABLE t (id int, val vector(3));
INSERT INTO t (id, val) VALUES (1, '[1,0,0]');
INSERT INTO t (id, val) VALUES (2, '[1,0,1]');
-- mock create index
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://api.pinecone.io/indexes', 'POST', $${
        "name": "invalid",
        "metric": "euclidean",
        "dimension": 3,
        "status": {
                "ready": true,
                "state": "Ready"
        },
        "host": "fakehost",
        "spec": {
                "serverless": {
                        "cloud": "aws",
                        "region": "us-west-2"
                }
        }
}$$);
-- mock describe index stats
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/describe_index_stats', 'GET', '{"namespaces":{},"dimension":3,"indexFullness":0,"totalVectorCount":0}');
-- mock upsert
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/upsert', 'POST', '{"upsertedCount":1}');
-- create index
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
-- mock query
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', $${
        "results":      [],
        "matches":      [{
                        "id":   "000000000002",
                        "score":        1,
                        "values":       []
                }, {
                        "id":   "000000000001",
                        "score":        2,
                        "values":       []
                }],
        "namespace":    "",
        "usage":        {
                "readUnits":    5
        }
}$$);
-- mock fetch
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/fetch', 'GET', $${
        "code": 3,
        "message":      "No IDs provided for fetch query",
        "details":      []
}$$);
SELECT id,val,val<->'[1,1,1]' as dist FROM t ORDER BY val <-> '[1,1,1]'; 
DROP TABLE t;