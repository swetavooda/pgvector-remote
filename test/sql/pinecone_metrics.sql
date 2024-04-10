delete from pinecone_mock;
SET client_min_messages = 'notice';
SET enable_seqscan = off;
-- flush each vector individually
SET pinecone.vectors_per_request = 1;
SET pinecone.requests_per_batch = 1;
-- CREATE TABLE
CREATE TABLE t (val vector(3));
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
-- create index
CREATE INDEX i2 ON t USING pinecone (val vector_l2_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
-- mock upsert
INSERT INTO pinecone_mock (url_prefix, method, response) VALUES ('https://fakehost/vectors/upsert', 'POST', '{"upsertedCount":1}');
INSERT INTO t (val) VALUES ('[1,0,0]');
INSERT INTO t (val) VALUES ('[1,0,1]');
INSERT INTO t (val) VALUES ('[1,1,0]');
EXPLAIN SELECT val,val<->'[1,1,1]' as dist FROM t ORDER BY val <-> '[1, 1, 1]';
EXPLAIN SELECT val,val<=>'[1,1,1]' as dist FROM t ORDER BY val <=> '[1, 1, 1]';
DROP INDEX i2;
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://api.pinecone.io/indexes', 'POST', $${
        "name": "invalid",
        "metric": "cosine",
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
CREATE INDEX i3 ON t USING pinecone (val vector_cosine_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
EXPLAIN SELECT val,val<=>'[1,1,1]' as dist FROM t ORDER BY val <=> '[1, 1, 1]';
EXPLAIN SELECT val,val<->'[1,1,1]' as dist FROM t ORDER BY val <-> '[1, 1, 1]';
DROP TABLE t;
