-- SETUP
-- suppress output
\o /dev/null
delete from pinecone_mock;
-- logging level
SET client_min_messages = 'notice';
-- flush each vector individually
SET pinecone.vectors_per_request = 1;
SET pinecone.requests_per_batch = 1;
SET pinecone.max_buffer_scan = 0;

-- disable flat scan to force use of the index
SET enable_seqscan = off;
-- CREATE TABLE
DROP TABLE IF EXISTS t;
CREATE TABLE t (id int, val vector(3));
\o

-- CREATE INDEX
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
VALUES ('https://fakehost/describe_index_stats', 'GET', '{"namespaces":{},"dimension":3,"indexFullness":0,"totalVectorCount":2}');

-- mock upsert
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/upsert', 'POST', '{"upsertedCount":1}');

-- mock query
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', $${
        "results":      [],
        "matches":      [{
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

-- create index
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');

-- insert vectors: throws warning while flushing zero-vector
INSERT INTO t (id, val) VALUES (1, '[100,1,1]');
INSERT INTO t (id, val) VALUES (2, '[0,0,0]');
INSERT INTO t (id, val) VALUES (3, '[10120,76,1]');

-- returns only id = 1 as it is flushed to pinecone )zero vector not flushed to pinecone)
SELECT * FROM t ORDER BY val <-> '[3,3,3]';

SELECT * FROM t;

DROP INDEX i2;

SELECT * FROM t ORDER BY val <-> '[3,3,3]';

DELETE FROM pinecone_mock
WHERE url_prefix = 'https://fakehost/query' AND method = 'POST';

INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', $${
        "results":      [],
        "matches":      [{
                        "id":   "000000000001",
                        "score":        2,
                        "values":       []
                },
                {
                        "id":   "000000000003",
                        "score":        2,
                        "values":       []
                }],
        "namespace":    "",
        "usage":        {
                "readUnits":    5
        }
}$$);

-- displays warning while flushing zero vector to pinecone
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');

SELECT * FROM t ORDER BY val <-> '[3,3,3]';
SELECT * FROM t;

DROP TABLE t;