-- SETUP
-- suppress output
\o /dev/null
delete from pinecone_mock;
-- logging level
SET client_min_messages = 'notice';
-- flush each vector individually
SET pinecone.vectors_per_request = 1;
SET pinecone.requests_per_batch = 1;
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
VALUES ('https://fakehost/describe_index_stats', 'GET', '{"namespaces":{},"dimension":3,"indexFullness":0,"totalVectorCount":0}');


INSERT INTO t (id, val) VALUES (2, '[0,0,0]');

-- create index after insering 0 vector - Throws an error
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');

-- Truncate the table to remove the values for creating an index successfully
TRUNCATE TABLE t;

-- create index
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');

INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/upsert',
'{ "vectors":	[{
			"id":	"000000000001",
			"values":	[100, 1, 1],
			"metadata":	{
			}
		}]
        }', 
        '{"upsertedCount":1}'
);

INSERT INTO t (id, val) VALUES (1, '[100,1,1]');
INSERT INTO t (id, val) VALUES (2, '[0,0,0]');

DROP TABLE t;