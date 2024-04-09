

-- SETUP
-- suppress output
\o /dev/null
-- logging level
SET client_min_messages = 'notice';
-- flush each vector individually
SET pinecone.vectors_per_request = 5;
SET pinecone.requests_per_batch = 5;
-- disable flat scan to force use of the index
SET enable_seqscan = off;
-- Testing database is responsible for initializing the mock table with 
-- SELECT pinecone_create_mock_table(); 
DELETE FROM pinecone_mock;
-- CREATE TABLE
DROP TABLE IF EXISTS cities;
CREATE TABLE cities (name text, coords vector(3));
\o

-- COPY FROM CSV
\copy cities(name, coords) FROM './test/data/cities_coordinates.csv' WITH CSV HEADER DELIMITER ',';

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
VALUES ('https://fakehost/describe_index_stats', 'GET', '{"namespaces":{},"dimension":3,"indexFullness":0,"totalVectorCount":51}');
-- mock upsert
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/upsert', 'POST', '{"upsertedCount":5}');
-- create index
CREATE INDEX i2 ON cities USING pinecone (coords) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');

-- SELECT
-- select from table
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', '{"results":[],"matches":[{"id":"000000000016","score":0.34670651,"values":[]},{"id":"000000000027","score":0.412868381,"values":[]},{"id":"00000000001c","score":0.434622884,"values":[]},{"id":"000000000019","score":0.493869543,"values":[]},{"id":"000000000030","score":0.57345736,"values":[]},{"id":"00000000002a","score":0.619416595,"values":[]},{"id":"00000000000f","score":0.68766582,"values":[]},{"id":"00000000000a","score":0.695464492,"values":[]}]}');
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/fetch', 'GET', '{"code": 3, "message": "No IDs provided for fetch query", "details": [] }');
SELECT name,coords<->'[0,0,1]' as dist FROM cities ORDER BY coords <-> '[0, 0, 1]' limit 5;

DROP TABLE cities;