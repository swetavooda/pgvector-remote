SET enable_seqscan = off;
SET client_min_messages = 'notice';
ALTER SYSTEM RESET pinecone.api_key;
SELECT pg_reload_conf();
CREATE TABLE t (val vector(3));
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
ALTER SYSTEM SET pinecone.api_key = 'fake-key';
SELECT pg_reload_conf();
CREATE INDEX i2 ON t USING pinecone (val);
DROP TABLE t;