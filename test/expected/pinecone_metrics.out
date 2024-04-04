SET enable_seqscan = off;
CREATE TABLE t (val vector(3));
CREATE INDEX i2 ON t USING pinecone (val vector_l2_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
CREATE INDEX i3 ON t USING pinecone (val vector_cosine_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
CREATE INDEX i1 ON t USING pinecone (val vector_ip_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
DROP TABLE t;
