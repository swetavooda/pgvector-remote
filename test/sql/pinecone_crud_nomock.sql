-- SETUP
-- suppress output
\o /dev/null
-- logging level
SET client_min_messages = 'debug1';
-- flush each vector individually
-- disable flat scan to force use of the index
SET enable_seqscan = off;
-- Testing database is responsible for initializing the mock table with 
-- SELECT remote_create_mock_table(); 
-- CREATE TABLE
DROP TABLE IF EXISTS t;
CREATE TABLE t (id int, val vector(3));
\o

-- CREATE INDEX
-- create index
CREATE INDEX i2 ON t USING remote (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}', batch_size=1);
-- CREATE INDEX i2 ON t USING remote (val) WITH (host = 'fakehost');



-- INSERT INTO TABLE
-- insert into table
INSERT INTO t (id, val) VALUES (1, '[1,0,0]');
INSERT INTO t (id, val) VALUES (2, '[2,0,0]');
INSERT INTO t (id, val) VALUES (3, '[3,0,0]');
INSERT INTO t (id, val) VALUES (4, '[4,0,0]');

SELECT *, ctid FROM t;
-- SELECT FROM TABLE
-- select from table
SELECT id,val,val<->'[1,1,1]' as dist FROM t ORDER BY val <-> '[1, 1, 1]';

-- UPDATE A TUPLE AND SELECT FROM TABLE
-- this will trigger an insert, we'll reuse mock upsertedCount:1
UPDATE t SET val = '[1, 1, 1]' WHERE id = 1;
-- this will trigger a query and a fetch request, we'll reuse the mock responses
SELECT id,val,val<->'[1,1,1]' as dist FROM t ORDER BY val <-> '[1,1,1]'; 
\q


-- DELETE AND QUERY FROM TABLE
DELETE FROM t WHERE id = 1;
-- this will trigger a query and a fetch request, we'll reuse the mock responses
SELECT id,val,val<->'[1,1,1]' as dist FROM t ORDER BY val <-> '[1,1,1]';

DROP TABLE t;
