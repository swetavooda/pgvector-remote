-- SETUP
-- suppress output
\o /dev/null
--starting
delete from pinecone_mock;
-- logging level
SET client_min_messages = 'notice';
-- disable flat scan to force use of the index
SET enable_seqscan = true;
-- CREATE TABLE
DROP TABLE IF EXISTS t;
CREATE TABLE t (id int, val vector(2));
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
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://api.pinecone.io/indexes', 'POST', $${
        "name": "invalid",
        "metric": "inner",
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
VALUES ('https://fakehost/describe_index_stats', 'GET', '{"namespaces":{},"dimension":2,"indexFullness":0,"totalVectorCount":0}');

-- mock fetch
INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/fetch', 'GET', $${
        "code": 3,
        "message":      "No IDs provided for fetch query",
        "details":      []
}$$);

INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/vectors/upsert',
'{ "vectors":	[{
			"id":	"000000000001",
			"values":	[1, 1],
			"metadata":	{
			}
		}, {
			"id":	"000000000002",
			"values":	[3, 0],
			"metadata":	{
			}
		}, {
			"id":	"000000000003",
			"values":	[4, 1],
			"metadata":	{
			}
		}]
}');

INSERT INTO t (id, val) VALUES (1, '[1,1]');
INSERT INTO t (id, val) VALUES (2, '[3,0]');
INSERT INTO t (id, val) VALUES (3, '[4,1]');

CREATE INDEX i1 ON t USING pinecone (val vector_l2_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
CREATE INDEX i2 ON t USING pinecone (val vector_cosine_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
CREATE INDEX i3 ON t USING pinecone (val vector_ip_ops) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');

INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', $${
        "results":	[],
	"matches":	[{
			"id":	"000000000001",
			"score":	1,
			"values":	[]
		}, {
			"id":	"000000000002",
			"score":	4,
			"values":	[]
		}, {
			"id":	"000000000003",
			"score":	10,
			"values":	[]
		}],
	"namespace":	"",
	"usage":	{
		"readUnits":	5
	}
}

$$);

SELECT id,val,val<->'[1,0]' as dist FROM t ORDER BY val <-> '[1,0]'; 

DELETE FROM pinecone_mock
WHERE url_prefix = 'https://fakehost/query' AND method = 'POST';

INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', $${
        "results":	[],
	"matches":	[{
			"id":	"000000000003",
			"score":	4,
			"values":	[]
		}, {
			"id":	"000000000002",
			"score":	3,
			"values":	[]
		}, {
			"id":	"000000000001",
			"score":	1,
			"values":	[]
		}],
	"namespace":	"",
	"usage":	{
		"readUnits":	5
	}
}

$$);

SELECT id,val,val<=>'[1,0]' as dist FROM t ORDER BY val <=> '[1,0]'; 

DELETE FROM pinecone_mock
WHERE url_prefix = 'https://fakehost/query' AND method = 'POST';


SELECT id,val,val<#>'[1,0]' as dist FROM t ORDER BY val <#> '[1,0]'; 

INSERT INTO pinecone_mock (url_prefix, method, response)
VALUES ('https://fakehost/query', 'POST', $${
        "results":	[],
	"matches":	[{
			"id":	"000000000003",
			"score":	4,
			"values":	[]
		}, {
			"id":	"000000000002",
			"score":	3,
			"values":	[]
		}, {
			"id":	"000000000001",
			"score":	1,
			"values":	[]
		}],
	"namespace":	"",
	"usage":	{
		"readUnits":	5
	}
}

$$);

DROP TABLE t;