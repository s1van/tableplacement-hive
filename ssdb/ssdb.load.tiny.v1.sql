DROP TABLE IF EXISTS cycle_s;
DROP TABLE IF EXISTS cycle;

set hive.io.rcfile.record.buffer.size=64;

CREATE EXTERNAL TABLE IF NOT EXISTS cycle_t (
	tile INT, x INT, y INT, pix INT, 
	var INT, valid INT, sat INT, v0 INT, 
	v1 INT, v2 INT, v3 INT, v4 INT, 
	v5 INT, v6 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/ssdb/cycle';


CREATE TABLE cycle_s (
	CG1 STRUCT<v1:INT>,
	CG2 STRUCT<tile:INT, x:INT, y:INT, pix:INT, 
	var:INT, valid:INT, sat:INT, v0:INT, 
	v2:INT, v3:INT, v4:INT, v5:INT, 
	v6:INT>,
	tile INT, x INT, y INT)
CLUSTERED BY (tile, x, y) INTO 2 BUCKETS
ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
STORED AS RCFILE LOCATION '/user/hive/warehouse/ssdb/cycle';


INSERT OVERWRITE TABLE cycle_s 
SELECT
	named_struct('v1', v1),
	named_struct('tile', tile, 'x', x, 'y', y, 
	'pix', pix, 'var', var, 'valid', valid, 
	'sat', sat, 'v0', v0, 'v2', v2, 
	'v3', v3, 'v4', v4, 'v5', v5, 
	'v6', v6),
	tile, x, y
FROM cycle_t
	CLUSTER BY tile, x, y;


DROP TABLE cycle_t;


CREATE VIEW IF NOT EXISTS cycle
AS SELECT
	CG1.v1 AS v1,
	CG2.tile AS tile, CG2.x AS x, CG2.y AS y, 
	CG2.pix AS pix, CG2.var AS var, CG2.valid AS valid, 
	CG2.sat AS sat, CG2.v0 AS v0, CG2.v2 AS v2, 
	CG2.v3 AS v3, CG2.v4 AS v4, CG2.v5 AS v5, 
	CG2.v6 AS v6
FROM cycle_s;


