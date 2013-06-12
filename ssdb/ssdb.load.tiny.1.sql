DROP TABLE IF EXISTS cycle;
DROP VIEW IF EXISTS cycle;
set hive.io.rcfile.record.buffer.size=64;


-- cycle
create external table cycle_t(tile INT, x INT, y INT, pix INT, var INT, valid INT, 
		sat INT, v0 INT, v1 INT, v2 INT, v3 INT, v4 INT, v5 INT, v6 INT)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
	STORED AS TEXTFILE LOCATION '/ssdb/cycle';

create table cycle(tile INT, x INT, y INT, pix INT, var INT, valid INT, 
		sat INT, v0 INT, v1 INT, v2 INT, v3 INT, v4 INT, v5 INT, v6 INT)
	CLUSTERED BY (tile, y, x) INTO 2 BUCKETS 
	ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
	STORED AS RCFILE LOCATION '/user/hive/warehouse/ssdb/cycle'; 
INSERT overwrite table cycle select * from cycle_t
	CLUSTER BY tile, y, x;
	
DROP TABLE cycle_t;


