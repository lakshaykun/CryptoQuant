CREATE TABLE IF NOT EXISTS delta.`delta/bronze` (
	raw_json STRING,
	topic STRING,
	partition INT,
	offset LONG,
	kafka_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS delta.`delta/silver` (
	id STRING,
	timestamp STRING,
	source STRING,
	text STRING,
	engagement INT,
	symbol STRING,
	event_time TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS delta.`delta/gold` (
	window STRUCT<start:TIMESTAMP,end:TIMESTAMP>,
	symbol STRING,
	sentiment_index DOUBLE,
	total_engagement BIGINT,
	avg_confidence DOUBLE,
	message_count BIGINT
) USING DELTA;
