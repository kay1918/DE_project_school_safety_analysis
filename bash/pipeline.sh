#!/bin/bash

# Change if these commands are not on the path.
DUCKDB=duckdb
SPARK=spark-submit

TRANSFORMER=../spark/spark-job.py
DATABASE=../duckdb/final.db
OUTPUT=../output
QUERIES=../duckdb/queries.sql

# Variables for DuckDB load.
LOADPATH="$OUTPUT/*.parquet"


rollback() {
	rm -fr $OUTPUT
	rm -f $DATABASE
}

message() {
	printf "%50s\n" | tr " " "-"
	printf "$1\n"
	printf "%50s\n" | tr " " "-"
}


check() {
	if [ $? -eq 0 ]; then
		message "$1"
	else
		message "$2"
		rollback
		exit 1
	fi
}


run_spark() {
	rm -fr $OUTPUT
	$SPARK \
		--master local[2] \
		--conf "spark.sql.shuffle.partitions=4" \
		--conf "spark.driver.memory=1g" \
        	--conf "spark.executor.memory=1g" \
		--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
		--conf "spark.sql.adaptive.enabled=true" \
		--name "Crime Rate and  School Analysis" \
		$TRANSFORMER \
			$OUTPUT
	check "Spark job successfully completed (E and T)." "Spark job FAILED."
}


run_duckdb() {
	sed "s|\$LOADPATH|${LOADPATH//\//\\/}|g" "$QUERIES" | $DUCKDB "$DATABASE"
	check "Data loaded into DuckDB successfully (L)." "Data load FAILED."
}



message "\n\n\n\nSTARTING DATA PROCESSING PIPELINE...\n\n\n\n"

run_spark
run_duckdb

check "PROCESS COMPLETE" "PIPELINE FAILED"
