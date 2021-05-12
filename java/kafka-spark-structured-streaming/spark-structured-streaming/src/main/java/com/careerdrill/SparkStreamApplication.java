package com.careerdrill;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class SparkStreamApplication {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Kafka stream")
                .master("local[1]")
                .appName("LoadCSV")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topic1")
                .option("includeHeaders", "true")
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers");

         df.printSchema();

        StreamingQuery query = null;
        try {
            query = df
                    .writeStream()
                    .outputMode(OutputMode.Append())
                    .format("console")
                    .start();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
           e.printStackTrace();
        }

        System.out.println("Query status:" + query.status());

        System.out.println("<- start()");

    }
}
