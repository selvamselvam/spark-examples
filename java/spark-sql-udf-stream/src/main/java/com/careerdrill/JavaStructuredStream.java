package com.careerdrill;

import com.careerdrill.util.SparkSessionBuilder;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class JavaStructuredStream {

    /*
    Download ncat from https://nmap.org/download.html

    Run the following command before start run the program
    C:\Program Files (x86)\Nmap>ncat -lk 9999
    1
    2
    3
    4
    5
    6
    7
    7
    8
    9
    ^C
    C:\Program Files (x86)\Nmap>
     */
    public static void main(String[] args) throws Exception {

        String host = "localhost";
        int port = 9999;

        SparkSession spark = SparkSessionBuilder.getSparkSessionInstance();

        // Create DataFrame representing the stream of input lines from connection to host:port
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .load();

        // Split the lines into words
        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
