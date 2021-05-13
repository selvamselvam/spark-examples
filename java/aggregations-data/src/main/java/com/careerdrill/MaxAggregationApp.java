package com.careerdrill;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;


public class MaxAggregationApp {
    public static void main(String[] args) {
        SparkSession sparkSession =SparkSession
                .builder()
                .appName("AggregateMax")
                .master("local")
                .getOrCreate();

        Dataset<Row> ds = sparkSession.read()
                .format("csv")
                .option("headers","true")
                .load("data/StudentsPerformance.csv");


        Dataset<Row> df = ds.withColumnRenamed("_c0", "gender")
                .withColumnRenamed("_c2","parental level")
                .withColumnRenamed("_c5","MathScore")
                .drop(col("_c1"))
                .drop(col("_c3"))
                .drop(col("_c4"))
                .drop(col("_c6"))
                .drop(col("_c7"))
                .filter("CAST(MathScore AS DECIMAL) IS NOT NULL");

        Dataset<Row> libraryDf = df.orderBy(df.col("MathScore").desc());


        libraryDf.show();
        libraryDf.printSchema();



        Dataset<Row> max = df.select("*")
                .groupBy(col("gender"))
                .agg(max("MathScore"));
        max.show(5);

    }
}
