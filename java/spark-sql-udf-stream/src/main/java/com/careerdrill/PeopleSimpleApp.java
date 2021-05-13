package com.careerdrill;

import com.careerdrill.util.SparkSessionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PeopleSimpleApp {
    public static void main(String[] args) {
        Dataset<Row> df = SparkSessionBuilder.getSparkSessionInstance()
                .read().json("data/people.json");

        df.show();

    }
}
