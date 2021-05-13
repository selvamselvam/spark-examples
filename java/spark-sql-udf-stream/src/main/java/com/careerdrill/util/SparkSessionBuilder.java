package com.careerdrill.util;

import org.apache.spark.sql.SparkSession;

import java.util.Objects;

public class SparkSessionBuilder {
    private static SparkSession sparkSession= null;

    private SparkSessionBuilder(){

    }

    public static SparkSession getSparkSessionInstance(){

        if(Objects.isNull(sparkSession)){
            sparkSession = SparkSession
                    .builder()
                    .appName("SimpleSelect")
                    .master("local")
                    .getOrCreate();
        }

        return sparkSession;
    }



}
