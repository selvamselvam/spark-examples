package com.careerdrill;


import com.careerdrill.util.SparkSessionBuilder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

public class UserDefinedFuncation {

    public static void main(String[] args) {

        UserDefinedFunction displayFunction = udf(
                () -> "Hello", DataTypes.StringType
        );

        displayFunction.asNondeterministic();

        SparkSession spark = SparkSessionBuilder.getSparkSessionInstance();
        spark.udf().register("display", displayFunction);
        spark.sql("SELECT display()").show();




        spark.udf().register("plusTen", new UDF1<Integer, Integer>() {
            @Override
            public Integer call(Integer x) {
                return x + 10;
            }
        }, DataTypes.IntegerType);
        spark.sql("SELECT plusTen(5)").show();


    }

}
