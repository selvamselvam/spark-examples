package com.careerdrill;

import com.careerdrill.util.SparkSessionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleTempViewApp {
    public static void main(String[] args) {

        StructType schema = DataTypes.createStructType(
          new StructField[]{
                  DataTypes.createStructField("geo",DataTypes.StringType,true),
                  DataTypes.createStructField("yr1980",DataTypes.DoubleType,false)
          }
        );

        Dataset<Row> df= SparkSessionBuilder.getSparkSessionInstance()
                .read()
                .format("csv")
                .option("header","true")
                .schema(schema)
                .load("data/populationbycountry19802010millions.csv");

        df.createOrReplaceTempView("geodata");

        df.printSchema();

        Dataset<Row> smallCountries = SparkSessionBuilder.getSparkSessionInstance()
                .sql("SELECT * FROM geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");

        smallCountries.show(10,false);


        //write to parquet
        df.write().parquet("populationbycountry.parquet");


        //Read parquet data
        Dataset<Row> parquetFileDF = SparkSessionBuilder.getSparkSessionInstance().read().parquet("populationbycountry.parquet");

        //display
        parquetFileDF.show();

    }
}
