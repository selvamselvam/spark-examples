import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;


public class DataFrameCollectStore {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("CollectStore")
                .master("local[1]")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());


        Dataset<Row> dataset= sparkSession.read()
                .format("csv")
                .option("header","true")
                .option("encoding", "UTF-8")
                .load("data/authors.csv");

        dataset.show(10);

        dataset = dataset.withColumn("Name",
                concat(dataset.col("lname"),lit(" ,"),dataset.col("fname")));
        dataset.show(10);

        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);



        MongoSpark.save(dataset, writeConfig);

        jsc.close();


    }
}
