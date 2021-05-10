import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.split;

public class AnalysisDataSetSample {
    public static final String JSON_FILE = "data/Restaurants_in_Durham_County_NC.json";
    public static final String CSV_FILE = "data/Restaurants_in_Wake_County_NC.csv";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .appName("Frame")
                .getOrCreate();

        analysisCSV(sparkSession);
        analysisJSON(sparkSession);



    }

    private static void analysisCSV(SparkSession sparkSession){
        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load(CSV_FILE);

        System.out.println("*** Right after ingestion");
        dataset.show(10);


        dataset = dataset.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY")
                .drop("OBJECTID")
                .drop("PERMITID")
                .drop("GEOCODESTATUS");

        dataset.show(10);

        StructType schema = dataset.schema();

        String schemaAsString = schema.mkString();

        String schemaAsJson = schema.prettyJson();
        System.out.println("*** Schema as string: " + schemaAsJson);
    }

    private static void analysisJSON(SparkSession sparkSession){

        Dataset<Row> df = sparkSession.read().format("json")
                .load(MergeDataSetSample.JSON_FILE);
        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();


        df = df.withColumn("county", lit("Durham"))
                .withColumn("datasetId", df.col("fields.id"))
                .withColumn("name", df.col("fields.premise_name"))
                .withColumn("address1", df.col("fields.premise_address1"))
                .withColumn("address2", df.col("fields.premise_address2"))
                .withColumn("city", df.col("fields.premise_city"))
                .withColumn("state", df.col("fields.premise_state"))
                .withColumn("zip", df.col("fields.premise_zip"))
                .withColumn("tel", df.col("fields.premise_phone"))
                .withColumn("dateStart", df.col("fields.opening_date"))
                .withColumn("dateEnd", df.col("fields.closing_date"))
                .withColumn("type",
                        split(df.col("fields.type_description"), " - ").getItem(1))
                .withColumn("geoX", df.col("fields.geolocation").getItem(0))
                .withColumn("geoY", df.col("fields.geolocation").getItem(1));

        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " +
                partitionCount);
        df = df.repartition(4);
        System.out.println("Partition count after repartition: " +
                df.rdd().partitions().length);
    }
}
