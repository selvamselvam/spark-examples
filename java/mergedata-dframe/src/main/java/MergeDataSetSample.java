import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MergeDataSetSample {
    public static final String JSON_FILE = "data/Restaurants_in_Durham_County_NC.json";
    public static final String CSV_FILE = "data/Restaurants_in_Wake_County_NC.csv";

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[1]")
                .appName("Frame")
                .getOrCreate();


        MergeDataSet mergeDataSet = new MergeDataSet(sparkSession);

        Dataset<Row> wakeRestaurantsDf = mergeDataSet.buildWakeRestaurantsDataframe();
        Dataset<Row> durhamRestaurantsDf = mergeDataSet.buildDurhamRestaurantsDataframe();
        mergeDataSet.combineDataframes(wakeRestaurantsDf, durhamRestaurantsDf);

    }

}
