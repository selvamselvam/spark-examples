
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class LoadCSV {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Dataset")
                .master("local[1]")
                .appName("LoadCSV")
                .getOrCreate();
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/books.csv");
        df.show(5);
    }
}
