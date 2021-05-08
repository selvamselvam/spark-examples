
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.SPACE;

public class SparkHelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello world");

        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("careerdrill.com")
                .getOrCreate();
        System.out.println("Spark version: " + spark.version());
        System.out.println("Spark version: " + spark.sparkContext().appName());
        System.out.println("Spark version: " + spark.sparkContext().applicationId());

        SparkContext ctx2 = spark.sparkContext();
        ctx2.hadoopConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        ctx2.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        JavaSparkContext ctx = new JavaSparkContext(ctx2);
        JavaRDD<String> lines = ctx.textFile("file///c:/selvam/sample.txt", 1);


        JavaRDD<String> words
                = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        // collect RDD for printing
        for (String word : words.collect()) {
            System.out.println("* " + word);
        }

        JavaPairRDD<String, Integer> counts = lines
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        ctx.stop();


    }
}
