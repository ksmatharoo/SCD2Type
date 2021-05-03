import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Runner {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkUtils.getSparkSession();
        Dataset<Row> ds1 = sparkSession.read().option("header", "true").csv("src/main/resources/dataT1.csv");
        Dataset<Row> ds2 = sparkSession.read().option("header", "true").csv("src/main/resources/dataT2.csv");

        SCDEngine scdEngine = new SCDEngine(ds1, ds2);
        Dataset<Row> df = scdEngine.process("key");
        df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("src/main/resources/ds");
        df.show(false);

        Dataset<Row> newDS = sparkSession.read().option("header", "true").csv("src/main/resources/dataT3.csv");
        Dataset<Row> oldDS = sparkSession.read().parquet("src/main/resources/ds");

        SCDEngine scdEngine1 = new SCDEngine(oldDS, newDS);
        Dataset<Row> process = scdEngine1.process("key");
        process.show(false);

        System.out.println("test");

    }
}
