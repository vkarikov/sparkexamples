import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Spark user-activity").setMaster("local[2]") //local - означает запуск в локальном режиме.
                .set("spark.driver.host", "localhost");//это тоже необходимо для локального режима

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "vkarikov");
        connectionProperties.put("password", "vkarikov");
        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:oracle:thin:@localhost:1521:xe", "VKARIKOV.ULOG", connectionProperties);

        jdbcDF2.show();
    }

}
