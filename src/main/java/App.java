import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

    public static void main(String[] args) throws ClassNotFoundException {

        SparkConf conf = new SparkConf()

                .setAppName("Spark user-activity").setMaster("local[2]"); //local - означает запуск в локальном режиме.
                //.set("spark.driver.host", "localhost");//это тоже необходимо для локального режима


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf)
                .getOrCreate();

        Class.forName("net.sf.log4jdbc.DriverSpy");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "vkarikov");
        connectionProperties.put("password", "vkarikov");
        connectionProperties.put("partitionColumn", "id");
        connectionProperties.put("lowerBound", "1");
        connectionProperties.put("upperBound", "1000");
        connectionProperties.put("numPartitions", "10");


        Dataset<Row> jdbcDF2 = spark.read()
                //.jdbc("jdbc:log4jdbc:oracle:thin:@localhost:1521:xe", "VKARIKOV.ULOG2", connectionProperties);
                .jdbc("jdbc:oracle:thin:@localhost:1521:xe", "VKARIKOV.ULOG2", connectionProperties);

        //jdbcDF2.show();
        jdbcDF2.count();
    }

}
