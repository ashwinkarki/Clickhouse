import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class ClickHouseSparkExample {
    public static void main(String[] args) {

        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("ClickHouseSparkExample")
                .master("local[*]")
                .getOrCreate();

        // JDBC URL (HTTP port)
        String jdbcURL = "jdbc:clickhouse://localhost:8123/testdb";
        String query = "SELECT * FROM events WHERE user_id < 10";

        //---------------------------------------------------------------------------------------------------
        // Load using jdbc() method
        //---------------------------------------------------------------------------------------------------
        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", "ashwin");
        jdbcProperties.put("password", "mysecret");

        Dataset<Row> df1 = spark.read()
                .jdbc(jdbcURL, String.format("(%s) AS t", query), jdbcProperties);

        System.out.println("=== DataFrame using jdbc() method ===");
        df1.show();

        //---------------------------------------------------------------------------------------------------
        // Load using format("jdbc")
        //---------------------------------------------------------------------------------------------------
        Dataset<Row> df2 = spark.read()
                .format("jdbc")
                .option("url", jdbcURL)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", "ashwin")
                .option("password", "mysecret")
                .option("dbtable", String.format("(%s) AS t", query))
                .load();

        System.out.println("=== DataFrame using format(\"jdbc\") method ===");
        df2.show();

        // Stop Spark
        spark.stop();
    }
}
