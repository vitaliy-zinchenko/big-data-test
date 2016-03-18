package zinjvi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class DriverRunner {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spark1 hw log processor");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        Driver driver = new Driver(javaSparkContext);
        String logFile = "/user/root/spark_hw1/access_logs.log";
        String destPath = "/user/root/spark_hw1/out";
        driver.run(logFile, destPath);
    }

}
