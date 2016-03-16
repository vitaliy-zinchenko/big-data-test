package zinjvi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DriverTest {
    static JavaSparkContext sparkCtx;

    @BeforeClass
    public static void sparkSetup() {
        // Setup Spark
        SparkConf conf = new SparkConf();
        sparkCtx = new JavaSparkContext("local", "test", conf);
    }

    @AfterClass
    public static void sparkTeardown() {
        sparkCtx.stop();
    }

    @Test
    public void integrationTest() {
        JavaRDD<String> logRawInput = sparkCtx.parallelize(Arrays.asList("data1", "data2", "garbage", "data3"));
        long count = logRawInput.count();
        System.out.println(count);

        List<String> result = logRawInput
                .filter(new TestFilter())
                .collect();

        System.out.println(result);
    }

    @Test
    public void integrationTest2() {
        String srcPath = "file:///D:\\zinchenko\\BDCC\\training\\big-data-test\\spark1java\\test_data.txt";

        Driver driver = new Driver(sparkCtx);
        driver.run(srcPath);
    }
}
