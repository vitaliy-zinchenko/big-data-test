package zinjvi;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class DriverTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private File out;

    private String inputPath;
    private String outputFolderPath;

    static JavaSparkContext sparkCtx;

    @BeforeClass
    public static void sparkSetup() {
        SparkConf conf = new SparkConf();
        sparkCtx = new JavaSparkContext("local", "test", conf);
    }

    @Before
    public void before() throws IOException {
        out = new File(testFolder.getRoot() + "/out");
        inputPath = DriverTest.class.getClassLoader().getResource("test_data.txt").getPath();
        outputFolderPath = "file://" + out;
    }

    @AfterClass
    public static void sparkTeardown() {
        sparkCtx.stop();
    }

    @Test
    public void integrationTest2() throws IOException {
        Driver driver = new Driver(sparkCtx);
        driver.run(inputPath, outputFolderPath);

        String expectedResult = IOUtils.toString(DriverTest.class.getClassLoader().getResourceAsStream("expected.txt"));
        String actualResult = IOUtils.toString(new FileInputStream(out + "/part-00000"));
        Assert.assertEquals(expectedResult, actualResult);
    }
}
