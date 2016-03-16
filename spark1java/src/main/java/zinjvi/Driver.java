package zinjvi;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class Driver {

    private JavaSparkContext sparkContext;

    public Driver(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public void run(String srcPath) {
        final Accumulator<Map<String, Long>> accumulator = sparkContext.accumulator(new HashMap<String, Long>(), "BrowsersAccumulators", new MapAccumulator());

        sparkContext
                .textFile(srcPath)
                .mapToPair(new Mapper(accumulator))
                .reduceByKey(new Reducer())
                .saveAsTextFile("D:\\zinchenko\\BDCC\\training\\big-data-test\\spark1java\\q");

        for (Map.Entry<String, Long> entry : accumulator.value().entrySet()) {
            System.out.println(entry.getKey() + " - " + entry.getValue());
        }
    }
}
