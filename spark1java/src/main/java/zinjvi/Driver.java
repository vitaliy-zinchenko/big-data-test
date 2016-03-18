package zinjvi;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

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

    public void run(String srcPath, String destPath) {
        final Accumulator<Map<String, Long>> accumulator = sparkContext.accumulator(new HashMap<String, Long>(), "BrowsersAccumulators", new MapAccumulator());

        System.out.println("Src Path: " + srcPath);

        sparkContext
                .textFile(srcPath)
                .mapToPair(new Mapper(accumulator))
                .reduceByKey(new Reducer())
                .map(new CsvConverter())
                .saveAsTextFile(destPath);
//                .foreach(new Printer());

        for (Map.Entry<String, Long> entry : accumulator.value().entrySet()) {
            System.out.println(entry.getKey() + " - " + entry.getValue());
        }
    }
}
