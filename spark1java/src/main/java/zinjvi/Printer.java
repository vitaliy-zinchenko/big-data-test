package zinjvi;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by ubuntu2 on 17.03.16.
 */
public class Printer implements VoidFunction<Tuple2<String, LogData>> {
    public void call(Tuple2<String, LogData> stringLogDataTuple2) throws Exception {
        System.out.println(stringLogDataTuple2._1() + " " + stringLogDataTuple2._2());
    }
}