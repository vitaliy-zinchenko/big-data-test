package zinjvi;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by ubuntu2 on 17.03.16.
 */
public class CsvConverter implements Function<Tuple2<String,LogData>, String> {
    public String call(Tuple2<String, LogData> stringLogDataTuple2) throws Exception {
        LogData logData = stringLogDataTuple2._2();
        double avg = logData.getBytes() / logData.getNumber();
        return String.format("%s,%s,%d", stringLogDataTuple2._1(), avg, logData.getBytes());
    }
}
