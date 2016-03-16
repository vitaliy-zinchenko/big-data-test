package zinjvi;

import org.apache.spark.api.java.function.Function2;

public class Reducer implements Function2<LogData, LogData, LogData> {

    public LogData call(LogData v1, LogData v2) throws Exception {
        return new LogData(v1.getBytes() + v2.getBytes(), v1.getNumber() + v2.getNumber());
    }
}
