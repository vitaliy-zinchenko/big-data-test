package zinjvi;

import org.apache.spark.api.java.function.Function;

public class TestFilter implements Function<String, Boolean> {

    public Boolean call(String v1) throws Exception {
        return v1.startsWith("d");
    }
}
