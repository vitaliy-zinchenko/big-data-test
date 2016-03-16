package zinjvi;

import org.apache.spark.AccumulatorParam;

import java.util.HashMap;
import java.util.Map;

public class MapAccumulator implements AccumulatorParam<Map<String, Long>> {

    public Map<String, Long> addAccumulator(Map<String, Long> t1, Map<String, Long> t2) {
        return merge(t1, t2);
    }

    public Map<String, Long> addInPlace(Map<String, Long> r1, Map<String, Long> r2) {
        return merge(r1, r2);
    }

    public Map<String, Long> zero(Map<String, Long> initialValue) {
        return new HashMap<String, Long>();
    }

    private Map<String, Long> merge(Map<String, Long> mapFirst, Map<String, Long> mapSecond) {
        Map<String, Long> mapResult = new HashMap<String, Long>(mapFirst);
        for (Map.Entry<String, Long> entry : mapSecond.entrySet()) {
            if (mapResult.containsKey(entry.getKey())) {
                Long mergedValue = mapResult.get(entry.getKey()) + entry.getValue();
                mapResult.put(entry.getKey(), mergedValue);
            } else {
                mapResult.put(entry.getKey(), entry.getValue());
            }
        }
        return mapResult;
    }

}
