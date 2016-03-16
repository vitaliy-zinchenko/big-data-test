package zinjvi;

import eu.bitwalker.useragentutils.UserAgent;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Mapper implements PairFunction<String, String, LogData> {

    private static final Pattern PATTERN = Pattern.compile("^([^\\s]+) .+ (\\d+) .+ \"(.+)\"$");
    public static final int IP_GROUP_POSITION = 1;
    public static final int BYTES_GROUP_POSITION = 2;
    public static final int USER_AGENT_POSITION = 3;

    private Accumulator<Map<String, Long>> accumulator;

    public Mapper(Accumulator<Map<String, Long>> accumulator) {
        this.accumulator = accumulator;
    }

    //    public Tuple2<String, LogData> call(String s) throws Exception {
//        return null;
//    }


    public Tuple2<String, LogData> call(String line) throws Exception {
        Matcher matcher = PATTERN.matcher(line.toString());
        if(!matcher.find()) {
            System.out.println("Couldn't find!!!");
        }
        String ip = matcher.group(IP_GROUP_POSITION);
        Long bytes = Long.parseLong(matcher.group(BYTES_GROUP_POSITION));
        String userAgentString = matcher.group(USER_AGENT_POSITION);

        UserAgent userAgent = new UserAgent(userAgentString);

        String browser = userAgent.getBrowser().getGroup().name();
        accumulator.add(Collections.singletonMap(browser, 1L));
        return new Tuple2<String, LogData>(ip, new LogData(bytes, 1));
    }
}
