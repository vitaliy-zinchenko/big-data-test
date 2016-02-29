package zinjvi.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogMapper extends Mapper<LongWritable, Text, Text, LogMapResultWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(LogMapper.class);

    private static final Pattern PATTERN = Pattern.compile("^([^\\s]+) .+ (\\d+) .+ \"([^\\/]+).+$");
    public static final int IP_GROUP_POSITION = 1;
    public static final int BYTES_GROUP_POSITION = 2;
    public static final int GROUPS_COUNT = 3;
    private static final int BROWSER_GROUP_POSITION = 3;
    public static final String LOG_BROWSERS_COUNTER = "Log Browsers";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("map!!!!!!!!!!!");
        LOG.info("map keyIn '{}' valueIn '{}'", key, value);
        Matcher matcher = PATTERN.matcher(value.toString());
        if(!matcher.find()) {
            LOG.warn("Could not find required data in log line {}", value);
            return;
        }
        String ip = matcher.group(IP_GROUP_POSITION);
        Long bytes = Long.parseLong(matcher.group(BYTES_GROUP_POSITION));
        String browser = matcher.group(BROWSER_GROUP_POSITION);

        context.getCounter(LOG_BROWSERS_COUNTER, browser).increment(1);

        Text keyOut = new Text(ip);
        LongWritable valueOut = new LongWritable(bytes);
        LOG.info("map keyOut '{}', valueOut '{}'", keyOut, valueOut);



        context.write(keyOut, new LogMapResultWritable(valueOut, new IntWritable(1)));
    }
}