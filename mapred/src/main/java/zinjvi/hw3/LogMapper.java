package zinjvi.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(LogMapper.class);

    private static final Pattern PATTERN = Pattern.compile("^([^\\s]+) .+ (\\d+) \".+\"");
    public static final int IP_GROUP_POSITION = 1;
    public static final int BYTES_GROUP_POSITION = 2;
    public static final int GROUPS_COUNT = 2;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = PATTERN.matcher(value.toString());
        if(!matcher.find()) {
            LOG.warn("Could not find required data in log line {}", value);
            return;
        }
        String ip = matcher.group(IP_GROUP_POSITION);
        Long bytes = Long.parseLong(matcher.group(BYTES_GROUP_POSITION));
        context.write(new Text(ip), new LongWritable(bytes));
    }
}