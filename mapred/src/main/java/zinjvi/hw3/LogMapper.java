package zinjvi.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Pattern PATTERN = Pattern.compile("^([^\\s]+) .+ (\\d+) \".+\"");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = PATTERN.matcher(value.toString());
        String ip = matcher.group(1);
        Long bytes = Long.parseLong(matcher.group(2));
        context.write(new Text(ip), new LongWritable(bytes));
        //TODO handle errors
    }
}
