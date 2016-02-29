package zinjvi.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LogCombiner extends Reducer<Text, LogMapResultWritable, Text, LogMapResultWritable> {
    @Override
    protected void reduce(Text key, Iterable<LogMapResultWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        int count = 0;
        for(LogMapResultWritable bytes: values) {
            sum += bytes.getBytes().get();
            count += bytes.getCount().get();
        }
        context.write(key, new LogMapResultWritable(new LongWritable(sum), new IntWritable(count)));
    }
}
