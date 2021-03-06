package zinjvi.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class LogReducer extends Reducer<Text, LogMapResultWritable, Text, LogResultWritable> {
    @Override
    protected void reduce(Text key, Iterable<LogMapResultWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        int count = 0;
        for(LogMapResultWritable bytes: values) {
            sum += bytes.getBytes().get();
            count += bytes.getCount().get();
        }

        Long avgBytes = sum / count;
        LogResultWritable logResultWritable = new LogResultWritable();
        logResultWritable.setAvgBytes(new LongWritable(avgBytes));
        logResultWritable.setSumBytes(new LongWritable(sum));
        context.write(key, logResultWritable);
    }
}
