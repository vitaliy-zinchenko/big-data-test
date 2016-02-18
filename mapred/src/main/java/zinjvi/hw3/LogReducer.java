package zinjvi.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class LogReducer extends Reducer<Text, LongWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long bytesSum = 0L;
        Integer count = 0;
        for(LongWritable bytes: values) {
            bytesSum += bytes.get();
            count++;
        }
        Long avgBytes = bytesSum / count;
        context.write(key, new Text(avgBytes + "," + bytesSum));
    }
}
