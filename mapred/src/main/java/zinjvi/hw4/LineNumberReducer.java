package zinjvi.hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LineNumberReducer extends Reducer<IntWritable, Text, LongWritable, Text> {

    private static final int LOCAL_NUMBER_POSITION = 0;
    private static final int LINE_POSITION = 1;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long offset = getOffset(context, key);
        for (Text numberWithLine : values) {
            String[] numberWithLineParsed = numberWithLine.toString().split("_-_");
            long localNumber = Long.parseLong(numberWithLineParsed[LOCAL_NUMBER_POSITION]);
            String line = numberWithLineParsed[LINE_POSITION];

            long globalNumber = offset + localNumber;
            System.out.println(String.format("offset %s, localNumber %s, globalNumber %s", offset, localNumber, globalNumber));
            context.write(new LongWritable(globalNumber), new Text(line));
        }

    }

    private long getOffset(Context context, IntWritable partition) throws IOException, InterruptedException {
        long offset = 0;
        int currentPartition = partition.get();
        System.out.println(String.format("currentPartition = %s", currentPartition));
        while (currentPartition > 0) {
            int previousPartition = currentPartition - 1;
            long linesPerPartition = getMapCounter(context, "LN", "partition_" + previousPartition);
            //context.getCounter("LN", "partition_" + previousPartition).getValue();
            offset += linesPerPartition;
            System.out.println(String.format("in loop currentPartition = %s, previousPartition = %s, linesPerPartition = %s, offset = %s",
                                                      currentPartition,      previousPartition,      linesPerPartition,      offset));
            currentPartition--;
        }
        System.out.println("result offset for partition " + partition + " is " + offset);
        return offset;
    }

    private long getMapCounter(Context context, String groupName, String counterName) throws IOException, InterruptedException {
        Cluster cluster = new Cluster(context.getConfiguration());
        Job currentJob = cluster.getJob(context.getJobID());
        return currentJob.getCounters().findCounter(groupName, counterName).getValue();
    }

}
