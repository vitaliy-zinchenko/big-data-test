package zinjvi.hw4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LineNumberMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(LineNumberMapper.class);

    private LongWritable valueOut = new LongWritable(1);
    private Text countKey = new Text("count");

    private int counter = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int partitionId = context.getConfiguration().getInt("mapreduce.task.partition", 0);
        System.out.println("partitionId = " + partitionId + "    " + this);
        context.write(new IntWritable(partitionId), new Text(counter + "_-_" + value));
        context.getCounter("LN", "partition_" + partitionId).setValue(counter);
        counter++;
    }
}