package zinjvi.hw4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LineNumberMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(LineNumberMapper.class);

    private LongWritable valueOut = new LongWritable(1);
    private Text countKey = new Text("count");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(countKey, valueOut);
    }
}