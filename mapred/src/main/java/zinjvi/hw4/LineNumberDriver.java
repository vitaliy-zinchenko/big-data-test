package zinjvi.hw4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.lang.reflect.Array;
import java.util.Arrays;

public class LineNumberDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int code = ToolRunner.run(new LineNumberDriver(), args);
        System.exit(code);
    }

    public int run(String[] args) throws Exception {
        System.out.println("args:" + Arrays.asList(args));


        Configuration conf = getConf();

        System.out.println("mapreduce.job.reduces=" + conf.get("mapreduce.job.reduces"));

        Job job = Job.getInstance(conf, "log map reduce");
        job.setJarByClass(LineNumberDriver.class);
        job.setMapperClass(LineNumberMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        System.out.println("job.getNumReduceTasks()=" + job.getNumReduceTasks());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
