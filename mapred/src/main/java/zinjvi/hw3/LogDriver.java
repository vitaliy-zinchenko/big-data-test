package zinjvi.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int code = ToolRunner.run(new LogDriver(), args);
    System.exit(code);
  }

  public int run(String[] args) throws Exception {
    System.out.println("Start!!");

    Configuration conf = getConf();
    conf.set(TextOutputFormat.SEPERATOR, ",");
    conf.set(MRJobConfig.COUNTERS_MAX_KEY, "2000");

    Job job = Job.getInstance(conf, "log map reduce");
    job.setJarByClass(LogDriver.class);
    job.setMapperClass(LogMapper.class);
    job.setCombinerClass(LogCombiner.class);
    job.setReducerClass(LogReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LogMapResultWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LogResultWritable.class);

//    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}