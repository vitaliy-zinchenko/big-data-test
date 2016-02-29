package zinjvi.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogMapResultWritable implements Writable {

    private LongWritable bytes;
    private IntWritable count;

    public LogMapResultWritable() {
        bytes = new LongWritable();
        count = new IntWritable();
    }

    public LogMapResultWritable(LongWritable bytes, IntWritable count) {
        this.bytes = bytes;
        this.count = count;
    }

    public void write(DataOutput out) throws IOException {
        bytes.write(out);
        count.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        bytes.readFields(in);
        count.readFields(in);
    }

    public LongWritable getBytes() {
        return bytes;
    }

    public void setBytes(LongWritable bytes) {
        this.bytes = bytes;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }
}
