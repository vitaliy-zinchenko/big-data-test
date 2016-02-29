package zinjvi.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogResultWritable implements Writable {

    private LongWritable avgBytes;

    private LongWritable sumBytes;

    public LogResultWritable() {
        avgBytes = new LongWritable();
        sumBytes = new LongWritable();
    }

    public LogResultWritable(LongWritable avgBytes, LongWritable sumBytes) {
        this.avgBytes = avgBytes;
        this.sumBytes = sumBytes;
    }

    public void write(DataOutput dataOutput) throws IOException {
        avgBytes.write(dataOutput);
        sumBytes.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        avgBytes.readFields(dataInput);
        sumBytes.readFields(dataInput);
    }

    public LongWritable getAvgBytes() {
        return avgBytes;
    }

    public void setAvgBytes(LongWritable avgBytes) {
        this.avgBytes = avgBytes;
    }

    public LongWritable getSumBytes() {
        return sumBytes;
    }

    public void setSumBytes(LongWritable sumBytes) {
        this.sumBytes = sumBytes;
    }

    @Override
    public String toString() {
        return avgBytes + "," + sumBytes;
    }
}
