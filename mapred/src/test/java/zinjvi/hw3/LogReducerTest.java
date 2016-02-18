package zinjvi.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by Vitaliy on 2/18/2016.
 */
public class LogReducerTest {

    ReduceDriver<Text, LongWritable, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {
        LogReducer logReducer = new LogReducer();
        reduceDriver = ReduceDriver.newReduceDriver(logReducer);
    }

    @Test
    public void testReduce() throws Exception {
        List<LongWritable> bytesList = Arrays.asList(new LongWritable(200), new LongWritable(400));
        reduceDriver.withInput(new Text("ip1"), bytesList);

        List<Pair<Text, Text>> result = reduceDriver.run();

        Assert.assertEquals(1, result.size());

        Assert.assertEquals("ip1", result.get(0).getFirst().toString());
        Assert.assertEquals("300,600", result.get(0).getSecond().toString());
    }
}