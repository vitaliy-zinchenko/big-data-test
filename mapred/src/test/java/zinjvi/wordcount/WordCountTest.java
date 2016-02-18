package zinjvi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import zinjvi.wordcount.WCMapper;

import java.util.List;

public class WordCountTest {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void before() {
        WCMapper mapper = new WCMapper();
       mapDriver = MapDriver.newMapDriver(mapper);

//        AwesomenessRatingReducer reducer = new AwesomenessRatingReducer();
//        ReduceDriver<LongWritable, AwesomenessRatingWritable, LongWritable, Text> reduceDriver = ReduceDriver.newReduceDriver(reducer);

//        MapReduceDriver<LongWritable, Text, LongWritable, AwesomenessRatingWritable, LongWritable, Text> mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMain() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(1), new Text("one two two"));

        //when
        List<Pair<Text, IntWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(3, result.size());

        Assert.assertEquals(new Text("one"), result.get(0).getFirst());
        Assert.assertEquals(new IntWritable(1),  result.get(0).getSecond());

        Assert.assertEquals(new Text("two"), result.get(1).getFirst());
        Assert.assertEquals(new IntWritable(1),  result.get(1).getSecond());

        Assert.assertEquals(new Text("two"), result.get(2).getFirst());
        Assert.assertEquals(new IntWritable(1),  result.get(2).getSecond());
    }
}