package zinjvi.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class LogMapReduceTest {

    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, Text> mapReduceDriver;

    @Before
    public void before() {
        LogMapper mapper = new LogMapper();
        LogReducer reducer = new LogReducer();
        mapReduceDriver =  MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapReduceDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 20020 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(2), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40040 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        //when
        List<Pair<Text, Text>> result = mapReduceDriver.run();

        //then
        Assert.assertEquals(1, result.size());

        Assert.assertEquals("ip1", result.get(0).getFirst().toString());
        Assert.assertEquals("30030,60060", result.get(0).getSecond().toString());
    }
}