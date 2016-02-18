package zinjvi.hw3;

import static org.junit.Assert.*;

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

public class LogMapperTest {

    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;

    @Before
    public void before() {
        LogMapper mapper = new LogMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));

        //when
        List<Pair<Text, LongWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(1, result.size());

        Assert.assertEquals("ip1", result.get(0).getFirst().toString());
        Assert.assertEquals(40028L, result.get(0).getSecond().get());
    }

    @Test
    public void shouldNotReturnDataWhenWrongLineFormat() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(1), new Text("the wrong line format"));

        //when
        List<Pair<Text, LongWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(0, result.size());
    }
}