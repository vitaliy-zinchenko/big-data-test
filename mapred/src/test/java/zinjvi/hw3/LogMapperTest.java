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

    MapDriver<LongWritable, Text, Text, LogMapResultWritable> mapDriver;

    @Before
    public void before() {
        LogMapper mapper = new LogMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMap() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(1), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withInput(new LongWritable(2), new Text("ip1124 - - [26/Apr/2011:15:22:56 -0400] \"GET /logs/administrator/components/com_gmajax/admin.gmajax.php?mosConfig_absolute_path=../../../../../../../../../../../../../../../proc/self/environ%00 HTTP/1.1\" 404 333 \"-\" \"libwww-perl/5.805\""));

        //when
        List<Pair<Text, LogMapResultWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(2, result.size());

        Assert.assertEquals("ip1", result.get(0).getFirst().toString());
        Assert.assertEquals(new LongWritable(40028L), result.get(0).getSecond().getBytes());
        Assert.assertEquals(1, mapDriver.getCounters().findCounter(LogMapper.LOG_BROWSERS_COUNTER, "Mozilla").getValue());

        Assert.assertEquals("ip1124", result.get(1).getFirst().toString());
        Assert.assertEquals(new LongWritable(40028L), result.get(0).getSecond().getBytes());
        Assert.assertEquals(1, mapDriver.getCounters().findCounter(LogMapper.LOG_BROWSERS_COUNTER, "libwww-perl").getValue());
    }

    @Test
    public void shouldNotReturnDataWhenWrongLineFormat() throws Exception {
        //given
        mapDriver.withInput(new LongWritable(1), new Text("the wrong line format"));

        //when
        List<Pair<Text, LogMapResultWritable>> result = mapDriver.run();

        //then
        Assert.assertEquals(0, result.size());
    }
}