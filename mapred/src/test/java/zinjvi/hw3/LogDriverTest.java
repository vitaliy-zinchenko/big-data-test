package zinjvi.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

public class LogDriverTest {

    @Test
    @Ignore
    public void test() throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\zinchenko\\apps\\hadoop");



        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path in = new Path("D:\\Vagrantfile");
        Path out = new Path("D:\\Vagrantfile_oit");

        FileSystem fileSystem = FileSystem.getLocal(conf);
        fileSystem.delete(out, true);

        LogDriver logDriver = new LogDriver();
        logDriver.setConf(conf);

        logDriver.run(new String[]{
            in.toString(),
            out.toString()
        });

        System.out.println("end");
    }

}
