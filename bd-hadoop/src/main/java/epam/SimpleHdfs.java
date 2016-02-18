package epam;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Vitaliy on 2/7/2016.
 */
public class SimpleHdfs {

    private static final String URL = "hdfs://10.23.14.37:8020";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration ();
        URI hdfsUrl = new URI(URL);
        FileSystem fileSystem = FileSystem.get(hdfsUrl, conf);

        boolean exists = fileSystem.exists(new Path("/data"));
        if(exists) {
            System.out.println(exists);
        }

        try ( BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path("/data/file.txt"))))) {
            writer.write("line 1");
        }


    }
}
