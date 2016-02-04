package epam;

import com.google.gson.internal.Streams;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private static final String URL = "hdfs://sandbox.hortonworks.com:8020"; //host is specified in 'hosts' file. specified host refers to 127.0.0.1

    private static final int I_PINYOU_ID_POSITION = 2;

    private static final String BASE_FOLDER = "/user/hue/test";

    private static final List<String> FILE_PATHES = Arrays.asList(
            BASE_FOLDER + "/bid.20130606.txt",
            BASE_FOLDER + "/bid.20130607.txt",
            BASE_FOLDER + "/bid.20130608.txt",
            BASE_FOLDER + "/bid.20130609.txt",
            BASE_FOLDER + "/bid.20130610.txt",
            BASE_FOLDER + "/bid.20130611.txt",
            BASE_FOLDER + "/bid.20130612.txt"
    );

    private FileSystem fileSystem;

    public static void main( String[] args ) throws IOException, URISyntaxException {
        new App().run();
    }

    public App() throws URISyntaxException, IOException {
        Configuration conf = new Configuration ();
        URI hdfsUrl = new URI(URL);
        fileSystem = FileSystem.get(hdfsUrl, conf);
    }

    public void run() throws IOException {

        FILE_PATHES.stream()
                .map(Path::new)
                .peek(System.out::println)
                .map(this::processFile)
                .forEach(System.out::println);
//                .collect(Collectors.toMap(Function.<String>identity(), iPinyouId -> 1, (integer, integer2) -> integer++, () -> {
//                    TreeMap treeMap = new TreeMap();
//                }))



        /*

        Path filePath = new Path("/user/hue/test/bid.20130608.txt");
        boolean fileExists = fileSystem.exists(filePath);
        if(fileExists) {
            System.out.println("file " + filePath + " exists");
            FSDataInputStream inputStream = fileSystem.open(filePath);

            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            reader.lines().map(line -> {
                String[] lineItems = line.split("\t");
                String iPinyouId = lineItems[I_PINYOU_ID_POSITION];
                return "null".equals(iPinyouId) ? Optional.empty() : Optional.of(iPinyouId);
            }).filter(Optional::isPresent)
                    .map(Optional::get)
                    .reduce((o, o2) -> )

//        })


        }

        */

    }

    private Stream<String> processFile(Path filePath) {
        try {
            boolean fileExists = fileSystem.exists(filePath);
            if(fileExists) {
                LOG.info("File {} exists", filePath);
                FSDataInputStream inputStream = fileSystem.open(filePath);
                try ( BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)) ) {
                    reader.lines()
                            .map(s -> {
                                return extractIPinyouId(s);
                            })
                            .forEach(s1 -> {
                                System.out.println(s1.orElse("nothing"));
                            });

                    return Stream.empty();
//                            .filter(Optional::isPresent)
//                            .map(Optional::get);
                }
            }
            LOG.info("File {} doesn't exists", filePath);
            return Stream.<String>empty();
        } catch (IOException e) {
            LOG.error("!!!", e);
            return Stream.<String>empty();
        }
    }

    private Optional<String> extractIPinyouId(String line) {
        String[] lineItems = line.split("\t");
        String iPinyouId = lineItems[I_PINYOU_ID_POSITION];
        return "null".equals(iPinyouId) ? Optional.empty() : Optional.of(iPinyouId);
    }


}
