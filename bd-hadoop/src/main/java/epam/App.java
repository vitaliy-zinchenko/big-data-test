package epam;

import com.google.gson.internal.Streams;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static final String URL = "hdfs://10.23.14.37:8020"; //host is specified in 'hosts' file. specified host refers to 127.0.0.1

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

    private DBCollection collection;

    private Map<String, Integer> localDb;

    private Long lines = 0L, ids = 0L;

    public static void main( String[] args ) throws IOException, URISyntaxException {
        new App().run();
    }

    public App() throws URISyntaxException, IOException {
        Configuration conf = new Configuration ();
        URI hdfsUrl = new URI(URL);
        fileSystem = FileSystem.get(hdfsUrl, conf);

        MongoClient mongo = new MongoClient( "10.23.15.18" , 27017 );
        DB db = mongo.getDB("hadoop1");
        collection = db.getCollection("temp");

        localDb = new HashMap<>();
    }

    public void run() throws IOException {
        readSource();
//        writeResult();
    }

    private void writeResult() throws IOException {
        Path resultPath = new Path(BASE_FOLDER + "/bid_result2.txt");
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.append(resultPath)))) {
            DBObject sort = BasicDBObjectBuilder.start("count", -1).get();
            collection.find().sort(sort).forEach(count -> {
                writeResultToHdfs((String)count.get("key"), (Integer)count.get("count"), writer);
            });
        }
    }

    private void readSource() {
        FILE_PATHES.stream()
                .map(Path::new)
                .forEach(this::processFile);
    }

    private void processFile(Path filePath) {
        try {
            boolean fileExists = fileSystem.exists(filePath);
            if(fileExists) {
                LOG.info("File {} exists", filePath);
                FSDataInputStream inputStream = fileSystem.open(filePath);
                try ( BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream)) ) {
                    reader.lines()
                            .map(this::extractIPinyouId)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .forEach(this::createOrUpdate);
                }
            } else {
                LOG.info("File {} doesn't exists", filePath);
            }
            LOG.info("File {} processed. Lines {}, ids {}", filePath, lines, ids);
            lines = ids = 0L;
        } catch (IOException e) {
            LOG.error("!!!", e);
        }
    }

    private Optional<String> extractIPinyouId(String line) {
        lines++;
        String[] lineItems = line.split("\t");
        String iPinyouId = lineItems[I_PINYOU_ID_POSITION];
        return "null".equals(iPinyouId) ? Optional.empty() : Optional.of(iPinyouId);
    }

    private void createOrUpdate(String iPinyouId) {
        Integer count = localDb.get(iPinyouId);
        if(count != null) {
            localDb.put(iPinyouId, count++);
            return;
        } else {
            localDb.put(iPinyouId, 1);
            ids++;
        }

//        DBObject query = BasicDBObjectBuilder.start("key", iPinyouId).get();
//        DBObject count = collection.findOne(query);
//        if (count != null) {
//            DBObject incrementField = BasicDBObjectBuilder.start("count", 1).get();
//            DBObject incrementCommand = BasicDBObjectBuilder.start("$inc", incrementField).get();
//            collection.update(query, incrementCommand);
//            LOG.info("Incremented count for iPinyouId {}", iPinyouId);
//            return;
//        }
//        DBObject newCount = BasicDBObjectBuilder.start(query.toMap()).add("count", 1).get();
//        collection.insert(newCount);
//        LOG.info("Created count for iPinyouId {}", iPinyouId);
    }

    private void writeResultToHdfs(String iPinyouId, Integer count, BufferedWriter writer) {
        try {
            writer.write(iPinyouId + "\t" + count);
            writer.newLine();
            LOG.info("Wrote to HDFS: IPinyouId={} count={}", iPinyouId, count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
