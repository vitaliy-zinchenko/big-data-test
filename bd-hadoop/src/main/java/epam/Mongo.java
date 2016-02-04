package epam;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

public class Mongo {

    public static void main(String[] args) throws UnknownHostException {
        MongoClient mongo = new MongoClient( "localhost" , 27017 );
        DB db = mongo.getDB("test");
        DBCollection collection = db.getCollection("user");

        BasicDBObject object = new BasicDBObject("kk", 1);
        collection.insert(object);
    }

}
