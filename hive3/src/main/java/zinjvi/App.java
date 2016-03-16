package zinjvi;

import org.apache.hive.hcatalog.streaming.ConnectionError;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.ImpersonationFailed;
import org.apache.hive.hcatalog.streaming.InvalidPartition;
import org.apache.hive.hcatalog.streaming.InvalidTable;
import org.apache.hive.hcatalog.streaming.PartitionCreationFailed;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
            throws InterruptedException, StreamingException, ClassNotFoundException {

        System.out.println("start");

        String dbName = "default";
        String tblName = "alerts";
        ArrayList<String> partitionVals = new ArrayList<String>(2);
        partitionVals.add("Asia");
        partitionVals.add("India");
        String serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

        HiveEndPoint hiveEP = new HiveEndPoint("thrift://sandbox:9083", dbName, tblName, partitionVals);




        //-------   Thread 1  -------//

        System.out.println("connecting");
        StreamingConnection connection = hiveEP.newConnection(true);
        System.out.println("connected");

        String[] fieldNames = {"id", "msg"};
        DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames,",", hiveEP);

        System.out.println("fetching TransactionBatch");
        TransactionBatch txnBatch = connection.fetchTransactionBatch(10, writer);
        System.out.println("fetched TransactionBatch");

        txnBatch.beginNextTransaction();
        txnBatch.write("1,Hello streaming".getBytes());
        txnBatch.write("2,Welcome to streaming".getBytes());
        txnBatch.commit();

        System.out.println("commited");

        txnBatch.close();
        connection.close();


//        if(txnBatch.remainingTransactions() > 0) {
//            txnBatch.beginNextTransaction();
//            txnBatch.write("3,Roshan Naik".getBytes());
//            txnBatch.write("4,Alan Gates".getBytes());
//            txnBatch.write("5,Owen O’Malley".getBytes());
//            txnBatch.commit();
//
//
//            txnBatch.close();
//            connection.close();
//        }
//
//
//        txnBatch = connection.fetchTransactionBatch(10, writer);

    }
}
