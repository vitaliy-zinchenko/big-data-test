package zinjvi.hw2;

import com.sun.net.httpserver.HttpExchange;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class AppMaster {

    private AMRMClientAsync amRMClient;

    public static void main(String[] args) throws InterruptedException, IOException, YarnException {
        System.out.println("AppMaster start");


//        Thread.sleep(1000L);


        AppMaster appMaster = new AppMaster();
        appMaster.run();


        System.exit(0);
    }

    public void run() throws IOException, YarnException, InterruptedException {
        final Configuration configuration = new Configuration();

        RmCallback rmCallback = new RmCallback();

        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, rmCallback);
        amRMClient.init(configuration);
        amRMClient.start();

        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, -1, "");

        SimpleHttpServer simpleHttpServer = new SimpleHttpServer(8889);

        simpleHttpServer.addHandler("stop", new Handler() {
            public String handle(HttpExchange httpExchange) throws IOException {
                FinalApplicationStatus status = FinalApplicationStatus.SUCCEEDED;
                try {
                    amRMClient.unregisterApplicationMaster(status, null, null);
                } catch (YarnException e) {
                    System.out.println("Error!!!");
                    e.printStackTrace(); //TODO
                }
                return "stopping application master";
            }
        });

        simpleHttpServer.addHandler("createContainer", new Handler() {
            public String handle(HttpExchange httpExchange) throws IOException {

                Priority pri = Priority.newInstance(0);
                Resource capability = Resource.newInstance(256, 1);
                AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, pri);
                amRMClient.addContainerRequest(request);



                return "cc";
            }
        });

        simpleHttpServer.run();

        while(true) {
            Thread.sleep(1000L);
//            System.out.println("Ask is server stopped.");
            if (simpleHttpServer.isStopped()) {
                System.out.println("Server is stopped.");
                return;
            }
        }


    }

}
