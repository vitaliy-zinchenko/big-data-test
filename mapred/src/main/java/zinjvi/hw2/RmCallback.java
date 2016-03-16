package zinjvi.hw2;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class RmCallback implements AMRMClientAsync.CallbackHandler {

    public void onContainersCompleted(List<ContainerStatus> list) {
        System.out.println("onContainersCompleted");
    }

    public void onContainersAllocated(List<Container> list) {
        System.out.println("onContainersAllocated");
        System.out.println("containers size = " + list.size());
        for(Container container: list) {
            System.out.println("containerId = " + container.getId());
            System.out.println("NodeHttpAddress = " + container.getNodeHttpAddress());
        }

        Configuration configuration = new Configuration();

        NmCallback nmCallback = new NmCallback();
        NMClientAsyncImpl nmClientAsync = new NMClientAsyncImpl(nmCallback);
        nmClientAsync.init(configuration);
        nmClientAsync.start();





        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : configuration.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }



        System.out.println("classPathEnv.toString() = " + classPathEnv.toString());

        Map<String, String> env = new HashMap<String, String>();

        env.put("CLASSPATH", classPathEnv.toString());


        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

//        FileSystem fs = null;
//        try {
//            fs = FileSystem.get(configuration);
//        } catch (IOException e) {
//            System.out.println("!!!!!!!!!!!!!!!!!!!! could not get FileSystem");
//            e.printStackTrace();
//        }
//        try {
//            addToLocalResources(fs, "mapred-1.0-SNAPSHOT.jar", "mapred-1.0-SNAPSHOT.jar", list.get(0).getId().toString(),
//                                localResources, null);
//        } catch (IOException e) {
//            System.out.println("!!!!!!!!!!!!!!!!!!!! could not addToLocalResources");
//            e.printStackTrace();
//        }


        localResources.put("mapred-1.0-SNAPSHOT.jar", Main.lr);



        String command =
                ApplicationConstants.Environment.JAVA_HOME.$$()
                + "/bin/java jar /root/hw2/container-1.0-SNAPSHOT.jar"
                + " -Xmx512m"
//                + " zinjvi.hw2.AppMaster"
//                + " zinjvi.hw2.Container"
                + " --container_memory 512"
                + " --container_vcores 1"
                + " --num_containers 1"
                + " --priority 0"
                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Container.stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Container.stderr";


//        String command =
//                "pwd"
////                + " -Xmx512m"
////                + " zinjvi.hw2.Container"
////                + " --container_memory 512"
////                + " --container_vcores 1"
////                + " --num_containers 1"
////                + " --priority 0"
//                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Container.stdout"
//                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Container.stderr";

        List<String> commands = new ArrayList<String>();
        commands.add(command);

        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);
//        containerListener.addContainer(list.get(0).getId(), list.get(0));
        nmClientAsync.startContainerAsync(list.get(0), ctx);
        System.out.println("Started container");
    }

    public void onShutdownRequest() {
        System.out.println("onShutdownRequest");
    }

    public void onNodesUpdated(List<NodeReport> list) {
        System.out.println("onNodesUpdated");
    }

    public float getProgress() {
        System.out.println("getProgress");
        return 0;
    }

    public void onError(Throwable throwable) {
        System.out.println("onError");

    }

    private void addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = "/tmp/MyApp" + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(suffix);
        System.out.println("destination path = " + dst);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem
                        .create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            System.out.println("copy AM jar from local");
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
        localResources.put(fileDstPath, scRsrc);
    }
}
