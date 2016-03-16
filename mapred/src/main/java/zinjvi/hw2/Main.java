package zinjvi.hw2;

//import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.DefaultParser;
//import org.apache.commons.cli.HelpFormatter;
//import org.apache.commons.cli.Options;
//import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    private static final boolean KEEP_CONTAINERS = true;
    private static final String APPLICATION_NAME = "MyApp";
    private YarnClient yarnClient;
    private Configuration configuration;
    public static LocalResource lr;

//    private Options options;

    public static void main(String[] args) throws IOException, YarnException, URISyntaxException, InterruptedException {
        System.out.println("start");
        Main main = new Main();
//        try {
//            main.init(args);
//        } catch (ParseException e) {
//            main.printUsage();
//            System.exit(1);
//        }
        main.run();
        System.out.println("end");

//        Thread.sleep(1000000L);

        System.exit(0);
    }

    public Main() {

        configuration = new Configuration();

        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);

//        options = new Options();
//        options.addOption("appname", true, "Application name");
//        options.addOption("help", false, "Print usage");
    }

//    public boolean init(String[] args) throws ParseException {
//        CommandLine commandLine = new DefaultParser().parse(options, args);
//        if (commandLine.hasOption("help")) {
//            printUsage();
//            return false;
//        }
//
//        return true;
//    }

//    private void printUsage() {
//        new HelpFormatter().printHelp("Main Yarn App", options);
//    }

    public void run() throws IOException, YarnException, URISyntaxException {
        yarnClient.start();



        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        Resource resource = appResponse.getMaximumResourceCapability();
        System.out.println("Mem = " + resource.getMemory());
        System.out.println("Cores = " + resource.getVirtualCores());

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        System.out.println("appId=" + appId);

        appContext.setKeepContainersAcrossApplicationAttempts(KEEP_CONTAINERS);
        appContext.setApplicationName(APPLICATION_NAME);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        FileSystem fs = FileSystem.get(configuration);
        lr = addToLocalResources(fs, "mapred-1.0-SNAPSHOT.jar", "mapred-1.0-SNAPSHOT.jar", appId.toString(), null);
        localResources.put("mapred-1.0-SNAPSHOT.jar", lr);

        Map<String, String> env = new HashMap<String, String>();



        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : configuration.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        System.out.println("classPathEnv.toString() = " + classPathEnv.toString());

        env.put("CLASSPATH", classPathEnv.toString());


        String command =
                ApplicationConstants.Environment.JAVA_HOME.$$()
                + "/bin/java"
                + " -Xmx512m"
                + " zinjvi.hw2.AppMaster"
                + " --container_memory 512"
                + " --container_vcores 1"
                + " --num_containers 1"
                + " --priority 0"
                + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout"
                + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr";

        System.out.println("command: " + command);

        List<String> commands = new ArrayList<String>();
        commands.add(command);


        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, null, null);

        Resource capability = Resource.newInstance(1000, 1);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);
        appContext.setQueue("default");

        System.out.println("Submit AM");

        yarnClient.submitApplication(appContext);

        monitorApplication(appId);
    }

    private LocalResource addToLocalResources(FileSystem fs, String fileSrcPath,
                                     String fileDstPath, String appId,
                                     String resources) throws IOException {
        String suffix = APPLICATION_NAME + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        System.out.println("destination path = " + dst);

        System.out.println("copy AM jar from local");
        fs.copyFromLocalFile(new Path(fileSrcPath), dst);

        FileStatus scFileStatus = fs.getFileStatus(dst);
        return LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(dst.toUri()),
                        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
                        scFileStatus.getLen(), scFileStatus.getModificationTime());
    }


    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            System.out.println("Got application report from ASM for"
                     + ", appId=" + appId.getId()
                     + ", clientToAMToken=" + report.getClientToAMToken()
                     + ", appDiagnostics=" + report.getDiagnostics()
                     + ", appMasterHost=" + report.getHost()
                     + ", appQueue=" + report.getQueue()
                     + ", appMasterRpcPort=" + report.getRpcPort()
                     + ", appStartTime=" + report.getStartTime()
                     + ", yarnAppState=" + report.getYarnApplicationState().toString()
                     + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                     + ", appTrackingUrl=" + report.getTrackingUrl()
                     + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    System.out.println("Application has completed successfully. Breaking monitoring loop");
                    return true;
                }
                else {
                    System.out.println("Application did finished unsuccessfully."
                                          + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                                          + ". Breaking monitoring loop");
                    return false;
                }
            }
            else if (YarnApplicationState.KILLED == state
                     || YarnApplicationState.FAILED == state) {
                System.out.println("Application did not finish."
                         + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                         + ". Breaking monitoring loop");
                return false;
            }
//
//            if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
//                System.out.println("Reached client specified timeout for application. Killing application");
//                forceKillApplication(appId);
//                return false;
//            }
        }

    }


}
