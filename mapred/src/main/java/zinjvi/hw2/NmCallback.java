package zinjvi.hw2;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;

public class NmCallback implements NMClientAsync.CallbackHandler  {

    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {

    }

    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    public void onContainerStopped(ContainerId containerId) {

    }

    public void onStartContainerError(ContainerId containerId, Throwable throwable) {

    }

    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

    }

    public void onStopContainerError(ContainerId containerId, Throwable throwable) {

    }
}
