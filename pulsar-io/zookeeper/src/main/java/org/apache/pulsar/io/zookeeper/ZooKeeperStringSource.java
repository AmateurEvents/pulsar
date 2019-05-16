package org.apache.pulsar.io.zookeeper;

//import org.apache.zookeeper.server.Request;
import org.apache.pulsar.shade2.org.apache.zookeeper.server.Request;


public class ZooKeeperStringSource extends ZooKeeperAbstractSource<String> {

    @Override
    public String extractValue(Request request) {
        return request.toString();
    }
}