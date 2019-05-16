package org.apache.pulsar.io.zookeeper;

import io.streamnative.ObserverRequest;
import org.apache.pulsar.shade2.org.apache.zookeeper.server.Request;
//import org.apache.zookeeper.server.Request;

import java.util.concurrent.BlockingQueue;

public class ObserverRequestImpl implements ObserverRequest {

    private BlockingQueue<Request> records;

    public ObserverRequestImpl(BlockingQueue<Request> records) {
        this.records = records;
    }

    public void onReceived(Request request) {
        if (request != null) {
            System.out.println(request.toString());
        }
        records.add(request);
    }
}
