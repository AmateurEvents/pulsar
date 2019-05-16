package org.apache.pulsar.io.zookeeper;

import io.streamnative.QuorumPeerConfig;
import io.streamnative.ZooKeeperObserver;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.shade2.org.apache.zookeeper.server.Request;
import org.apache.pulsar.shade2.org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Simple abstract class for mysql binlog sync to pulsar.
 */
@Slf4j
public abstract class ZooKeeperAbstractSource<V> extends PushSource<V> {

    private ZooKeeperSourceConfig zooKeeperSourceConfig;

    protected ZooKeeperObserver zooKeeperObserver;

    protected Thread thread = null;

    protected Thread observerThread = null;

    protected volatile boolean running = false;

    protected volatile boolean observerRunning = false;

    protected static BlockingQueue<Request> requests;

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        zooKeeperSourceConfig = ZooKeeperSourceConfig.load(config);

        QuorumPeerConfig quorumConfig = new QuorumPeerConfig();

        zooKeeperObserver = new ZooKeeperObserver();

        requests = new LinkedBlockingQueue<Request>();

        try {
            quorumConfig.parse(zooKeeperSourceConfig.getConfigPath());
            zooKeeperObserver.initConfig(quorumConfig);
            zooKeeperObserver.getQuorumPeer().setObserverRequest(new ObserverRequestImpl(requests));
        } catch (ConfigException e) {
            log.error("zookeeper invalid config {}", e);
            System.exit(2);
        }
        log.info("zookeeper init config success");
//        zooKeeperObserver.start();
        this.start();
        this.startObserver();
    }

    protected void start() {
        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });

        thread.setName("zookeeper observer");
        running = true;
        thread.start();
    }

    protected void startObserver() {
        observerThread = new Thread(new Runnable() {

            @Override
            public void run() {
                zooKeeperObserver.start();
            }
        });

        observerThread.setName("zookeeper observer");
        observerRunning = true;
        observerThread.start();
    }

    @Override
    public void close() throws InterruptedException {
        log.info("close zookeeper source");
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread.join();
        }
    }

    protected void process() {
        while (running) {
            try {
                while (running) {
                    while (requests != null && !requests.isEmpty()) {
                        Request request = requests.take();
                        System.out.println(request.toString());
                        ZooKeeperRecord<V> zooKeeperRecord = new ZooKeeperRecord<>();
                        zooKeeperRecord.setId(request.zxid);
                        zooKeeperRecord.setRecord(extractValue(request));
                        consume(zooKeeperRecord);
                    }
                }
            } catch (Exception e) {
                log.error("Parse request failed");
            }
        }
    }

    public abstract V extractValue(Request request);

    @Getter
    @Setter
    static private class ZooKeeperRecord<V> implements Record<V> {
        private V record;
        private Long id;

        @Override
        public Optional<String> getKey() {
            return Optional.of(Long.toString(id));
        }

        @Override
        public V getValue() {
            return record;
        }

        @Override
        public Optional<Long> getRecordSequence() {
            return Optional.of(id);
        }

    }
}
