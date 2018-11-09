package org.apache.pulsar.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

public abstract class CanalAbstractSource<V> extends PushSource<V> {

    protected final static Logger logger = LoggerFactory.getLogger(CanalAbstractSource.class);

    protected Thread thread = null;

    protected volatile boolean running = false;

    private CanalConnector connector;

    private CanalSourceConfig canalSourceConfig;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        canalSourceConfig = CanalSourceConfig.load(config);
        if (canalSourceConfig.getCluster()) {
            connector = CanalConnectors.newClusterConnector(canalSourceConfig.getZkServers(),
                    canalSourceConfig.getDestination(), canalSourceConfig.getUsername(), canalSourceConfig.getPassword());
        } else {
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(canalSourceConfig.getSingleHostname(), canalSourceConfig.getSinglePort()),
                    canalSourceConfig.getDestination(), canalSourceConfig.getUsername(), canalSourceConfig.getPassword());
        }
        this.start();

    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                process();
            }
        });

        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    @Override
    public void close() throws InterruptedException {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            thread.interrupt();
            thread.join();
        }
        if (connector != null) {
            connector.disconnect();
        }

        MDC.remove("destination");
    }

    protected void process() {
        while (running) {
            try {
                MDC.put("destination", canalSourceConfig.getDestination());
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(canalSourceConfig.getBatchSize());
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                         try {
                         Thread.sleep(1000);
                         } catch (InterruptedException e) {
                         }
                    } else {
                        CanalRecord<V> record = new CanalRecord<>(message, extractValue(message));
                        consume(record);
                    }

                    connector.ack(batchId);
                    // connector.rollback(batchId);
                }
            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    public abstract V extractValue(Message message);

    static private class CanalRecord<V> implements Record<V> {

        private final Message record;
        private final V value;


        public CanalRecord(Message message, V value) {
            this.record = message;
            this.value = value;
        }

        @Override
        public Optional<String>  getKey() {
            return Optional.of(Long.toString(record.getId()));
        }

        @Override
        public V getValue() {
            return value;
        }
    }

}
