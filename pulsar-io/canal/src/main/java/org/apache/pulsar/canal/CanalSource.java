package org.apache.pulsar.canal;

import com.alibaba.otter.canal.protocol.Message;


public class CanalSource extends CanalAbstractSource {

    @Override
    public String extractValue(Message message) {
        return message.toString();
    }

}
