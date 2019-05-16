package org.apache.pulsar.io.zookeeper;

import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * ZooKeeper source config.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class ZooKeeperSourceConfig {

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "ZooKeeper configuration path")
    private String configPath;

    public static ZooKeeperSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), ZooKeeperSourceConfig.class);
    }


    public static ZooKeeperSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), ZooKeeperSourceConfig.class);
    }
}
