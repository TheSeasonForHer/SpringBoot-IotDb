package com.iotdb.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author tjb
 * @date 2024/8/2
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "iotdb")
public class IoTDBProperties {

    @Value("${host:127.0.0.1}")
    private String host;

    @Value("${port:6667}")
    private int port;

    @Value("${username:root}")
    private String username;

    @Value("${password:root}")
    private String password;

    @Value("${maxSize:100}")
    private int maxSize;

}
