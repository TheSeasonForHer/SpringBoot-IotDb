package com.iotdb.config;

import cn.hutool.core.collection.CollectionUtil;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author tjb
 * @date 2024/8/2
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "iotdb")
public class IoTDBProperties {

    //@Value("${host}")
    private String host;

    //@Value("${port:6667}")
    private int port;

    //@Value("${username:root}")
    private String username;

    //@Value("${password:root}")
    private String password;

    //@Value("${maxSize:100}")
    private int maxSize;


    //@Value("${nodeUrls}")
    private List<String> nodeUrls;

    public boolean isEmptyOfHost() {
        return StringUtils.isEmpty(host) || StringUtils.isBlank(host);
    }
    public boolean isEmptyOfNodeUrls() {
        return ObjectUtils.isEmpty(nodeUrls) || CollectionUtil.isEmpty(nodeUrls);
    }
}
