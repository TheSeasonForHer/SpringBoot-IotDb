package com.iotdb.utils;

import com.iotdb.config.IoTDBProperties;
import com.iotdb.enums.StatusCodeEnum;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.impl.QueryServiceImpl;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author tjb
 * @date 2024/8/2
 */
@Configuration
public class SessionUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionUtil.class);

    boolean emptyOfHost;
    boolean emptyOfNodeUrls;
    String host;
    int port;
    List<String> nodeUrls;
    String username;
    String password;
    int maxSize;
    @Resource
    private IoTDBProperties ioTDBProperties;


    @Bean
    public SessionPool sessionPool(){
        init();
        if (emptyOfNodeUrls && emptyOfHost){
            host = "127.0.0.1";
            port = 6667;
            username = "root";
            password = "root";
            maxSize = 100;
            LOGGER.warn(String.format("iotdb参数异常，使用默认配置,主机：%s, 端口%d, 用户名%s,密码%s", host, port, username, password));
        }
        SessionPool.Builder sessionPoolBuilder = new SessionPool.Builder();
        sessionPoolBuilder
                .user(username)
                .password(password)
                .maxRetryCount(3)
                .maxSize(maxSize);
        if (emptyOfNodeUrls){
            LOGGER.info(String.format("the nodeUrls connect to iotdb is null, use the host(%s:%d) connect to iotdb", host,port));
            sessionPoolBuilder
                    .host(host)
                    .port(port);
            return sessionPoolBuilder
                    .build();
        }
        LOGGER.info(String.format("the host connect to iotdb is null, use the nodeUrls(%s) connect to iotdb", nodeUrls));
        return sessionPoolBuilder
                .nodeUrls(nodeUrls)
                .build();
    }
    public void init(){
        emptyOfHost = ioTDBProperties.isEmptyOfHost();
        emptyOfNodeUrls = ioTDBProperties.isEmptyOfNodeUrls();
        host = ioTDBProperties.getHost();
        port = ioTDBProperties.getPort();
        nodeUrls = ioTDBProperties.getNodeUrls();
        username = ioTDBProperties.getUsername();
        password = ioTDBProperties.getPassword();
        maxSize = ioTDBProperties.getMaxSize();
    }
}
