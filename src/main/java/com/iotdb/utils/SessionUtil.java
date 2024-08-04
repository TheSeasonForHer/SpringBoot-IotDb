package com.iotdb.utils;

import com.iotdb.config.IoTDBProperties;
import com.iotdb.service.impl.QueryServiceImpl;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
/**
 * @author tjb
 * @date 2024/8/2
 */
@Configuration
public class SessionUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImpl.class);
    @Resource
    IoTDBProperties ioTDBProperties;

    @Bean
    public SessionPool sessionPool(){
        LOGGER.info(ioTDBProperties.getHost());
        LOGGER.info(String.valueOf(ioTDBProperties.getPort()));
        LOGGER.info(ioTDBProperties.getUsername());
        LOGGER.info(ioTDBProperties.getPassword());
        LOGGER.info(String.valueOf(ioTDBProperties.getMaxSize()));
        return new SessionPool.Builder()
                .host(ioTDBProperties.getHost())
                .port(ioTDBProperties.getPort())
                .user(ioTDBProperties.getUsername())
                .password(ioTDBProperties.getPassword())
                .maxSize(ioTDBProperties.getMaxSize())
                .build();
    }
}
