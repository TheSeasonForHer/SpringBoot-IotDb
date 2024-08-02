package com.iotdb.utils;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.springframework.stereotype.Component;

@Component
public class SessionUtil {
    private static SessionPool session = null;
    private SessionUtil(){}
    public SessionPool getConnection(){
        if (session == null){
            synchronized (SessionUtil.class){
                session = new SessionPool.Builder()
                        .host("172.20.31.111")
                        .port(6667)
                        .user("root")
                        .password("root")
                        .maxSize(1000)
                        .build();
            }
        }
        return session;
    }
}
