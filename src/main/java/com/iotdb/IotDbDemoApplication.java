package com.iotdb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class IotDbDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(IotDbDemoApplication.class, args);
    }

}
