package com.iotdb.utils;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import com.iotdb.dto.QueryDto;
import com.iotdb.service.impl.ExportServiceImpl;
import com.iotdb.service.impl.QueryServiceImpl;
import com.iotdb.vo.Result;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.concurrent.*;

/**
 * @Author tjb
 * @Date 2024/9/13 16:10
 * 自定义线程池
 */
public class ThreadPool {
    @Getter
    private static final ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(8, 16,
            60, TimeUnit.MINUTES,
            new ArrayBlockingQueue<>(5000),
            new ThreadFactoryBuilder().setNamePrefix("http-nio-7779-exec-task").build(),
            new DiscardPolicy());

    /**
     * 自定义线程池拒绝策略
     */
    private static class DiscardPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            System.out.println("超过最大队列长度%s"+  r.toString());
        }
    }



}
