package com.iotdb.utils;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import com.iotdb.dto.QueryDto;
import com.iotdb.enums.StatusCodeEnum;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.impl.ExportServiceImpl;
import com.iotdb.service.impl.QueryServiceImpl;
import com.iotdb.vo.Result;
import lombok.Builder;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.*;

import static com.iotdb.enums.StatusCodeEnum.FAIL;

/**
 * @Author tjb
 * @Date 2024/9/13 16:10
 * 自定义线程池
 */
@Component
public class ThreadPool {
    @Getter
    private static final ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(8, 16,
            60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1000),
            new ThreadFactoryBuilder().setNamePrefix("http-nio-7779-exec-export-task").build(),
            new DiscardPolicy());

    /**
     * 自定义线程池拒绝策略
     */
    private static class DiscardPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            throw new ServiceException(
                    FAIL.getCode(),
                    String.format("任务提交失败,正在排队中,排队人数是%d, 请稍后重试",
                            executor.getActiveCount()
                    )
            );
        }
    }



}
