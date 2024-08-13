package com.iotdb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
/**
 * @author tjb
 * @date 2024/8/2
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryDto {
    /**
     * 序列参数
     */
    private TimeSeriesDto timeSeriesDto;
    /**
     * 测点
     */
    private List<String> measurements;
    /**
     * 读取上限
     */
    private Long reachMaxSize;
    /**
     * 开始时间
     */
    private Long startTime;
    /**
     * 结束时间
     */
    private Long endTime;
    /**
     * 极值类型
     */
    private String type;
}
