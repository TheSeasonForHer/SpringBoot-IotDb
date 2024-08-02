package com.iotdb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

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
    private String startTime;
    /**
     * 结束时间
     */
    private String endTime;
    /**
     * 极值类型
     */
    private String type;
}
