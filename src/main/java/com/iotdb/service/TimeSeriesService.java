package com.iotdb.service;

import com.iotdb.dto.TimeSeriesDto;

import java.util.List;
/**
 * @author tjb
 * @date 2024/8/2
 */
public interface TimeSeriesService {
    /**
     * 创建时间序列
     * @param timeSeriesDto
     * @return
     */
    public List<String> createTimeSeries(List<TimeSeriesDto> timeSeriesDto);

}
