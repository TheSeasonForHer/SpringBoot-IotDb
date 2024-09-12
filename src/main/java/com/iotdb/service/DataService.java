package com.iotdb.service;

import com.iotdb.dto.DataDto;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.vo.Result;
import org.springframework.web.bind.annotation.RequestBody;

import java.util.List;

/**
 * @author tjb
 * @date 2024/8/2
 */
public interface DataService {
    /**
     * 根据测点插入数据
     * @param dataDto
     * @return
     */
    public boolean insertRecordByTimeSeries(DataDto dataDto);

    /**
     * 删除测点数据
     * @return ；成功或失败
     */
    public boolean deleteDataByTimeRange(QueryDto queryDto);

    public boolean insertRecordByBatchTimeSeries(List<TimeSeriesDto> timeSeriesDtos, List<List<DataDto.Data>> dataDtoList);
}
