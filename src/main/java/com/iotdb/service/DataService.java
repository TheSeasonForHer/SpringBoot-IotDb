package com.iotdb.service;

import com.iotdb.dto.DataDto;
import com.iotdb.dto.DataDtos;
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

    /**
     * 批量插入数据（1个设备，多个测点）
       @param dataDtos  ： 插入的数据信息
     * @return
     */
    public boolean insertRecordByBatchTimeSeries(DataDtos dataDtos);
}
