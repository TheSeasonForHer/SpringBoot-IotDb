package com.iotdb.service;

import com.iotdb.dto.DataDto;
import com.iotdb.dto.QueryDto;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
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
}
