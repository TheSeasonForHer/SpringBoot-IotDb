package com.iotdb.service;

import com.iotdb.dto.QueryDto;
import com.iotdb.vo.Result;

import java.util.List;
import java.util.Map;
/**
 * @author tjb
 * @date 2024/8/2
 */
public interface QueryService {
    /**
     * 根据测点列表查询数据
     * @param queryDto
     */
    public Result<?> queryByMeasurementList(QueryDto queryDto);

    /**
     * 根据时间范围查询数据
     * @param queryDto : 查询数据
     * 返回测点数据列表
     */
    public Result<?> queryByTime(QueryDto queryDto);
    /**
     * 根据测点查询数据的起始时间
     * MAX_TIME	求最大时间戳。
     * MIN_TIME	求最小时间戳。
     * 返回两个时间
     */
    public List<Map<String, Object>> queryTimeRangeByMeasurement(QueryDto queryDto);
    /**
     * 根据极值类型获取测点数据的极值
     */
    public List<Map<String, Object>> queryByExtremeType(QueryDto queryDto);
    /**
     * 获取测点数据的总量
     * 返回数据总量
     */
    public List<Map<String, Object>> queryDataCountByMeasurement(QueryDto queryDto);
    /**
     * 获取测点有数据的时间段
     */
    public List<Map<String, Object>> queryDataGroupBySession(QueryDto queryDto);


}
