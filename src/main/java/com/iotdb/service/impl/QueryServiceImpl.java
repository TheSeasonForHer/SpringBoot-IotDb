package com.iotdb.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import com.iotdb.common.Constants;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.QueryService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import com.iotdb.utils.SessionUtil;
import com.iotdb.utils.TSDataTypeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Service
public class QueryServiceImpl implements QueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImpl.class);

    @Resource
    private SessionUtil sessionUtil;
    private static ExecutorService service;
    @Override
    public List<Map<String, Object>>  queryByMeasurementList(QueryDto queryDto) {
        //参数校验
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (!CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto) || CollectionUtil.isEmpty(measurements)) {
            throw new ServiceException(Constants.CODE_500,"please check input parameter of " + timeSeriesDto);
        }
        try{
            //执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            String queryByLimit = new SQLBuilder()
                    .select(queryDto.getMeasurements().stream().collect(Collectors.joining(",")))
                    .from(devicePath)
                    .limit(queryDto.getReachMaxSize() != null && queryDto.getReachMaxSize() > 0  ? queryDto.getReachMaxSize() : 20000)
                    .build();
            SessionDataSetWrapper dataSet = sessionUtil.getConnection().executeQueryStatement(queryByLimit);
            //封装结果集
            return getResultList(dataSet);
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据时间范围查询数据
     *
     * @param queryDto : 查询数据
     *                 返回测点数据列表
     */
    @Override
    public List<Map<String, Object>> queryByTime(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (!CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto) || CollectionUtil.isEmpty(measurements)) {
            throw new ServiceException(Constants.CODE_500,"please check input parameter of " + timeSeriesDto);
        }
        try{
            //执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            SQLBuilder queryByTimeRangeBuilder = new SQLBuilder()
                    .select(queryDto.getMeasurements().stream().collect(Collectors.joining(",")))
                    .from(devicePath);
            String startTime = queryDto.getStartTime();
            String endTime = queryDto.getEndTime();
            if (StringUtils.isBlank(startTime) || StringUtils.isBlank(endTime)){
                throw new ServiceException(Constants.CODE_500, "查询时间不能为空");
            }
            long start = Long.parseLong(startTime);
            long end = Long.parseLong(endTime);
            if (start >= end){
                throw new ServiceException(Constants.CODE_500, "查询开始时间不能大于结束时间");
            }
            queryByTimeRangeBuilder.where("time >=" + start + " and time <=" + end);
            SessionDataSetWrapper dataSet = sessionUtil.getConnection().executeQueryStatement(queryByTimeRangeBuilder.build());
            //封装结果集
            return getResultList(dataSet);
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据测点查询数据的起始时间
     * MAX_TIME	求最大时间戳。
     * MIN_TIME	求最小时间戳。
     * 返回两个时间
     *
     * @param queryDto
     */
    //TODO:检查参数校验
    @Override
    public List<Map<String, Object>> queryTimeRangeByMeasurement(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (!CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto) || CollectionUtil.isEmpty(measurements)) {
            throw new ServiceException(Constants.CODE_500,"please check input parameter of " + timeSeriesDto + " or the measurements size lt zero");
        }
        try{
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            //执行 select MAX_TIME(AV) as maxTime,MIN_TIME(AV) as minTime from root.sg.unit0.historyA.d3
            String testPoint = queryDto.getMeasurements().get(0);
            if (StringUtils.isBlank(testPoint) || StringUtils.isEmpty(testPoint)){
                throw new ServiceException(Constants.CODE_500, "测点不能为空");
            }
            String  queryMinAMaxTime = new SQLBuilder()
                    .selectAggregation(String.format("MAX_TIME(%s), MIN_TIME(%s)", testPoint, testPoint))
                    .from(devicePath)
                    .build();
            SessionDataSetWrapper dataSet = sessionUtil.getConnection().executeQueryStatement(queryMinAMaxTime);
            //封装结果集
            return getResultListAggregation(dataSet);
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(Constants.CODE_500, e.getCause().getMessage());
        }
    }

    /**
     * 根据极值类型获取测点数据的极值
     *
     * @param queryDto
     */
    @Override
    public List<Map<String, Object>> queryByExtremeType(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (!CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto) || CollectionUtil.isEmpty(measurements)) {
            throw new ServiceException(Constants.CODE_500,"please check input parameter of " + timeSeriesDto + " or the measurements size lt zero");
        }
        try{
            //执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            String selectType = null;
            if (StringUtils.isEmpty(queryDto.getType()) || StringUtils.isBlank(queryDto.getType())){
                throw new ServiceException(Constants.CODE_500, "极值类型参数错误");
            }
            selectType = queryDto.getType();
            String queryExtremeSql = null;
            switch (selectType){
                case "MAX_VALUE":
                    queryExtremeSql = new SQLBuilder()
                            .select("MAX_VALUE(" + queryDto.getMeasurements().get(0) + ")")
                            .from(devicePath)
                            .build();
                    break;
                case "MIN_VALUE":
                    queryExtremeSql = new SQLBuilder()
                            .select("MIN_VALUE(" + queryDto.getMeasurements().get(0) + ")")
                            .from(devicePath)
                            .build();
                    break;
                default:
                    throw new ServiceException(Constants.CODE_500,"the parameter select type is valid:" + selectType + ". the parameter should MAX_VALUE or MIN_VALUE");
            }
            SessionDataSetWrapper dataSet = sessionUtil.getConnection().executeQueryStatement(queryExtremeSql);
            //封装结果集
            return getResultListAggregation(dataSet);
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 获取测点数据的总量
     * 返回数据总量
     *
     * @param queryDto
     */
    @Override
    public List<Map<String, Object>> queryDataCountByMeasurement(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (!CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto) || CollectionUtil.isEmpty(measurements)) {
            throw new ServiceException(Constants.CODE_500,"please check input parameter of " + timeSeriesDto + " or the measurements size lt zero");
        }
        try{
            //执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            SQLBuilder queryCountByTime = new SQLBuilder()
                    .select("count(" + queryDto.getMeasurements().get(0) + ")")
                    .from(devicePath);
            String startTime = queryDto.getStartTime();
            String endTime = queryDto.getEndTime();
            if (StringUtils.isBlank(startTime) || StringUtils.isBlank(endTime)){
                throw new ServiceException(Constants.CODE_500, "查询时间不能为空");
            }
            long start = Long.parseLong(startTime);
            long end = Long.parseLong(endTime);
            if (start >= end){
                throw new ServiceException(Constants.CODE_500, "查询开始时间不能大于结束时间");
            }
            queryCountByTime.where("time >=" + start + " and time <=" + end);
            SessionDataSetWrapper dataSet = sessionUtil.getConnection().executeQueryStatement(queryCountByTime.build());
            //封装结果集
            return getResultListAggregation(dataSet);
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取测点有数据的时间段 group by session
     */
    @Override
    public List<Map<String, Object>> queryDataGroupBySession(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (!CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto) || CollectionUtil.isEmpty(measurements)) {
            throw new ServiceException(Constants.CODE_500,"please check input parameter of " + timeSeriesDto + " or the measurements size lt zero");
        }
        try{
            //执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            SQLBuilder queryGroupBySession = new SQLBuilder()
                    .select("__endTime")
                    .from(devicePath)
                    .groupBy(String.format("session(%s)", "1d"));
            SessionDataSetWrapper dataSet = sessionUtil.getConnection().executeQueryStatement(queryGroupBySession.build());
            //封装结果集
            return getResultListAggregation(dataSet);
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 返回聚合类查询的结果
     * @param dataSet
     * @return
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    //TODO:把返回的改为对象map
    private static List<Map<String, Object>> getResultListAggregation(SessionDataSetWrapper dataSet) throws IoTDBConnectionException, StatementExecutionException {
        List<Map<String, Object>> resultList = new LinkedList<>();
        List<String> columnNames = dataSet.getColumnNames();
        while (dataSet.hasNext()){
            RowRecord record = dataSet.next();
            Map<String, Object> map = new HashMap<>();
            List<Field> fields = record.getFields();
            for (int i = 0; i < columnNames.size(); i++) {
                map.put(columnNames.get(i), fields.get(i).getDataType() != null ? TSDataTypeUtil.getValueByFiled(fields.get(i)) : null);
            }
            resultList.add(map);
        }
        return resultList;
    }
    /**
     * 获取结果集
     * @param dataSet : 结果集数据
     * @return ： 返回封装好的List<Map<String, value>>
     * @throws StatementExecutionException
     * @throws IoTDBConnectionException
     */
    //TODO:把返回的改为对象map
    private static List<Map<String, Object>> getResultList(SessionDataSetWrapper dataSet) throws StatementExecutionException, IoTDBConnectionException {
        List<Map<String, Object>> resultList = new LinkedList<>();
        List<String> columnNames = dataSet.getColumnNames();
        while (dataSet.hasNext()){
            RowRecord record = dataSet.next();
            Map<String, Object> map = new HashMap<>();
            map.put("timestamp", record.getTimestamp());
            List<Field> fields = record.getFields();
            for (int i = 0; i < columnNames.size() - 1; i++) {
                map.put(columnNames.get(i+1), fields.get(i).getDataType() != null ? TSDataTypeUtil.getValueByFiled(fields.get(i)) : null);
            }
            resultList.add(map);
        }
        return resultList;
    }
}
