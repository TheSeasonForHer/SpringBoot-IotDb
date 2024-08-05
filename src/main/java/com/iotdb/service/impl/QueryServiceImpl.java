package com.iotdb.service.impl;

import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.QueryService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import com.iotdb.utils.TSDataTypeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.*;
import static com.iotdb.common.Constants.NUMBER_0L;
import static com.iotdb.common.Constants.NUMBER_20000L;
import static com.iotdb.enums.StatusCodeEnum.SYSTEM_ERROR;
import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;
/**
 * @author tjb
 * @date 2024/8/2
 */
@Service
public class QueryServiceImpl implements QueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImpl.class);

    @Resource
    private SessionPool sessionService;

    @Override
    public List<Map<String, Object>>  queryByMeasurementList(QueryDto queryDto) {
        // 参数校验
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        List<String> measurements = queryDto.getMeasurements();
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, false);

        try{
            // 执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            String queryByLimit = new SQLBuilder()
                    .select(String.join(",", queryDto.getMeasurements()))
                    .from(devicePath)
                    .limit(queryDto.getReachMaxSize() != null
                            && queryDto.getReachMaxSize() > NUMBER_0L ?
                               queryDto.getReachMaxSize() : NUMBER_20000L
                    )
                    .build();
            LOGGER.info(queryByLimit.toUpperCase());

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryByLimit);
            List<Map<String, Object>> resultList = getResultList(dataSet);
            dataSet.close();
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(),e.getMessage());
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
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, true);

        try{
            // 执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            SQLBuilder queryByTimeRangeBuilder = new SQLBuilder()
                    .select(String.join(",", queryDto.getMeasurements()))
                    .from(devicePath);

            CheckParameterUtil.checkRangeTime(queryDto);
            long start = Long.parseLong(queryDto.getStartTime());
            long end = Long.parseLong(queryDto.getEndTime());
            queryByTimeRangeBuilder.where("time >=" + start + " and time <=" + end);
            LOGGER.info(queryByTimeRangeBuilder.build().toUpperCase());

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryByTimeRangeBuilder.build());
            List<Map<String, Object>> resultList = getResultList(dataSet);
            dataSet.close();
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }

    /**
     * 根据测点查询数据的起始时间
     * MAX_TIME	求最大时间戳。
     * MIN_TIME	求最小时间戳。
     * 返回两个时间
     */
    @Override
    public List<Map<String, Object>> queryTimeRangeByMeasurement(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, true);

        try{
            // 执行 select MAX_TIME(AV) as maxTime,MIN_TIME(AV) as minTime from root.sg.unit0.historyA.d3
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            String testPoint = queryDto.getMeasurements().get(0);
            if (StringUtils.isBlank(testPoint) || StringUtils.isEmpty(testPoint)){
                throw new ServiceException(VALID_ERROR.getCode(), "测点不能为空");
            }
            String  queryMinAMaxTime = new SQLBuilder()
                    .selectAggregation(String.format("MAX_TIME(%s), MIN_TIME(%s)", testPoint, testPoint))
                    .from(devicePath)
                    .build();
            LOGGER.info(queryMinAMaxTime);

            //封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryMinAMaxTime);
            List<Map<String, Object>> resultList = getResultListAggregation(dataSet);
            dataSet.close();
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }

    /**
     * 根据极值类型获取测点数据的极值
     *
     * @param queryDto ：设备号、测点号、极值类型（MAX_VALUE,MIN_VALUE）
     */
    @Override
    public List<Map<String, Object>> queryByExtremeType(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, true);

        try{
            // 执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            String queryExtremeSql = getQueryExtremeSql(queryDto, devicePath);

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryExtremeSql);
            List<Map<String, Object>> resultList = getResultListAggregation(dataSet);
            dataSet.close();
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }

    /**
     * 获取极值的sql
     * @param queryDto : 查询数据
     * @param devicePath : 查询路径
     * @return : 返回sql
     */
    private static String getQueryExtremeSql(QueryDto queryDto, String devicePath) {
        String selectType = queryDto.getType();
        if (StringUtils.isEmpty(selectType) || StringUtils.isBlank(selectType)){
            throw new ServiceException(VALID_ERROR.getCode(), "极值类型参数错误");
        }

        String queryExtremeSql;
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
                throw new ServiceException(SYSTEM_ERROR.getCode(),"极值类型参数不合法");
        }
        return queryExtremeSql;
    }

    /**
     * 获取测点数据的总量
     * @param queryDto ：设备号，测点， 时间范围（可选）
     */
    @Override
    public List<Map<String, Object>> queryDataCountByMeasurement(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, true);

        try{
            //执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            SQLBuilder queryCountByTime = new SQLBuilder()
                    .select("count(" + queryDto.getMeasurements().get(0) + ")")
                    .from(devicePath);

            /**
             * 为了实现时间的可选，我们这里使用异常抛出的方式来进行判断，
             * 如果抛出了异常，我们就查询所有数据
             * 没有抛出异常，就根据时间范围查询数据
             */
            try {
                CheckParameterUtil.checkRangeTime(queryDto);
                long start = Long.parseLong(queryDto.getStartTime());
                long end = Long.parseLong(queryDto.getEndTime());
                queryCountByTime.where("time >=" + start + " and time <=" + end);
                LOGGER.info(queryCountByTime.build().toUpperCase());
            }catch (ServiceException serviceException){
                queryCountByTime.where("time >=" + Long.MIN_VALUE + " and time <=" + Long.MAX_VALUE);
                LOGGER.info(queryCountByTime.build().toUpperCase());
            }

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryCountByTime.build());
            List<Map<String, Object>> resultList = getResultListAggregation(dataSet);
            dataSet.close();
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }

    /**
     * 获取测点有数据的时间段 group by session
     */
    @Override
    public List<Map<String, Object>> queryDataGroupBySession(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, true);

        try{
            // 执行
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();
            SQLBuilder queryGroupBySession = new SQLBuilder()
                    .select("__endTime", String.format("count(%s)", measurements.get(0)))
                    .from(devicePath)
                    .groupBy(String.format("session(%s)", "1d"));

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryGroupBySession.build());
            List<Map<String, Object>> resultList = getResultList(dataSet);
            dataSet.close();
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }

    /**
     * 返回聚合类查询的结果
     * @param dataSet : 返回封装好的List<Map<String, value>>
     */
    //TODO:把返回的改为对象map
    private static List<Map<String, Object>> getResultListAggregation(SessionDataSetWrapper dataSet) {
        List<Map<String, Object>> resultList = new LinkedList<>();
        List<String> columnNames = dataSet.getColumnNames();

        try {
            while (dataSet.hasNext()){
                RowRecord record = dataSet.next();
                Map<String, Object> map = new HashMap<>();
                getDataByRecord(record, columnNames, map);
                resultList.add(map);
            }
            return resultList;

        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }
    /**
     * 获取结果集
     * @param dataSet : 结果集数据
     * @return ： 返回封装好的List<Map<String, value>>
     */
    private static List<Map<String, Object>> getResultList(SessionDataSetWrapper dataSet){
        List<Map<String, Object>> resultList = new LinkedList<>();
        List<String> columnNames = dataSet.getColumnNames();
        //移除时间列，因为结果集中返回的是测点的数据，不包含时间
        columnNames.remove(0);
        try{
            while (dataSet.hasNext()){
                RowRecord record = dataSet.next();
                Map<String, Object> map = new HashMap<>();
                map.put("time", record.getTimestamp());
                getDataByRecord(record, columnNames, map);
                resultList.add(map);
            }
            return resultList;
        }catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new ServiceException(SYSTEM_ERROR.getCode(), e.getMessage());
        }
    }

    private static void getDataByRecord(RowRecord record, List<String> columnNames, Map<String, Object> map) {
        List<Field> fields = record.getFields();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i), fields.get(i).getDataType() != null ? TSDataTypeUtil.getValueByFiled(fields.get(i)) : null);
        }
    }
}
