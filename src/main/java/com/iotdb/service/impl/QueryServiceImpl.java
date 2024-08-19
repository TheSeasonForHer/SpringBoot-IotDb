package com.iotdb.service.impl;

import com.iotdb.common.ServiceTypeConstants;
import com.iotdb.config.IoTDBProperties;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.QueryService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import com.iotdb.utils.TSDataTypeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.isession.SessionDataSet;
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
import java.util.concurrent.ConcurrentHashMap;

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
    @Resource
    private IoTDBProperties properties;

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
            long start = queryDto.getStartTime();
            long end = queryDto.getEndTime();
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
                long start = queryDto.getStartTime();
                long end = queryDto.getEndTime();
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
            LOGGER.info(queryGroupBySession.build().toUpperCase());

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryGroupBySession.build());
            // 这里也有聚合查询的 count(measurement)， 为什么不用聚合获取结果的方法？因为有时间列，聚合查询获取结果是会下标越界的
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
        // 移除时间列，因为结果集中返回的是测点的数据，不包含时间
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

    // 注意：通过getFields获取的数据列，不包含时间列的值
    private static void getDataByRecord(RowRecord record, List<String> columnNames, Map<String, Object> map) {
        List<Field> fields = record.getFields();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(columnNames.get(i), fields.get(i).getDataType() != null ? TSDataTypeUtil.getValueByFiled(fields.get(i)) : null);
        }
    }

    /**
     * TODO:
     * 根据map进行数据分组，然后组成单独的测点数据集，最后通过测点数据集进行字符串拼接
     * "select * from root.zh,root.zh1"
     * @param dataSetWrapper: 查询出来数据
     * @return : 返回值 byte[]
     */
    public byte[] getResulWithByteList(SessionDataSetWrapper dataSetWrapper, List<String> measurements){
        try {
            // 去除时间列和时间列的类型
            List<String> columnNameList = dataSetWrapper.iterator().getColumnNameList();
            List<String> deviceName = new ArrayList<>(columnNameList);
            deviceName.remove(0);
            if (columnNameList.size() <= 1){
                return null;
            }
            List<String> columnTypeList = dataSetWrapper.iterator().getColumnTypeList();
            columnTypeList.remove(0);

            // 所有设备的名称
            List<String> deviceNames = getDeviceName(deviceName);
            // 设备分区，测点分区，时间分区，数据(构建分区框架)
            Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew = getStringMapMap(deviceNames, deviceName);
            // 获取到字符串拼接数据
            StringBuilder resultDataWithString = getResultDataWithString(dataSetWrapper, deviceNames, deviceName, resultMapNew);
            System.out.println(resultMapNew);
            System.out.println(resultDataWithString);
            dataSetWrapper.close();
            assert resultDataWithString != null;
            return resultDataWithString.toString().getBytes();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 返回拼接头
     * @return
     */
    public StringBuilder getResultHeader(){
        return new StringBuilder()
                .append(properties.getHost())
                .append(ServiceTypeConstants.HISTORY.getCode());
    }
    private static StringBuilder getResultDataWithString(SessionDataSetWrapper dataSetWrapper, List<String> deviceNames, List<String> deviceName, Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew) throws IoTDBConnectionException, StatementExecutionException {
        while(dataSetWrapper.hasNext()){
            // 每一行数据
            RowRecord next = dataSetWrapper.next();
            // 每一行的时间
            long timestamp = next.getTimestamp();
            // 每一行对应的数据，没有时间列,但是可能是包括了其他测点的数据
            // align by device 第一列肯定是设备 + 其他列的数据
            List<Field> fields = next.getFields();
            for (int i = 0; i < deviceNames.size(); i++) {
                // 获取测点
                String[] testPointSplit = deviceName.get(i).split("\\.");
                String testPoint = testPointSplit[testPointSplit.length - 1];
                resultMapNew.get(deviceNames.get(i))
                        .get(testPoint)
                        .computeIfAbsent(timestamp, value -> new ArrayList<>())
                        .add(fields.get(i).getDataType() != null ?
                                TSDataTypeUtil.getValueByFiled(fields.get(i))
                                : null
                        );

            }
            System.out.println("data"+ next);
        }
        return null;
    }

    private static Map<String, Map<String, Map<Long, List<Object>>>> getStringMapMap(List<String> deviceNames, List<String> deviceName) {
        Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew = new ConcurrentHashMap<>();
        // 设备遍历
        deviceNames.forEach(name -> {
            // 通过设备进行分组，然后将结果存储到map中去
            resultMapNew.computeIfAbsent(name,value -> new ConcurrentHashMap<>());
            // 遍历测点， 通过测点的遍历，将设备和测点进行绑定在一起
            deviceName.forEach(testPoint -> {
                String[] split = testPoint.split("\\.");
                // 这里判断设备名称是否相等，然后再将测点添加进去
                if (name.equals(split[split.length - 2])){
                    resultMapNew.get(name).computeIfAbsent(split[split.length - 1], value -> new ConcurrentHashMap<>());
                }
            });

        });
        return resultMapNew;
    }


    private static List<String> getDeviceName(List<String> deviceName) {
        List<String> names = new ArrayList<>(deviceName.size());
        deviceName.forEach(name -> {
            String[] deviceString = name.split("\\.");
            String device = deviceString[deviceString.length - 2];
            names.add(device);
        });
        return names;
    }
}
