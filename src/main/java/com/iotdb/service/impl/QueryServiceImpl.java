package com.iotdb.service.impl;

import com.iotdb.common.Constants;
import com.iotdb.common.ServiceTypeConstants;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.enums.StatusCodeEnum;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.QueryService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import com.iotdb.utils.TSDataTypeUtil;
import com.iotdb.vo.Result;
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
import java.util.concurrent.ConcurrentHashMap;

import static cn.hutool.core.text.StrPool.*;
import static com.iotdb.common.Constants.*;
import static com.iotdb.common.Constants.JOIN_CHAR;
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
    public Result<?> queryByMeasurementList(QueryDto queryDto) {
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
            String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
            String queryByLimit = new SQLBuilder()
                    .select(String.join(COMMA, queryDto.getMeasurements()))
                    .from(devicePath)
                    .orderByTimeDesc()
                    .limit(queryDto.getReachMaxSize() != null
                            && queryDto.getReachMaxSize() > NUMBER_0L ?
                               queryDto.getReachMaxSize() : NUMBER_20000L
                    )
                    .build();
            LOGGER.info("查询语句为：{}", queryByLimit.toUpperCase());

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryByLimit);
            // 以前的List<Map>结果集返回
            List<Map<String, Object>> resultList = getResultList(dataSet);
            // 换成byte[] 返回数据
            //byte[] resultByte = getResultByte(dataSet);
            dataSet.close();
            return Result.ok(resultList);
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
    public Result<?> queryByTime(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        List<String> measurements = queryDto.getMeasurements();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列为空");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
        CheckParameterUtil.checkMeasurements(measurements, true);

        try{
            // 执行
            String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
            SQLBuilder queryByTimeRangeBuilder = new SQLBuilder()
                    .select(String.join(COMMA, queryDto.getMeasurements()))
                    .from(devicePath);

            CheckParameterUtil.checkRangeTime(queryDto);
            long start = queryDto.getStartTime();
            long end = queryDto.getEndTime();
            queryByTimeRangeBuilder.where("time >=" + start + " and time <=" + end);
            LOGGER.info(queryByTimeRangeBuilder.build().toUpperCase());

            // 封装结果集
            SessionDataSetWrapper dataSet = sessionService.executeQueryStatement(queryByTimeRangeBuilder.build());
            List<Map<String, Object>> resultList = getResultList(dataSet);
            //byte[] resultByte = getResultByte(dataSet);
            dataSet.close();
            return Result.ok(resultList);
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
            String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
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
            String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
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
            String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
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
            String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
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
     * 根据map进行数据分组，然后组成单独的测点数据集，最后通过测点数据集进行字符串拼接
     * "select * from root.zh,root.zh1"
     * @param dataSetWrapper: 查询出来数据
     * @return : 返回值 byte[]
     */
    public byte[] getResultByte(SessionDataSetWrapper dataSetWrapper){
        try {
            // device-timestamp-measurement
            List<String> columnNameList = dataSetWrapper.iterator().getColumnNameList();
            List<String> selectColumnList = new ArrayList<>(columnNameList);
            selectColumnList.remove(0);
            System.out.println(selectColumnList);
            if (selectColumnList.isEmpty()){
                throw new ServiceException(StatusCodeEnum.VALID_ERROR.getCode(), "查询结果为空,或sql语句有问题");
            }
            List<String> selectColumnTypeList = dataSetWrapper.iterator().getColumnTypeList();
            selectColumnTypeList.remove(0);

            // 所有设备的路径 包含root
            List<String> devicePathList = getDevicePathList(selectColumnList);
            // 设备分区，测点分区，时间分区，数据(构建分区框架)
            Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew = new ConcurrentHashMap<>();
            LOGGER.info("构建结果分区Map");
            constructionPartitionMap(resultMapNew,devicePathList, selectColumnList);
            // 分区框架map数据导入
            LOGGER.info("填充数据给map");
            fillDataToMap(dataSetWrapper, devicePathList, selectColumnList, resultMapNew);
            // 拼接结果集
            LOGGER.info("结果集拼接");
            byte[] resultBytes = getResultBytesByResultMap(resultMapNew, selectColumnList, selectColumnTypeList);
            dataSetWrapper.close();
            return resultBytes;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            LOGGER.error(e.getMessage());
            throw new ServiceException(SYSTEM_ERROR.getCode(), "数据库异常:"+e);
        }
    }

    /**
     * 通过构建好的数据map，返回我们想要的字节拼接
     * @param resultMapNew : 分区map
     * @return ： 字节
     */
    private static byte[] getResultBytesByResultMap(
            Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew,
            List<String> column,
            List<String> columnTypes) {
        // 获取拼接头
        StringBuilder resultBuilder = getResultHeader();
        // 拿到设备分区
        resultMapNew.keySet()
                .forEach(devicePath -> {
                    // 拼接设备头，反斜杠0
                    resultBuilder
                            .append(devicePath)
                            .append("\\0\\0")
                            .append(JOIN_CHAR);
                    // 拿到设备下的测点
                    Map<String, Map<Long, List<Object>>> deviceToTestPointMap = resultMapNew.get(devicePath);
                    // 通过设备对应的分区下 获取到测点和所对应的数据
                    Object[] keysOfOneDevice = deviceToTestPointMap.keySet().toArray();

                    // 遍历测点key
                    for (int i = 0; i < keysOfOneDevice.length; i++) {
                        // 拼接测点类型
                        // 拼接 数据点位
                        resultBuilder
                                .append(i + 1)
                                .append(JOIN_CHAR);

                        // 通过 device+testPoint获取到对应的测点类型
                        String selectColumn = devicePath + keysOfOneDevice[i];
                        // 单独处理测点别名的数据类型获取
                        if (selectColumn.lastIndexOf(DOT) == Constants.NUMBER_REVERSE_1){
                            selectColumn = devicePath;
                        }
                        resultBuilder
                                .append(getTypeOfSelectColumn(column, columnTypes, selectColumn))
                                .append(JOIN_CHAR);
                        // 获取到测点下对应的时间map
                        Map<Long, List<Object>> testPointToTimeMap = deviceToTestPointMap.get(keysOfOneDevice[i]);
                        // 测点所对应的时间
                        Set<Long> times = testPointToTimeMap.keySet();
                        // 拼接数据大小头
                        resultBuilder
                                .append(times.size())
                                .append(JOIN_CHAR);

                        // 封装数据
                        times.forEach(timestamp -> {
                            // 根据对应的时间获取对应的数据
                            List<Object> dataList = testPointToTimeMap.get(timestamp);
                            // 数据不为空
                            if (!dataList.isEmpty()){
                                // 数据遍历进行拼接
                                dataList.forEach(data -> {
                                    resultBuilder.append(timestamp)
                                            .append(COLON)
                                            .append(data)
                                            .append(JOIN_CHAR);
                                });
                            }
                        });
                    }
                });
        return resultBuilder.toString().getBytes();
    }
    private static byte getTypeOfSelectColumn(List<String> columns, List<String> columnTypes, String selectColumn){
        String selectColumnType = null;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(selectColumn)){
                selectColumnType = columnTypes.get(i);
                break;
            }
        }
        return TSDataTypeUtil.getTsDataType(selectColumnType).getType();
    }

    /**
     * devicePathList, selectColumnList
     * @param dataSetWrapper : 数据源
     * @param devicePathList ：设备路径
     * @param selectColumnList ：查询的字段
     * @param resultMapNew ： 构建的结果map
     * @throws IoTDBConnectionException
     * @throws StatementExecutionException
     */
    private static void fillDataToMap(SessionDataSetWrapper dataSetWrapper,
                                      List<String> devicePathList,
                                      List<String> selectColumnList,
                                      Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew)
            throws IoTDBConnectionException, StatementExecutionException {
        while(dataSetWrapper.hasNext()){
            // 每一行数据
            RowRecord next = dataSetWrapper.next();
            // 每一行的时间
            long timestamp = next.getTimestamp();
            // 每一行对应的数据，没有时间列,但是可能是包括了其他测点的数据
            // align by device 第一列肯定是设备 + 其他列的数据
            List<Field> fields = next.getFields();
            for (int i = 0; i < devicePathList.size(); i++) {
                // 获取测点
                String measurement = getMeasurement(selectColumnList, i);
                // 通过设备路径拿到 这个设备路径下对应的所有的测点的map，又通过测点获取到测点下，每个时间点的数据集合
                resultMapNew
                        .get(devicePathList.get(i))
                        .get(measurement)
                        .computeIfAbsent(timestamp, value -> new ArrayList<>())
                        .add(fields.get(i).getDataType() != null ?
                                TSDataTypeUtil.getValueByFiled(fields.get(i))
                                : null
                        );

            }
        }
    }

    /**
     * 获取测点名称
     * @param selectColumnList : 查询的字段
     * @param i ： 当前的设备路径下标
     * @return ： 测点名称
     */
    private static String getMeasurement(List<String> selectColumnList, int i) {
        String testPoint = selectColumnList.get(i);
        int index = testPoint.lastIndexOf(DOT);
        if (index != -1){
            // 获取的是设备(但是取出来的是 包含逗号+测点名称的)
            return testPoint.substring(index);
        }else {
            return testPoint;
        }
    }

    /**
     * 构建分区map
     *
     * @param devicePathList   : 设备路径 包含root
     * @param selectColumnList ： 去掉时间列的 selectColumn 名称
     */
    private static void constructionPartitionMap(Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew,
                                                 List<String> devicePathList,
                                                 List<String> selectColumnList) {
        // 设备遍历
        devicePathList.forEach(devicePath -> {
            // 通过设备进行分组，然后将结果存储到map中去
            resultMapNew.computeIfAbsent(devicePath,value -> new ConcurrentHashMap<>());
            // 遍历测点， 通过测点的遍历，将设备和测点进行绑定在一起
            selectColumnList.forEach(selectColumn -> {
                // 获取查询字段的设备路径和遍历的设备路径进行对比
                int lastDotIndex = selectColumn.lastIndexOf(DOT);
                String selectColumnDevicePath;
                // 区分别名 字段
                if (lastDotIndex != -1){
                    // 这里判断设备名称是否相等，然后再将测点添加进去
                    selectColumnDevicePath = selectColumn.substring(0 ,lastDotIndex);
                }else {
                    // 因为是别名， 直接赋值不用进行操作
                    selectColumnDevicePath = selectColumn;
                }
                if (devicePath.equals(selectColumnDevicePath)){
                    // 进行设备的map加入
                    String key = lastDotIndex != -1 ? selectColumn.substring(lastDotIndex) : selectColumnDevicePath;
                    resultMapNew.computeIfAbsent(
                                    devicePath,
                                    k -> new HashMap<>()
                                )
                                .computeIfAbsent(
                                        key,
                                        k -> new ConcurrentHashMap<>()
                                );
                }

            });

        });
    }

    /**
     * 返回拼接头
     * @return
     */
    public static StringBuilder getResultHeader(){
        return new StringBuilder()
                .append("127.0.0.1")
                .append(JOIN_CHAR)
                .append(ServiceTypeConstants.HISTORY.getCode())
                .append(JOIN_CHAR);
    }

    /**
     * 获取到设备的路径
     * @param selectColumn ：查询的字段
     * @return ： 返回处理后的设备的路径
     */
    private static List<String> getDevicePathList(List<String> selectColumn) {
        List<String> names = new ArrayList<>(selectColumn.size());
        selectColumn.forEach(name -> {
            // 如果存在设备路径就获取到设备路径
            int i = name.lastIndexOf(DOT);
            if (i != -1){
                // 获取设备路径
                String device = name.substring(0, i);
                names.add(device);
            }else {
                names.add(name);
            }
        });
        return names;
    }






}
