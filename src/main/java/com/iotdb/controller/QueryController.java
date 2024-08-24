package com.iotdb.controller;

import com.iotdb.common.Constants;
import com.iotdb.common.ServiceTypeConstants;
import com.iotdb.config.IoTDBProperties;
import com.iotdb.enums.StatusCodeEnum;
import com.iotdb.exception.ServiceException;
import com.iotdb.utils.TSDataTypeUtil;
import com.iotdb.vo.Result;
import com.iotdb.dto.QueryDto;
import com.iotdb.service.QueryService;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.swing.text.TabExpander;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static cn.hutool.core.text.StrPool.*;
import static com.iotdb.common.Constants.JOIN_CHAR;

/**
 * @author tjb
 * @date 2024/8/2
 */
@RestController
@RequestMapping("/query")
public class QueryController {
    @Resource
    private QueryService queryService;

    /**
     * 根据测点查询数据
     * @param queryDto : 设备号、测点列表、读取上限(可选)
     * @return 数据列表
     */
    @PostMapping("/queryByMeasurementList")
    public Result<?> getDataByMeasurementList(@RequestBody QueryDto queryDto) {
        return Result.ok(queryService.queryByMeasurementList(queryDto));
    }

    /**
     * 根据时间范围查询数据
     * @param queryDto : 设备号、测点号、时间范围（必须）
     * @return : 测点数据列表List<map>
     */
    @PostMapping("/queryByTimeRange")
    public Result<?> getDataByTimeRange(@RequestBody QueryDto queryDto){
        return Result.ok(queryService.queryByTime(queryDto));
    }
    /**
     * 获取测点的最早时间和最晚时间
     * @param queryDto : 设备号、测点号
     * @return : 数据列表，MAX_TIME/MIN_TIME
     */
    @PostMapping("/queryStartTimeAndEndTime")
    public Result<?> getDataStartAndEndTime(@RequestBody QueryDto queryDto){
        return Result.ok(queryService.queryTimeRangeByMeasurement(queryDto));
    }
    /**
     * 根据极值类型返回对应的极值
     * @param queryDto : 设备号、测点号、极值类型（MAX_VALUE,MIN_VALUE）
     * @return : 数据列表，MAX_VALUE/MIN_VALUE
     */
    @PostMapping("/queryByExtremeType")
    public Result<?> getDataByExtremeType(@RequestBody QueryDto queryDto){
        return Result.ok(queryService.queryByExtremeType(queryDto));
    }

    /**
     * 根据测点返回该测点的数据总量
     * @param queryDto : 设备号，测点， 时间范围（可选）
     * @return
     */
    @PostMapping("/getDataCountByMeasurement")
    public Result<?> getDataCountByMeasurement(@RequestBody QueryDto queryDto){
        return Result.ok(queryService.queryDataCountByMeasurement(queryDto));
    }
    /**
     * 获取测点有数据的时间段
     * 设备，测点
     */
    @PostMapping("/groupBySession")
    public Result<?> getDataGroupBySession(@RequestBody QueryDto queryDto){
        return Result.ok(queryService.queryDataGroupBySession(queryDto));
    }

    //TODO:首先将root进行提出来，然后需要将从root到设备进行提取出来进行map，最后自己将设备在拼接的时候分割，最后将测点通过root 设备拼接一条完整的路径，通过column进行匹配下标，通过下标访问类型数组
    @Resource
    private SessionPool sessionPool;
    @Resource
    private IoTDBProperties properties;
    @GetMapping("/")
    public Result<?> get(){
        try {
            SessionDataSetWrapper dataSetWrapper = sessionPool.executeQueryStatement("select * from root.zh,root.zh1");
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
            Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew = contructionParititionMap(devicePathList, selectColumnList);
            // 分区框架map数据导入
            fillDataToMap(dataSetWrapper, devicePathList, selectColumnList, resultMapNew);
            // 拼接结果集
            byte[] resultBytes = getResultBytesByResultMap(resultMapNew, selectColumnList, selectColumnTypeList);
            dataSetWrapper.close();
            return Result.ok(resultBytes);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
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
        StringBuilder resultHeader = getResultHeader();
        // 拿到设备分区
        resultMapNew.keySet()
                .forEach(devicePath -> {
                    // 拼接设备头，反斜杠0
                    resultHeader.append("设备")
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
                        System.out.println(devicePath + keysOfOneDevice[i]);
                        // 拼接 数据点位
                        resultHeader.append("点位ID")
                                    .append(i + 1)
                                    .append(JOIN_CHAR);

                        // 通过 device+testPoint获取到对应的测点类型
                        String selectColumn = devicePath + DOT + keysOfOneDevice[i];
                        resultHeader
                                .append("点位数据类型")
                                .append(getTypeOfSelectColumn(column, columnTypes, selectColumn))
                                .append(JOIN_CHAR);
                        // 获取到测点下对应的时间map
                        Map<Long, List<Object>> testPointToTimeMap = deviceToTestPointMap.get(keysOfOneDevice[i]);
                        // 测点所对应的时间
                        Set<Long> times = testPointToTimeMap.keySet();
                        // 拼接数据大小头
                        resultHeader
                                .append("数据大小")
                                .append(times.size())
                                .append(JOIN_CHAR);
                        times.forEach(timestamp -> {
                            // 根据对应的时间获取对应的数据
                            List<Object> dataList = testPointToTimeMap.get(timestamp);
                            // 数据不为空
                            if (!dataList.isEmpty()){
                                // 数据遍历进行拼接
                                dataList.forEach(data -> {
                                    resultHeader.append(timestamp)
                                                .append(COLON)
                                                .append(data)
                                                .append(JOIN_CHAR);
                                });
                            }
                        });
                    }
        });
        return resultHeader.toString().getBytes();
    }
    private static byte getTypeOfSelectColumn(List<String> columns, List<String> columnTypes, String selectColumn){
        String selectColumnType = null;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(selectColumn)){
                selectColumnType = columnTypes.get(i);
                System.out.println(selectColumnType);
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
            // 获取的是设备 这里加1
            return testPoint.substring(index + 1);
        }else {
            return testPoint;
        }
    }

    /**
     * 构建分区map
     * @param devicePathList : 设备路径 包含root
     * @param selectColumnList ： 去掉时间列的 selectColumn 名称
     * @return : 返回一个构建好的设备-》测点-》时间-》数据map
     */
    private static Map<String, Map<String, Map<Long, List<Object>>>> contructionParititionMap(List<String> devicePathList,
                                                                                              List<String> selectColumnList) {
        Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew = new ConcurrentHashMap<>();
        // 设备遍历
        devicePathList.forEach(devicePath -> {
            // 通过设备进行分组，然后将结果存储到map中去
            resultMapNew.computeIfAbsent(devicePath,value -> new ConcurrentHashMap<>());
            // 遍历测点， 通过测点的遍历，将设备和测点进行绑定在一起
            selectColumnList.forEach(selectColumn -> {
                // 获取查询字段的设备路径和遍历的设备路径进行对比
                // TODO:抽取出一个方法来
                int lastDotIndex = selectColumn.lastIndexOf(DOT);
                String selectColumnDevicePath;
                if (lastDotIndex != -1){
                    // 这里判断设备名称是否相等，然后再将测点添加进去
                    selectColumnDevicePath = selectColumn.substring(0 ,lastDotIndex);
                    if (devicePath.equals(selectColumnDevicePath)){
                        // 进行设备的map加入
                        resultMapNew
                                .get(devicePath)
                                .computeIfAbsent(
                                        // 这里应该是lastDotIndex + 1，不然会多一个逗号出来。
                                        // 或者我们后面再拼接的时候不用逗号拼接也可以避免
                                        selectColumn.substring(lastDotIndex + 1),
                                        value -> new ConcurrentHashMap<>()
                                );
                    }
                }else {
                  selectColumnDevicePath = selectColumn;
                    // 这里判断设备名称是否相等，然后再将测点添加进去
                    if (devicePath.equals(selectColumnDevicePath)){
                        resultMapNew
                                .get(devicePath)
                                .computeIfAbsent(
                                        selectColumnDevicePath,
                                        value -> new ConcurrentHashMap<>()
                                );
                    }
                }

            });

        });
        return resultMapNew;
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
