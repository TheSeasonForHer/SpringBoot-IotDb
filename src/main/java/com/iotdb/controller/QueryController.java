package com.iotdb.controller;

import com.iotdb.common.ServiceTypeConstants;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.iotdb.common.Constants.JOIN_CHAR;
import static com.iotdb.common.Constants.TIME_VALUE;

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

    @Resource
    private SessionPool sessionPool;
    @GetMapping("/")
    public void get(){
        try {
            SessionDataSetWrapper dataSetWrapper = sessionPool.executeQueryStatement("select * from root.zh,root.zh1");
            // device-timestamp-measurement
            List<String> columnNameList = dataSetWrapper.iterator().getColumnNameList();
            List<String> deviceName = new ArrayList<>(columnNameList);
            deviceName.remove(0);
            if (columnNameList.size() <= 1){
                return;
            }
            List<String> columnTypeList = dataSetWrapper.iterator().getColumnTypeList();
            columnTypeList.remove(0);

            // 所有设备的名称
            List<String> deviceNames = getDeviceName(deviceName);
            // 设备分区，测点分区，时间分区，数据(构建分区框架)
            Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew = getStringMapMap(deviceNames, deviceName);
            // 分区框架map数据导入
            fillDataToMap(dataSetWrapper, deviceNames, deviceName, resultMapNew);
            byte[] resultBytes = getResultBytesByResultMap(resultMapNew);
            dataSetWrapper.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过构建好的数据map，返回我们想要的字节拼接
     * @param resultMapNew
     * @return
     */
    private static byte[] getResultBytesByResultMap(Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew) {
        // 获取拼接头
        StringBuilder resultHeader = getResultHeader();
        // 拿到设备分区
        resultMapNew.keySet()
                .forEach(deviceName -> {
                    // 拼接设备头，反斜杠0
                    resultHeader.append("设备")
                                .append(deviceName)
                                .append("\\0\\0")
                                .append(JOIN_CHAR);
                    // 设备对应的map
                    Map<String, Map<Long, List<Object>>> deviceToTestPointMap = resultMapNew.get(deviceName);
                    // 通过设备对应的分区下 获取到测点和所对应的数据
                    Object[] keysOfOneDevice = deviceToTestPointMap.keySet().toArray();
                    System.out.println( "key的长度"+ keysOfOneDevice.length);
                    for (int i = 0; i < keysOfOneDevice.length; i++) {
                        // 拼接 数据点位
                        resultHeader.append("点位ID")
                                    .append(i + 1)
                                    .append(JOIN_CHAR);

                        // 获取到测点下对应的时间map
                        Map<Long, List<Object>> testPointToTimeMap = deviceToTestPointMap.get(keysOfOneDevice[i]);
                        // 测点所对应的时间
                        Set<Long> times = testPointToTimeMap.keySet();
                        // 拼接数据大小头
                        resultHeader.append("数据大小").append(times.size()).append(JOIN_CHAR);
                        times.forEach(timestamp -> {
                            // 根据对应的时间获取对应的数据
                            List<Object> dataList = testPointToTimeMap.get(timestamp);
                            // 数据不为空
                            if (!dataList.isEmpty()){
                                // 数据遍历进行拼接
                                dataList.forEach(data -> {
                                    resultHeader.append(timestamp)
                                                .append(TIME_VALUE)
                                                .append(data)
                                                .append(JOIN_CHAR);
                                });
                            }
                        });
                    }
        });
        System.out.println(resultHeader);
        return resultHeader.toString().getBytes();
    }

    private static void fillDataToMap(SessionDataSetWrapper dataSetWrapper, List<String> deviceNames, List<String> deviceName, Map<String, Map<String, Map<Long, List<Object>>>> resultMapNew) throws IoTDBConnectionException, StatementExecutionException {
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
                String[] testPoint = deviceName.get(i).split("\\.");
                resultMapNew.get(deviceNames.get(i))
                        .get(testPoint[testPoint.length - 1])
                        .computeIfAbsent(timestamp, value -> new ArrayList<>())
                        .add(fields.get(i).getDataType() != null ?
                                TSDataTypeUtil.getValueByFiled(fields.get(i))
                                : null
                        );

            }
            System.out.println("data"+ next);
        }
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

    /**
     * 返回拼接头
     * @return
     */
    public static StringBuilder getResultHeader(){
        return new StringBuilder()
                .append("127.0.0.1")
                .append("|")
                .append(ServiceTypeConstants.HISTORY.getCode())
                .append("|");
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
