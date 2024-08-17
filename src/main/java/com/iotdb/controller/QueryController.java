package com.iotdb.controller;

import com.iotdb.utils.CheckParameterUtil;
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
import java.util.concurrent.ConcurrentHashMap;

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
            SessionDataSetWrapper dataSetWrapper = sessionPool.executeQueryStatement("select * from root.wf01.wt01,root.wf01.wt02,root.wf01.wt03");
            // device-timestamp-measurement
            List<String> columnNameList = dataSetWrapper.iterator().getColumnNameList();
            List<String> columnTypeList = dataSetWrapper.iterator().getColumnTypeList();
            Map<String, Map<Long, Map<Long, List<Object>>>> resultMap = new ConcurrentHashMap<>();
            while(dataSetWrapper.hasNext()){
                RowRecord next = dataSetWrapper.next();

                long timestamp = next.getTimestamp();
                List<Field> fields = next.getFields();
                String[] deviceString = fields.get(0).toString().split("\\.");
                String device = deviceString[deviceString.length - 1];

                Map<Long, Map<Long, List<Object>>> longMapMap = resultMap.computeIfAbsent(device, k -> new ConcurrentHashMap<>());
                Map<Long, List<Object>> longListMap = longMapMap.computeIfAbsent(timestamp, k -> new ConcurrentHashMap<>());
                fields.remove(0);
                fields.forEach(field -> {
                    longListMap.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(TSDataTypeUtil.getValueByFiled(field));
                });
            }

            dataSetWrapper.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
