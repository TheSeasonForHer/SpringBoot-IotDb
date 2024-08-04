package com.iotdb.controller;

import com.iotdb.vo.Result;
import com.iotdb.dto.QueryDto;
import com.iotdb.service.QueryService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
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


}
