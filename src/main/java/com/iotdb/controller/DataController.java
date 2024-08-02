package com.iotdb.controller;

import com.iotdb.common.Result;
import com.iotdb.dto.DataDto;
import com.iotdb.dto.QueryDto;
import com.iotdb.service.DataService;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/data")
public class DataController {
    @Resource
    private DataService dataService;

    /**
     *
     * @param dataDto
     * @return 成功失败
     */
    @PostMapping("/insertDataByTimeSeries")
    public Result insertData(@RequestBody DataDto dataDto) throws IoTDBConnectionException, StatementExecutionException {
        boolean b = dataService.insertRecordByTimeSeries(dataDto);
        return b ? Result.success("插入成功") : Result.error("插入失败");
    }

    /**
     * 删除数据根据时间范围
     * @param queryDto : 设备、测点、时间范围（可选）
     * @return
     */
    @PostMapping("/deleteDataByTimeRange")
    public Result deleteDataByTimeRange(@RequestBody QueryDto queryDto){
        boolean b = dataService.deleteDataByTimeRange(queryDto);
        return b ? Result.success("删除成功") : Result.error("删除失败");
    }
}
