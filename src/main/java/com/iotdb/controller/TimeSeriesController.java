package com.iotdb.controller;

import cn.hutool.core.collection.CollectionUtil;
import com.iotdb.common.Result;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.service.TimeSeriesService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

@RestController
@RequestMapping("/timeSeries")
public class TimeSeriesController {
    @Resource
    private TimeSeriesService timeSeriesService;

    /**
     * 创建新测点的时间序列 : 客户端使用的版本一定要和session一样，不然不支持很多类型
     * @param timeSeriesDto
     * @return 失败成功的信息，timeseries列名
     */
    @PostMapping("/createTimeSeries")
    public Result createTimeSeries(@RequestBody List<TimeSeriesDto> timeSeriesDto) {
        if (!CollectionUtil.isEmpty(timeSeriesDto)){
            return Result.success("成功插入",timeSeriesService.createTimeSeries(timeSeriesDto));
        }
        return Result.error();
    }
    @GetMapping("/hello")
    public Result hello(){
        return Result.success();
    }

}
