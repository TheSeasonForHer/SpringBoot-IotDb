package com.iotdb.controller;

import cn.hutool.core.collection.CollectionUtil;
import com.iotdb.vo.Result;
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
     * @param timeSeriesDto : 时间序列
     * @return 失败成功的信息，timeseries列名
     */
    //todo:跟换校验方式
    @PostMapping("/createTimeSeries")
    public Result<?> createTimeSeries(@RequestBody List<TimeSeriesDto> timeSeriesDto) {
        return Result.ok(timeSeriesService.createTimeSeries(timeSeriesDto));
    }
    @GetMapping("/hello")
    public Result<?> hello(){
        return Result.ok();
    }

}
