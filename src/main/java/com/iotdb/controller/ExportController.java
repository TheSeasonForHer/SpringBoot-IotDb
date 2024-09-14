package com.iotdb.controller;

import com.iotdb.dto.QueryDto;
import com.iotdb.service.ExportService;
import com.iotdb.vo.Result;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * @author tjb
 * @date 2024/8/2
 */
@RestController
@RequestMapping("/export")
public class ExportController {
    @Resource
    private ExportService exportService;
    @PostMapping("/exportData")
    public Result<?> exportData(@RequestBody QueryDto queryDto){
        exportService.export(queryDto);
        return Result.ok();
    }

}
