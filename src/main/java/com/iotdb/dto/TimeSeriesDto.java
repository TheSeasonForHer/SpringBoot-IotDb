package com.iotdb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimeSeriesDto {
    /**
     * 数据库路径
     */
    private String path;
    /**
     * 设备ID
     */
    private String device;
    /**
     * 测点名称
     */
    private String testPointName;
    /**
     * 测点类型
     */
    private String testPointType;
}
