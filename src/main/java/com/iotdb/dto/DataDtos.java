package com.iotdb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataDtos {
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
    private List<String> testPointName;
    /**
     * 测点类型
     */
    private List<String> testPointType;

    /**
     * 插入数据
     */
    private List<List<Data>> dataList;

    @lombok.Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Data{
        private Long time;
        private Object data;
    }
}
