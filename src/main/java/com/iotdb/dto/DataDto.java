package com.iotdb.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataDto {
    private TimeSeriesDto timeSeriesDto;
    private List<Data> dataList;

    @lombok.Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Data{
        private long time;
        private String data;
    }
}
