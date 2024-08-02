package com.iotdb.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ObjectUtil;
import com.iotdb.dto.DataDto;
import com.iotdb.dto.TimeSeriesDto;

import java.sql.Time;

public class CheckParameterUtil {
    /**
     * 检查数据完整性-- 时间序列必须完整
     * @param seriesDto
     * @return
     */
    public static boolean checkTimeSeriesParameter(TimeSeriesDto seriesDto) {
        if (seriesDto.getPath() != null
                && seriesDto.getDevice() != null
                && seriesDto.getTestPointName() != null
                && seriesDto.getTestPointType() !=null){
            return true;
        }
        return false;
    }

    /**
     * 查询的时候的时间序列参数校验-->数据库和设备不为空就行
     * @param seriesDto
     * @return 不为空返回 true 为空返回 false
     */
    public static boolean checkQueryTimeSeriesParameter(TimeSeriesDto seriesDto){
        return seriesDto.getPath() != null && seriesDto.getDevice() != null;
    }
    /**
     * 校验时间
     */
    public static boolean checkTime(String startTime, String endTime){
        if (startTime == null || endTime == null){
            return false;
        }
        long start = Long.parseLong(startTime);
        long end = Long.parseLong(endTime);
        if (start > end){
            return false;
        }
        return true;
    }

    /**
     * 检查参数是否有效
     * @param dataDto
     * @return
     */
    public static boolean checkParameter(DataDto dataDto) {
        TimeSeriesDto timeSeriesDto = dataDto.getTimeSeriesDto();
        if (ObjectUtil.isEmpty(timeSeriesDto)){
            return false;
        }
        if (!CheckParameterUtil.checkTimeSeriesParameter(timeSeriesDto)){
            return false;
        }
        if (CollectionUtil.isEmpty(dataDto.getDataList())){
            return false;
        }
        return true;
    }
}
