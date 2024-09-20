package com.iotdb.utils;

import cn.hutool.core.collection.CollectionUtil;
import com.iotdb.common.Constants;
import static com.iotdb.enums.StatusCodeEnum.*;
import com.iotdb.dto.DataDto;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.exception.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
/**
 * @author tjb
 * @date 2024/8/2
 */
public class CheckParameterUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckParameterUtil.class);
    /**
     * 检查数据完整性-- 时间序列必须完整
     * 这里只检查属性的值，对象是否初始化需要再外层做
     * @param seriesDto : 时间序列对象
     */
    public static void checkTimeSeriesParameterIsBlank(TimeSeriesDto seriesDto) {
        checkQueryTimeSeriesParameter(seriesDto);
        if (StringUtils.isBlank(seriesDto.getTestPointName())
                || StringUtils.isEmpty(seriesDto.getTestPointName())){
            throw new ServiceException(VALID_ERROR.getCode(), "测点参数异常");
        }
        if (StringUtils.isBlank(seriesDto.getTestPointType())
                || StringUtils.isEmpty(seriesDto.getTestPointType())){
            throw new ServiceException(VALID_ERROR.getCode(), "测点参数异常");
        }
    }

    /**
     * 查询的时候的时间序列参数校验
     * @param seriesDto : 查询中的时间序列对象只需要校验 数据库 和 设备
     */
    public static void checkQueryTimeSeriesParameter(TimeSeriesDto seriesDto){
        if (StringUtils.isBlank(seriesDto.getPath())
                || StringUtils.isEmpty(seriesDto.getPath())){
            throw new ServiceException(VALID_ERROR.getCode(), "数据库名称参数异常");
        }
        if (StringUtils.isBlank(seriesDto.getDevice())
                || StringUtils.isEmpty(seriesDto.getDevice())){
            throw new ServiceException(VALID_ERROR.getCode(), "设备参数异常");
        }
    }
    /**
     * 根据时间是否可选，校验时间是否正确，并且是否在一个范围内
     * 需要多走一个判断
     */
    public static void checkRangeTime(QueryDto queryDto){
        String startTime = String.valueOf(queryDto.getStartTime());
        String endTime = String.valueOf(queryDto.getEndTime());
        if (StringUtils.isBlank(startTime)
                || StringUtils.isBlank(endTime)
                || StringUtils.isEmpty(startTime)
                || StringUtils.isEmpty(endTime)){
            throw new ServiceException(VALID_ERROR.getCode(), "查询时间不能为空");
        }
        long start = 0;
        long end = 0;
        try{
             start = Long.parseLong(startTime);
             end = Long.parseLong(endTime);
        }catch (Exception e){
            throw new ServiceException(FAIL.getCode(), "解析时间出问题了");
        }
        if (start < Constants.NUMBER_0L || end < Constants.NUMBER_0L){
            throw new ServiceException(VALID_ERROR.getCode(), "时间参数输入有误");
        }
        if (start >= end){
            throw new ServiceException(VALID_ERROR.getCode(), "查询开始时间不能大于结束时间");
        }
    }

    /**
     * 检查插入数据是否有效
     * @param dataList : 插入数据
     */
    public static void checkInsertData(List<DataDto.Data> dataList) {
        if (dataList.isEmpty()){
            throw new ServiceException(VALID_ERROR.getCode(), "插入数据不能为空");
        }
        dataList.forEach(data -> {
            if (StringUtils.isBlank(data.getTime().toString())
                    || StringUtils.isEmpty(data.getTime().toString())){
                LOGGER.warn("插入数据中有非法的时间:{}", data.getTime());
            }
            if (StringUtils.isBlank(data.getData().toString())
                    || StringUtils.isEmpty(data.getData().toString())){
                LOGGER.warn("插入数据中有非法数据:{}", data.getData());
            }
        });
    }

    /**
     * 检查测点参数是否合法
     * @param measurements : 测点参数
     * @param flag ： 是否为 1 个测点参数
     */
    public static void checkMeasurements(List<String> measurements, boolean flag){
        if (CollectionUtil.isEmpty(measurements)){
            throw new ServiceException(VALID_ERROR.getCode(),"测点参数为空");
        }
        if (CollectionUtil.contains(measurements, "")){
            throw new ServiceException(VALID_ERROR.getCode(), "测点参数中包含了空白字符串");
        }
        if (flag && measurements.size() > Constants.NUMBER_1){
            throw new ServiceException(VALID_ERROR.getCode(), "测点参数数量与需求不对等");
        }
    }

    /**
     * 判断传入字符串是否为空
     * @param strings 字符串(可以传入多个)
     * @return
     */
    public static boolean checkStrings(String... strings){
        for (String string : strings) {
            if (StringUtils.isBlank(string) || StringUtils.isEmpty(string)){
                return false;
            }
        }
        return true;
    }
}
