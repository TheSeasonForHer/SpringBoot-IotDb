package com.iotdb.service.impl;

import static cn.hutool.core.text.StrPool.*;
import com.iotdb.dto.DataDto;
import com.iotdb.dto.DataDtos;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.DataService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.TSDataTypeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

import static com.iotdb.enums.StatusCodeEnum.FAIL;
import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;

/**
 * @author tjb
 * @date 2024/8/2
 */
@Service
public class DataServiceImpl implements DataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImpl.class);
    @Resource
    private SessionPool sessionService;

    /**
     * 根据测点插入数据
     */
    @Override
    public boolean insertRecordByTimeSeries(DataDto dataDto) {
        // 判断参数
        TimeSeriesDto timeSeriesDto = dataDto.getTimeSeriesDto();
        List<DataDto.Data> dataList = dataDto.getDataList();
        if (Objects.isNull(timeSeriesDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列参数不能为空");
        }
        CheckParameterUtil.checkTimeSeriesParameterIsBlank(timeSeriesDto);
        CheckParameterUtil.checkInsertData(dataList);
        // 过滤掉空数数据
         dataList = dataDto.getDataList().stream()
                 .filter(Objects::nonNull)
                 .filter(data -> CheckParameterUtil.checkStrings(data.getTime().toString(),data.getData().toString()))
                 .collect(Collectors.toList());

        // 封装要插入的测点
        String devicePath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice();
        List<MeasurementSchema> schemaList = new ArrayList<>();
        TSDataType dataType = TSDataTypeUtil.getTsDataType(timeSeriesDto.getTestPointType());

        Path correctPath = new Path(devicePath, timeSeriesDto.getTestPointName(), true);

        schemaList.add(new MeasurementSchema(correctPath.getMeasurement(), dataType));

        // 构建 tablet 并且填充数据
        Tablet tablet = new Tablet(correctPath.getDevice(), schemaList, dataList.size());
        // 填充数据
        for (long row = 0; row < dataList.size(); row++) {
            int rowIndex = tablet.rowSize++;
            // 添加时间戳
            tablet.addTimestamp(rowIndex, dataList.get((int) row).getTime());
            // 根据每一行测点数量进行插入数据
            for (int s = 0; s < schemaList.size(); s++) {
                tablet.addValue(
                        schemaList.get(s).getMeasurementId(),
                        rowIndex,
                        TSDataTypeUtil.getValueByData(dataType.getType(), dataList.get((int) row).getData().toString())
                );
            }
            // 如果达到可以插入的数量，就进行插入
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                try{
                    sessionService.insertTablet(tablet, true);
                }catch (IoTDBConnectionException | StatementExecutionException e){
                    throw new ServiceException(VALID_ERROR.getCode(), e.getMessage());
                }
                tablet.reset();
            }
        }
        return true;
    }

    /**
     * 删除测点数据 时间范围可选择
     *
     * @return ；成功或失败
     */
    @Override
    public boolean deleteDataByTimeRange(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        //  参数校验成功
        if (!Objects.isNull(timeSeriesDto)){
            CheckParameterUtil.checkQueryTimeSeriesParameter(timeSeriesDto);
            //  添加路径
            List<String> pathList = new ArrayList<>();
            String testPointPath = timeSeriesDto.getPath() + DOT + timeSeriesDto.getDevice() + DOT + timeSeriesDto.getTestPointName();
            pathList.add(testPointPath);
            try {
                if (!sessionService.checkTimeseriesExists(testPointPath)) {
                    throw  new ServiceException(VALID_ERROR.getCode(), "时间序列不存在,无法删除数据");
                }

                /**
                 * 为了实现时间的可选，我们这里使用异常抛出的方式来进行判断，
                 * 如果抛出了异常，我们就查询所有数据
                 * 没有抛出异常，就根据时间范围查询数据
                 */
                try {
                    CheckParameterUtil.checkRangeTime(queryDto);
                    long start = queryDto.getStartTime();
                    long end = queryDto.getStartTime();
                    //删除时间范围内的数据
                    sessionService.deleteData(pathList, start, end);
                    LOGGER.info(testPointPath.toUpperCase());
                }catch (ServiceException serviceException){
                    sessionService.deleteData(pathList, Long.MAX_VALUE);
                    LOGGER.info(testPointPath.toUpperCase());
                }
            }catch (IoTDBConnectionException | StatementExecutionException e){
                throw new ServiceException(VALID_ERROR.getCode(), e.getMessage());
            }
            return true;
        }
        throw new ServiceException(VALID_ERROR.getCode(), "时间序列参数异常");
    }

    @Override
    public boolean insertRecordByBatchTimeSeries(DataDtos dataDtos) {
        // 检查参数
        if (!CheckParameterUtil.checkStrings(dataDtos.getPath(), dataDtos.getDevice())){
            throw new ServiceException(VALID_ERROR.getCode(), "插入设备参数异常");
        }
        List<String> testPointName = dataDtos.getTestPointName();
        CheckParameterUtil.checkMeasurements(testPointName, false);
        List<String> testPointType = dataDtos.getTestPointType();
        if (testPointName.size() != testPointType.size()){
            throw new ServiceException(VALID_ERROR.getCode(), "测点名称和测点类型不对应");
        }

        List<List<DataDtos.Data>> dataLists = dataDtos.getDataList();
        // TODO:数据判断
        dataLists
                .stream().filter(Objects::nonNull)
                .forEach(dataList -> {
                    dataList.stream().filter(Objects::nonNull)
                            .filter(data -> CheckParameterUtil.checkStrings(data.getTime().toString(),data.getData().toString()))
                            .collect(Collectors.toList());
                });
        if (dataLists.size() != testPointName.size()){
            throw new ServiceException(VALID_ERROR.getCode(), "测点名称和需要插入的数据不对应");
        }

        // 构造数据
        String devicePath = dataDtos.getPath() + DOT + dataDtos.getDevice();
        List<MeasurementSchema> schemaList = new ArrayList<>();
        List<TSDataType> dataTypes = new ArrayList<>(testPointType.size());
        // 构造测点
        for (int i = 0; i < testPointName.size(); i++) {
            TSDataType dataType = TSDataTypeUtil.getTsDataType(testPointType.get(i));
            schemaList.add(new MeasurementSchema(testPointName.get(i), dataType));
            dataTypes.add(dataType);
        }

        // 构建 tablet 并且填充数据
        /*
         * A Tablet example:
         *      device1
         * time s1, s2, s3
         * 1,   1,  1,  1
         * 2,   2,  2,  2
         * 3,   3,  3,  3
         */
        Tablet tablet = new Tablet(devicePath, schemaList, dataLists.get(0).size());
        tablet.initBitMaps();
        // 数据插入
        // 一个测点的数据大小
        for (long row = 0; row < dataLists.get(0).size(); row++) {
            // 0开始计数
            int rowIndex = tablet.rowSize++;
            // 添加时间戳
            tablet.addTimestamp(rowIndex, dataLists.get(0).get((int) row).getTime());
            // 根据每一行测点数量进行插入数据
            for (int s = 0; s < schemaList.size(); s++) {
                String value = dataLists.get(s).get((int) row).getData().toString();
                if (StringUtils.isBlank(value) || StringUtils.isEmpty(value)){
                    tablet.bitMaps[s].mark((int) row);
                }
                tablet.addValue(
                        schemaList.get(s).getMeasurementId(),
                        rowIndex,
                        TSDataTypeUtil.getValueByData(dataTypes.get(s).getType(), value)
                );
            }
            // 如果达到可以插入的数量，就进行插入
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                try{
                    sessionService.insertTablet(tablet, true);
                    tablet.reset();
                }catch (IoTDBConnectionException | StatementExecutionException e){
                    throw new ServiceException(FAIL.getCode(), e.getMessage());
                }
            }
        }
        if (tablet.rowSize != 0) {
            try {
                sessionService.insertTablet(tablet);
                tablet.reset();
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new ServiceException(FAIL.getCode(), e.getMessage());
            }
        }
        return true;
    }

}
