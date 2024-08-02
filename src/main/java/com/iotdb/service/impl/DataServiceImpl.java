package com.iotdb.service.impl;

import com.iotdb.dto.DataDto;
import com.iotdb.dto.QueryDto;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.service.DataService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SessionUtil;
import com.iotdb.utils.TSDataTypeUtil;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class DataServiceImpl implements DataService {
    @Resource
    private SessionUtil sessionUtil;
    /**
     * 根据测点插入数据
     * @param dataDto
     * @return
     */
    @Override
    public boolean insertRecordByTimeSeries(DataDto dataDto) throws IoTDBConnectionException, StatementExecutionException {
        //判断参数
        if (CheckParameterUtil.checkParameter(dataDto)){
            TimeSeriesDto timeSeriesDto = dataDto.getTimeSeriesDto();
            //过滤掉空数数据
            List<DataDto.Data> dataList = dataDto.getDataList().stream().filter(data -> data.getTime() > 0).collect(Collectors.toList());
            //contruct the path to store
            String devicePath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice();

            List<MeasurementSchema> schemaList = new ArrayList<>();
            TSDataType dataType = TSDataTypeUtil.getTsDataType(timeSeriesDto.getTestPointType());
            schemaList.add(new MeasurementSchema(timeSeriesDto.getTestPointName(), dataType));

            Tablet tablet = new Tablet(devicePath, schemaList, dataList.size());

            for (long row = 0; row < dataList.size(); row++) {
                int rowIndex = tablet.rowSize++;
                tablet.addTimestamp(rowIndex, dataList.get((int) row).getTime());
                for (int s = 0; s < 1; s++) {
                    tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, TSDataTypeUtil.getValueByData(dataType.getType(),dataList.get((int) row).getData()));
                }
                if (tablet.rowSize == tablet.getMaxRowNumber()) {
                    sessionUtil.getConnection().insertTablet(tablet, true);
                    tablet.reset();
                }
            }
//            sessionPool.close();
            return true;
        }
        return false;
    }

    /**
     * 删除测点数据 时间范围可选择
     *
     * @return ；成功或失败
     */
    @Override
    public boolean deleteDataByTimeRange(QueryDto queryDto) {
        TimeSeriesDto timeSeriesDto = queryDto.getTimeSeriesDto();
        //参数校验成功
        if (CheckParameterUtil.checkTimeSeriesParameter(timeSeriesDto)){
            //添加路径
            List<String> pathList = new ArrayList<>();
            String testPointPath = timeSeriesDto.getPath() + "." + timeSeriesDto.getDevice() + "." + timeSeriesDto.getTestPointName();
            pathList.add(testPointPath);
            try {
                //看看存在不
                if (!sessionUtil.getConnection().checkTimeseriesExists(testPointPath)) {
                    throw  new RuntimeException("时间序列不存在");
                }
                //根据是否有时间进行删除
                if (queryDto.getStartTime() != "" && queryDto.getEndTime() != "" && Long.parseLong(queryDto.getStartTime()) < Long.parseLong(queryDto.getEndTime())) {
                    //删除时间范围内的数据
                    sessionUtil.getConnection().deleteData(pathList, Long.parseLong(queryDto.getStartTime()), Long.parseLong(queryDto.getEndTime()));
                }else{
                    //没有时间范围就指定了一个最大值 就相当于删除全部数据
                    sessionUtil.getConnection().deleteData(pathList, Long.MAX_VALUE);
                }
            }catch (IoTDBConnectionException | StatementExecutionException e){
                throw new RuntimeException(e);
            }
            return true;
        }
        return false;
    }

}
