package com.iotdb.service.impl;

import com.iotdb.common.Constants;
import com.iotdb.dto.TimeSeriesDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.TimeSeriesService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SessionUtil;
import com.iotdb.utils.TSDataTypeUtil;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class TimeSeriesServiceImpl implements TimeSeriesService {

    @Resource
    private SessionUtil sessionUtil;
    /**
     * 创建时间序列
     */
    @Override
    public List<String> createTimeSeries(List<TimeSeriesDto> timeSeriesDto) {
        //  返回的时序列名
        List<String> timeSeriesNameList = new ArrayList<>();
        for (TimeSeriesDto seriesDto : timeSeriesDto) {
            //  检查是否数据完整
            if (Objects.isNull(seriesDto)){
                continue;
            }
            CheckParameterUtil.checkTimeSeriesParameterIsBlank(seriesDto);
            //  获取连接
            String path = seriesDto.getPath() + "." + seriesDto.getDevice() + "." + seriesDto.getTestPointName();
            try {
                if(!sessionUtil.getConnection().checkTimeseriesExists(path)){
                    sessionUtil.getConnection().createTimeseries(path, TSDataTypeUtil.getTsDataType(seriesDto.getTestPointType()), TSEncoding.PLAIN, CompressionType.SNAPPY);
                    timeSeriesNameList.add(path);
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new ServiceException(Constants.CODE_500, e.getMessage());
            }
        }
        return timeSeriesNameList;
    }
}
