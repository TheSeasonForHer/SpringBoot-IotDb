package com.iotdb.service.impl;

import com.iotdb.common.Constants;
import com.iotdb.config.FileStorageProperties;
import com.iotdb.dto.QueryDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.ExportService;
import com.iotdb.utils.AsyncFileWriterWithEvents;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.ExportData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import static cn.hutool.core.text.StrPool.*;
import javax.annotation.Resource;
import java.util.*;

import static com.iotdb.enums.StatusCodeEnum.FAIL;
import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/9/13 0:34
 */
@Service
public class ExportServiceImpl extends ExportData implements ExportService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExportServiceImpl.class);

    @Resource
    private SessionPool sessionPool;

    @Resource
    private FileStorageProperties fileStorageProperties;


    @Override
    public void export(QueryDto queryDto) {
        if (Objects.isNull(queryDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "参数异常");
        }
        if (Objects.isNull(queryDto.getTimeSeriesDto())){
            throw new ServiceException(VALID_ERROR.getCode(), "时间序列参数异常");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(queryDto.getTimeSeriesDto());
        CheckParameterUtil.checkMeasurements(queryDto.getMeasurements(), false);
        // 构造查询
        SQLBuilder sqlBuilder = new SQLBuilder();
        sqlBuilder.select(String.join(COMMA, queryDto.getMeasurements()))
                .from(queryDto.getTimeSeriesDto().getPath() + DOT + queryDto.getTimeSeriesDto().getDevice());

        // 如果时间不为空就拼接时间范围
        Long start = queryDto.getStartTime();
        Long end = queryDto.getEndTime();
        if (start != null && end != null){
            CheckParameterUtil.checkRangeTime(queryDto);
            sqlBuilder.where("time >=" + start + " and time <=" + end);
        }
        if (queryDto.getReachMaxSize() > Constants.NUMBER_1){
            sqlBuilder.limit(queryDto.getReachMaxSize());
        }
        long start_export = System.currentTimeMillis();
        AsyncFileWriterWithEvents writer = new AsyncFileWriterWithEvents(sessionPool, exportInfoDto -> {
            //            notifyUserByUserEmail();
            long end_export = System.currentTimeMillis();
            System.out.println("导出成功,下载地址是：" + exportInfoDto.getDownloadUrl() + ",导出花费" + (end_export - start_export) + "ms时间");
        });
        String storagePath = fileStorageProperties.getStorage_path();
        if (!CheckParameterUtil.checkStrings(storagePath)){
            throw new ServiceException(FAIL.getCode(), "存储路径不明确");
        }
        writer.writeToFileAsync(sqlBuilder.build(), fileStorageProperties);
    }



}
