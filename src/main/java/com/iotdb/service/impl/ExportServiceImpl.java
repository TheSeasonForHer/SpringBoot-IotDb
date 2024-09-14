package com.iotdb.service.impl;

import com.iotdb.common.Constants;
import com.iotdb.dto.QueryDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.ExportService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.AbstractDataTool;
import org.apache.iotdb.tool.ExportData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import static cn.hutool.core.text.StrPool.*;
import javax.annotation.Resource;
import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.iotdb.enums.StatusCodeEnum.FAIL;
import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/9/13 0:34
 */
@Service
public class ExportServiceImpl extends AbstractDataTool implements ExportService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryServiceImpl.class);

    @Resource
    private SessionPool sessionPool;

    // todo: 地址改为配置文件
    private final String exportDirectory = "C:\\SoftWare\\";

    @Override
    public void export(QueryDto queryDto) {
        if (Objects.isNull(queryDto.getTimeSeriesDto())){
            throw new ServiceException(VALID_ERROR.getCode(), "");
        }
        CheckParameterUtil.checkQueryTimeSeriesParameter(queryDto.getTimeSeriesDto());
        CheckParameterUtil.checkMeasurements(queryDto.getMeasurements(), false);
        // 构造查询
        SQLBuilder sqlBuilder = new SQLBuilder();
        sqlBuilder.select(String.join(COMMA, queryDto.getMeasurements()))
                .from(queryDto.getTimeSeriesDto().getPath() + DOT + queryDto.getTimeSeriesDto().getDevice());
        long start = queryDto.getStartTime();
        long end = queryDto.getEndTime();
        CheckParameterUtil.checkRangeTime(queryDto);
        sqlBuilder.where("time >=" + start + " and time <=" + end);
        if (queryDto.getReachMaxSize() > Constants.NUMBER_1){
            sqlBuilder.limit(queryDto.getReachMaxSize()).build();
        }
        LOGGER.info("导出的数据的SQL是：{}", sqlBuilder);
        // todo:利用线程池操作
        new Thread(() -> submitExportRequest(sqlBuilder)).start();
    }

    @Async
    public void submitExportRequest(SQLBuilder sqlBuilder) {
        // 模拟导出过程
        dumpResult(sqlBuilder.toString(), 0, exportDirectory);
        // 通知用户
        //notifyUserOfCompletion();
    }
    public void dumpResult(String sql, int index, String path) {
        legalCheck(sql);
        path = path + index;
        try {
            SessionDataSet sessionDataSet = this.sessionPool.executeQueryStatement(sql).getSessionDataSet();
            List<String> names = sessionDataSet.getColumnNames();
            List<String> types = sessionDataSet.getColumnTypes();
            List<Object> headers = new ArrayList<>(names.size());

            if (Boolean.TRUE) {
                for (int i = 0; i < names.size(); i++) {
                    if (!"Time".equals(names.get(i)) && !"Device".equals(names.get(i))) {
                        headers.add(String.format("%s(%s)", names.get(i), types.get(i)));
                    } else {
                        headers.add(names.get(i));
                    }
                }
            } else {
                headers.addAll(names);
            }
            ExportData.timeFormat = "default";
            ExportData.zoneId = ZoneId.systemDefault();
            ExportData.writeCsvFile(sessionDataSet, path, headers, 200);
            sessionDataSet.closeOperationHandle();
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new ServiceException(FAIL.getCode(), "Cannot dump result because: " + e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static void legalCheck(String sql) {
        String aggregatePattern =
                "\\b(count|sum|avg|extreme|max_value|min_value|first_value|last_value|max_time|min_time|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|max_by|min_by)\\b\\s*\\(";
        Pattern pattern = Pattern.compile(aggregatePattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql.toUpperCase(Locale.ROOT));
        if (matcher.find()) {
            throw new ServiceException(VALID_ERROR.getCode(),"The sql you entered is invalid, please don't use aggregate query.");
        }
    }

}
