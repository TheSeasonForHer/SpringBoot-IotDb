package com.iotdb.service.impl;

import com.iotdb.common.Constants;
import com.iotdb.dto.QueryDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.ExportService;
import com.iotdb.utils.CheckParameterUtil;
import com.iotdb.utils.SQLBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.AbstractDataTool;
import org.apache.iotdb.tool.ExportData;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import static cn.hutool.core.text.StrPool.*;
import javax.annotation.Resource;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
        if (Objects.isNull(queryDto)){
            throw new ServiceException(VALID_ERROR.getCode(), "参数异常");
        }
        if (Objects.isNull(queryDto.getTimeSeriesDto())){
            throw new ServiceException(VALID_ERROR.getCode(), "");
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
        // todo:利用线程池操作
        new Thread(() -> submitExportRequest(sqlBuilder.build())).start();
    }

    @Async
    public void submitExportRequest(String sqlBuilder) {
        LOGGER.info("导出的数据的SQL是：{}", sqlBuilder);
        // 模拟导出过程
        dumpResult(sqlBuilder, exportDirectory);
        // 通知用户
        //notifyUserOfCompletion();
    }
    public void dumpResult(String sql, String path) {
        legalCheck(sql);
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
            writeCsvFile(sessionDataSet, path, headers);
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

    public void writeCsvFile(
            SessionDataSet sessionDataSet, String filePath, List<Object> headers)
            throws IOException, IoTDBConnectionException, StatementExecutionException {
        final String finalFilePath = filePath + ".csv";
        final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
        csvPrinterWrapper.printRecord(headers);
        if (sessionDataSet.hasNext()) {
            RowRecord rowRecord = sessionDataSet.next();
            if (rowRecord.getTimestamp() != 0) {
                csvPrinterWrapper.print(timeTrans(rowRecord.getTimestamp()));
            }
            rowRecord
                    .getFields()
                    .forEach(
                            field -> {
                                String fieldStringValue = field.getStringValue();
                                if (!"null".equals(fieldStringValue)) {
                                    if ((field.getDataType() == TSDataType.TEXT
                                            || field.getDataType() == TSDataType.STRING)
                                            && !fieldStringValue.startsWith("root.")) {
                                        fieldStringValue = "\"" + fieldStringValue + "\"";
                                    }
                                    csvPrinterWrapper.print(fieldStringValue);
                                } else {
                                    csvPrinterWrapper.print("");
                                }
                            });
            csvPrinterWrapper.println();
        }
        csvPrinterWrapper.flush();
        csvPrinterWrapper.close();
    }

    public static String timeTrans(Long time) {
        // todo:
        switch (timeFormat) {
            case "default":
                return RpcUtils.parseLongToDateWithPrecision(
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME, time, zoneId, "ns"
                        );
            case "timestamp":
            case "long":
            case "number":
                return String.valueOf(time);
            default:
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), zoneId)
                        .format(DateTimeFormatter.ofPattern(timeFormat));
        }
    }
    static class CSVPrinterWrapper {
        private final String filePath;
        private final CSVFormat csvFormat;
        private CSVPrinter csvPrinter;

        public CSVPrinterWrapper(String filePath) {
            this.filePath = filePath;
            this.csvFormat =
                    CSVFormat.Builder.create(CSVFormat.DEFAULT)
                            .setHeader()
                            .setSkipHeaderRecord(true)
                            .setEscape('\\')
                            .setQuoteMode(QuoteMode.NONE)
                            .build();
        }

        public void printRecord(final Iterable<?> values) throws IOException {
            if (csvPrinter == null) {
                csvPrinter = csvFormat.print(new PrintWriter(filePath));
            }
            csvPrinter.printRecord(values);
        }

        public void print(Object value) {
            if (csvPrinter == null) {
                try {
                    csvPrinter = csvFormat.print(new PrintWriter(filePath));
                } catch (IOException e) {
                    return;
                }
            }
            try {
                csvPrinter.print(value);
            } catch (IOException e) {
                throw new ServiceException(FAIL.getCode(), e.getMessage());
            }
        }

        public void println() throws IOException {
            csvPrinter.println();
        }

        public void close() throws IOException {
            if (csvPrinter != null) {
                csvPrinter.close();
            }
        }

        public void flush() throws IOException {
            if (csvPrinter != null) {
                csvPrinter.flush();
            }
        }
    }

}
