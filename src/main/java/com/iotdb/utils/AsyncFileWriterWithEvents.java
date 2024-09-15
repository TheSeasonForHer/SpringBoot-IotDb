package com.iotdb.utils;

import com.iotdb.config.FileStorageProperties;
import com.iotdb.dto.ExportInfoDto;
import com.iotdb.exception.ServiceException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.ExportData;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.iotdb.enums.StatusCodeEnum.FAIL;
import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;

public class AsyncFileWriterWithEvents extends ExportData {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncFileWriterWithEvents.class);

    public interface FileWriteListener {
        void onFileWritten(ExportInfoDto info);
    }
    private final FileWriteListener listener;
    private final SessionPool sessionPool;

    public AsyncFileWriterWithEvents(SessionPool sessionPool, FileWriteListener listener) {
        if (Objects.isNull(sessionPool)){
            throw new ServiceException(FAIL.getCode(),"connect to the iotdb server failed");
        }
        this.sessionPool = sessionPool;
        this.listener = listener;
    }

    public void writeToFileAsync(String sqlBuilder, FileStorageProperties fileStorageProperties) {
        ThreadPool.getPoolExecutor().execute(new Runnable() {
            @Override
            public void run() {
                String UUID = java.util.UUID.randomUUID().toString() + ".csv";
                final String filePath = fileStorageProperties.getStorage_path() + UUID;
                dumpResult(sqlBuilder, filePath,fileStorageProperties);
                listener.onFileWritten(new ExportInfoDto(sqlBuilder, filePath, fileStorageProperties.getDownload_pre() + UUID, "2020211637", "天谋科技"));
            }
        });
    }

    public void dumpResult(String sql,String finalFilePath, FileStorageProperties fileStorageProperties) {
        legalCheck(sql);
        try {
            LOGGER.info("导出的数据的SQL是：{}", sql);
            SessionDataSetWrapper dataSetWrapper = sessionPool.executeQueryStatement(sql);
            String timestampPrecision = sessionPool.getTimestampPrecision();
            SessionDataSet sessionDataSet = dataSetWrapper.getSessionDataSet();

            List<String> names = sessionDataSet.getColumnNames();
            List<String> types = sessionDataSet.getColumnTypes();
            List<Object> headers = new ArrayList<>(names.size());

            if (Boolean.TRUE.equals(fileStorageProperties.isNeed_dataType())) {
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
            writeCsvFile(sessionDataSet, finalFilePath , headers, timestampPrecision);
            sessionPool.closeResultSet(dataSetWrapper);
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

    public void writeCsvFile(SessionDataSet sessionDataSet,
                             String finalFilePath,
                             List<Object> headers,
                             String timeStampPrecision
    ) throws IOException, IoTDBConnectionException, StatementExecutionException {

        final CSVPrinterWrapper csvPrinterWrapper = new CSVPrinterWrapper(finalFilePath);
        csvPrinterWrapper.printRecord(headers);
        while (sessionDataSet.hasNext()) {
            RowRecord rowRecord = sessionDataSet.next();
            if (rowRecord.getTimestamp() != 0) {
                csvPrinterWrapper.print(timeTrans(rowRecord.getTimestamp(), timeStampPrecision));
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

    public static String timeTrans(Long time, String timeStampPrecision) {
        ExportData.timeFormat = "default";
        ExportData.zoneId = ZoneId.systemDefault();
        switch (timeFormat) {
            case "default":
                return RpcUtils.parseLongToDateWithPrecision(
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME, time, zoneId, timeStampPrecision
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

