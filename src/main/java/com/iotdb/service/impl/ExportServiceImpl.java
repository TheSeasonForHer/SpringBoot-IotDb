package com.iotdb.service.impl;

import com.iotdb.dto.QueryDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.ExportService;
import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tool.AbstractDataTool;
import org.apache.iotdb.tool.ExportData;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/9/13 0:34
 */
@Service
public class ExportServiceImpl extends AbstractDataTool implements ExportService {

    @Resource
    private SessionPool sessionPool;

    private final String exportDirectory = "D:\\";
    private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);
    private final Object lock = new Object();

    @Override
    public void export(QueryDto queryDto) {
//        if (Objects.isNull(queryDto.getTimeSeriesDto())){
//            throw new ServiceException(VALID_ERROR.getCode(), "");
//        }
        new Thread(() -> submitExportRequest(queryDto)).start();
    }

    public void submitExportRequest(QueryDto queryDto) {
        // 创建一个新的任务ID
        String taskId = UUID.randomUUID().toString();
        // 查询数据
        // 模拟导出过程
        dumpResult("select * from root.tjb.d", 0, exportDirectory);
        //notifyUserOfCompletion(userId, taskId);
    }
    public void dumpResult(String sql, int index, String path) {
        legalCheck(sql);
        path = path + index;
        try {
            SessionDataSet sessionDataSet = this.sessionPool.executeQueryStatement(sql).getSessionDataSet();
            List<Object> headers = new ArrayList<>();
            List<String> names = sessionDataSet.getColumnNames();
            List<String> types = sessionDataSet.getColumnTypes();

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
            ExportData.writeCsvFile(sessionDataSet, path, headers, 200);
            sessionDataSet.closeOperationHandle();
            ioTPrinter.println("Export completely!");
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            ioTPrinter.println("Cannot dump result because: " + e.getMessage());
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
            ioTPrinter.println("The sql you entered is invalid, please don't use aggregate query.");
        }
    }

}
