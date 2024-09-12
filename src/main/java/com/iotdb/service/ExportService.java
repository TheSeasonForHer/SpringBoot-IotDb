package com.iotdb.service;

import com.iotdb.dto.QueryDto;

import java.util.List;
import java.util.Map;

/**
 * @Author：tjb
 * @Date：2024/9/13 0:32
 */
public interface ExportService {
    public List<Map<String, Object>> export(QueryDto  queryDto);
}
