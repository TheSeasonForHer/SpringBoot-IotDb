package com.iotdb.service.impl;/**
 * @Author：tjb
 * @Date：2024/9/13 0:34
 */

import com.iotdb.dto.QueryDto;
import com.iotdb.service.ExportService;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/9/13 0:34
 */
@Service
public class ExportServiceImpl implements ExportService {
    @Override
    public List<Map<String, Object>> export(QueryDto queryDto) {
        return null;
    }
}
