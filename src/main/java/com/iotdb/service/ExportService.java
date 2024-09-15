package com.iotdb.service;

import com.iotdb.dto.QueryDto;


/**
 * @Author：tjb
 * @Date：2024/9/13 0:32
 */
public interface ExportService {
    public void export(QueryDto  queryDto);
}
