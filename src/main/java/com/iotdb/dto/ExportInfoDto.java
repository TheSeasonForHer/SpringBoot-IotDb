package com.iotdb.dto;/**
 * @Author：tjb
 * @Date：2024/9/16 4:53
 */

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/9/16 4:53
 */
@Data
@AllArgsConstructor
public class ExportInfoDto {
    private String exportSql;
    private String filePath;
    private String downloadUrl;
    private String userId;
    private String userName;

}
