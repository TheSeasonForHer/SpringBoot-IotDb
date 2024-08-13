package com.iotdb.dto;/**
 * @Author：tjb
 * @Date：2024/8/13 0:16
 */

import lombok.Builder;
import lombok.Data;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/8/13 0:16
 */
@Data
@Builder
public class DataItemDto {
    // 时间戳
    private Long timestamp;
    // 值
    private Object value;
}
