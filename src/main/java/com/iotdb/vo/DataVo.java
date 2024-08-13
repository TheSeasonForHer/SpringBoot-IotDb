package com.iotdb.vo;/**
 * @Author：tjb
 * @Date：2024/8/13 0:12
 */

import com.iotdb.dto.DataItemDto;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @version 1.0
 * @author tjb
 * @date 2024/8/13 0:12
 */
@Data
@Builder
/**
 * 点位数据
 */
public class DataVo {
    private String id;
    private String dataType;
    private Long dataCount;
    // 数据项
    List<DataItemDto> dataItems;
}
