package com.iotdb.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
/**
 * @author tjb
 * @date 2024/8/2
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryResponseVo {
    private String dataSource;
    private String serviceType;
    // 点位数据
    private List<DataVo> dataVoList;
}
