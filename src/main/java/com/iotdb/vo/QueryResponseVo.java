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
    /**
     * 数据源
     */
    private int dataSource;
    /**
     * 业务类型
     */
    private int serviceType;
    /**
     * 点位数据
     */
    private List<DataVo> dataVoList;
}
