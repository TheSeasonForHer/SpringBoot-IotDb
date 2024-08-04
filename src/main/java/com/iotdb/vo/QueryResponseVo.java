package com.iotdb.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
/**
 * @author tjb
 * @date 2024/8/2
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QueryResponseVo {
    public Map<String, Object> resultMap;
}
