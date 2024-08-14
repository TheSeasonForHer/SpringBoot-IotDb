package com.iotdb.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ServiceTypeConstants {
    PRETEND_REAL("仿真", 1),

    RUNNING("运行", 2),

    HISTORY("历史", 3);
    /**
     * 业务类型描述
     */
    private String info;
    /**
     * 对应的code
     */
    private int code;
}
