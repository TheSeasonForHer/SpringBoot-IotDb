package com.iotdb.exception;

import lombok.Getter;

/**
 * @Date: 2022/8/21/021 上午 9:18
 * @Author: ljm
 * @Description:
 */
@Getter
public class ServiceException extends RuntimeException {

    private Integer code;

    public ServiceException(Integer code, String msg) {
        super(msg);
        this.code = code;
    }
}
