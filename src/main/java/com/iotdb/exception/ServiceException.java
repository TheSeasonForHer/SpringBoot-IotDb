package com.iotdb.exception;

import lombok.Getter;

/**
 * @author tjb
 * @date 2024/8/2
 */
@Getter
public class ServiceException extends RuntimeException {

    private Integer code;

    public ServiceException(Integer code, String msg) {
        super(msg);
        this.code = code;
    }
}
