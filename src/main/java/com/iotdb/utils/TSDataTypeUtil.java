package com.iotdb.utils;


import cn.hutool.core.date.DateTime;
import com.iotdb.exception.ServiceException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import static com.iotdb.enums.StatusCodeEnum.VALID_ERROR;

/**
 * @author tjb
 * @date 2024/8/2
 */
public class TSDataTypeUtil {
    public static TSDataType getTsDataType(String type) {
        String upperCase = type.toUpperCase();
        switch (upperCase) {
            case "BOOLEAN":
                return TSDataType.BOOLEAN;
            case "INT":
                return TSDataType.INT32;
            case "LONG":
                return TSDataType.INT64;
            case "FLOAT":
                return TSDataType.FLOAT;
            case "DOUBLE":
                return TSDataType.DOUBLE;
            case "STRING":
                return TSDataType.STRING;
            case "TXT":
                return TSDataType.TEXT;
            case "TIMESTAMP":
                return TSDataType.TIMESTAMP;
            case "BLOB":
                return TSDataType.BLOB;
            case "DATE":
                return TSDataType.DATE;
            default:
                throw new ServiceException(VALID_ERROR.getCode(),"Invalid input type: " + type);
        }
    }
    public static Object getValueByFiled(Field field) {
        TSDataType dataType = field.getDataType();
        byte type = dataType.getType();
        switch (type) {
            case 0:
                // BOOLEAN
                return field.getBoolV();
            case 1:
                // INT32;
                return field.getIntV();
            case 2:
                // INT64;
                return field.getLongV();
            case 3:
                // return FLOAT;
                return field.getFloatV();
            case 4:
                // return DOUBLE;
                return field.getDoubleV();
            case 5:
                // return TEXT;
                return field.getStringValue();
            case 8:
                // return TIMESTAMP;
                return field.getDataType();
            case 9:
                // return DATE;
                return field.getDateV();
            case 10:
                // return BLOB;
                return field.getBinaryV();
            case 11:
                // return STRING;
                return field.getStringValue();
            default:
                throw new ServiceException(VALID_ERROR.getCode(),"Invalid input type: " + type);
        }
    }
    public static Object getValueByData(int dataType,String data) {
        switch (dataType) {
            case 0:
                // BOOLEAN
                return Boolean.parseBoolean(data);
            case 1:
                // INT32;
                return Integer.parseInt(data);
            case 2:
                // INT64;
                return Long.parseLong(data);
            case 3:
                // return FLOAT;
                return Float.parseFloat(data);
            case 4:
                // return DOUBLE;
                return Double.parseDouble(data);
            case 5:
                // return TEXT;
                return data;
            case 8:
                // return TIMESTAMP;
                return Long.parseLong(data);
            case 9:
                // return DATE;
                return DateTime.of(Long.parseLong(data));
            case 10:
                // return BLOB;
                return data;
            case 11:
                // return STRING;
                return data;
            default:
                throw new IllegalArgumentException("Invalid input: " + dataType);
        }
    }
}
