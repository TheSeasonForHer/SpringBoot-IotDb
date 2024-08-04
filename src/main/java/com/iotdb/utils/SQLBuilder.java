package com.iotdb.utils;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
/**
 * @author tjb
 * @date 2024/8/2
 */
public class SQLBuilder {

    private StringBuilder sql;

    public SQLBuilder() {
        this.sql = new StringBuilder();
    }

    public SQLBuilder insertInto(String tableName) {
        sql.append("INSERT INTO ").append(tableName).append(" ");
        return this;
    }

    public SQLBuilder columns(String... columns) {
        sql.append("(");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i]);
            if (i < columns.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(") ");
        return this;
    }

    public SQLBuilder values(Object... values) {
        sql.append("VALUES (");
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof String) {
                sql.append("'").append(values[i]).append("'");
            } else {
                sql.append(values[i]);
            }
            if (i < values.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(")");
        return this;
    }
    public SQLBuilder select(String... columns) {
        sql.append("SELECT ");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i]);
            if (i < columns.length - 1) {
                sql.append(", ");
            }
        }
        sql.append(" ");
        return this;
    }
    public SQLBuilder selectAggregation(String aggregation){
         sql.append("SELECT ").append(aggregation).append(" ");
         return this;
    }
    public SQLBuilder deleteFrom(String tableName) {
        sql.append("DELETE FROM ").append(tableName);
        return this;
    }

    public SQLBuilder from(String tableName) {
        sql.append("FROM ").append(tableName);
        return this;
    }

    public SQLBuilder where(String condition) {
        sql.append(" WHERE ").append(condition);
        return this;
    }

    public SQLBuilder groupBy(String column) {
        sql.append(" GROUP BY ").append(column);
        return this;
    }

    public SQLBuilder having(String condition) {
        sql.append(" HAVING ").append(condition);
        return this;
    }

    public SQLBuilder limit(long limit) {
        sql.append(" LIMIT ").append(limit);
        return this;
    }

    public String build() {
        return sql.toString();
    }
    public static void main(String[] args) throws InterruptedException {
        long limit = -1;
        //1限定数据大小
        List<String>  list = new ArrayList<>();
        list.add("AV");
        list.add("EV");
        String queryByLimit = new SQLBuilder()
                .select(list.stream().collect(Collectors.joining(",")))
                .from("deviceId")
                .limit(limit > 0 ? list.size() : 2000)
                .build();
        System.out.println(queryByLimit);
        //2开始结束时间
        long start = System.currentTimeMillis();
        Thread.sleep(1);
        long end = System.currentTimeMillis();
        if (start < 0 || end < 0) {
            return;
        }
        String queryByTimeRange = new SQLBuilder()
                .select(list.stream().collect(Collectors.joining(",")))
                .from("deviceId")
                .where("time >=" + start + " and time <=" + end)
                .build();
        System.out.println(queryByTimeRange);
        //3最大最小时间
        String queryByAgg = new SQLBuilder()
                .select("MAX_TIME(" + list.get(0) + ") as maxTime,MIN_TIME(" + list.get(1) + ") as minTime")
                .from("deviceId")
                .build();
        System.out.println(queryByAgg);
        //4获取极值 MAX_VALUE , MIN_VALUE
        String selectType = "MAX_VALUE";
        String queryExtreme = null;
        switch (selectType){
            case "MAX_VALUE":
                queryExtreme = new SQLBuilder()
                        .select("MAX_VALUE(" + list.get(0) + ")")
                        .from("deviceId")
                        .build();
                break;
            case "MIN_VALUE":
                queryExtreme = new SQLBuilder()
                        .select("MIN_VALUE(" + list.get(0) + ")")
                        .from("deviceId")
                        .build();
                break;
            default:
                throw new InvalidParameterException("the parameter select type is valid:" + selectType);
        }

        //5 获取测点数据的总量 时间可选
        SQLBuilder sql = new SQLBuilder()
                .select("count(" + list.get(0) + ")")
                .from("deviceId");
        if (start < end){
            sql.where("time >=" + start + " and time <=" + end);
        }
        String selectCountByTimeRange = sql.build();
        System.out.println(selectCountByTimeRange);
        //6 不会

        //删除
        SQLBuilder deleteDataBuilder = new SQLBuilder()
                .deleteFrom("deviceId + 测点");
        if (start < end){
            sql.where("time >=" + start + " and time <=" + end);
        }


    }
}