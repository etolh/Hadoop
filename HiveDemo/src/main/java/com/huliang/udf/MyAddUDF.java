package com.huliang.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 自定义UDF加法：接收2个或3个参数
 * @author huliang
 * @date 2018/10/8 15:34
 */

@Description(name="myadd",
value="_FUNC_(value, default_value) - return value + default_value",
extended="Example:\n "
        + "myadd(1,2) = 3 \n" +
        "myadd(1,2,3) = 6")
public class MyAddUDF extends UDF {

    public int evaluate(int a, int b) {
        return a + b;
    }

    public int evaluate(int a, int b, int c) {
        return a + b + c;
    }
}
