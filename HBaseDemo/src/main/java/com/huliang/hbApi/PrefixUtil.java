package com.huliang.hbApi;

import java.text.DecimalFormat;

/**
 * rowKey哈希前缀工具类
 * @author huliang
 * @date 2018/10/14 21:33
 */
public class PrefixUtil {

    public static String getRowPrefix(String phoneNum, String callTime) {
        int hashCode = (phoneNum + callTime.substring(0, 6)).hashCode();
        hashCode = (hashCode & Integer.MAX_VALUE) % 100;    // 保证非负，与Integer.MAX_VALUE相与
        DecimalFormat format = new DecimalFormat();
        format.applyPattern("00"); // 格式化
        return format.format(hashCode);
    }

    public static String getTelSuffix(String str) {
        String rs = "";
        for(int i = 0; i < 8; i++)
            rs += str;
        return rs;
    }
}
