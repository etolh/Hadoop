package com.huliang.mr;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * MR运行信息工具：
 * 打印当前运行：hostname(主机) --> PID(进程id) -> TID(线程) -> OID@hashCode(对象id@哈希码)
 * @author huliang
 * @date 2018/9/27 19:18
 */
public class MRInfoUtil {

    public static String info(Object obj, String msg) {
        return getHostname() + ":" + getPID() + ":" + getTID() + ":" + getObjInfo(obj) + ":" + msg;
    }

    // 主机名
    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 进程id
    public static int getPID() {
        String info = ManagementFactory.getRuntimeMXBean().getName();   // PID@hostname
        return Integer.parseInt(info.substring(0, info.indexOf("@")));
    }

    // 线程id
    public static String getTID() {
        return Thread.currentThread().getName();
    }

    // OID@hashCode
    public static String getObjInfo(Object obj) {
        String oName = obj.getClass().getSimpleName();
        return oName + "@" + obj.hashCode();
    }
}
