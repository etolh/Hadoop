package com.huliang.stormdemo.util;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 获取当前执行方法的对象、线程、进程等信息，并利用socket编程发送给nc服务器
 * @author huliang
 * @date 2018/10/19 15:22
 */
public class InfoUtil {

    // 发送(进程+线程+对象+方法)信息到nc服务器
    public static void sendToLocal(Object obj,String msg) {
        try {
            String info = info(obj, msg);
            Socket socket = new Socket("localhost", 8888);
            OutputStream os = socket.getOutputStream();
            // 输出信息
            os.write((info + "\r\n").getBytes());
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 发送(进程+线程+对象+方法)信息到nc服务器
    public static void sendToNcServer(Object obj,String msg, int port) {
        try {
            String info = info(obj, msg);
            Socket socket = new Socket("s201", port);
            OutputStream os = socket.getOutputStream();
            // 输出信息
            os.write((info + "\r\n").getBytes());
            os.flush();
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
