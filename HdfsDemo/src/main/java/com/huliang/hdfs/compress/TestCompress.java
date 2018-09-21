package com.huliang.hdfs.compress;

import com.hadoop.compression.lzo.LzoCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Hdfs压缩测试及远程调试
 * @author huliang
 * @date 2018/8/31 21:52
 */
public class TestCompress {

    public static void main(String[] args) throws Exception {

        Class[] codecs = new Class[]{
                DeflateCodec.class,
                GzipCodec.class,
                BZip2Codec.class,
                Lz4Codec.class,
                SnappyCodec.class,
                LzoCodec.class
        };

        System.out.println("============ zip ===============");

        for (Class codec : codecs) {
            zip(codec);
        }

        System.out.println("============== uzip =============");

        for (Class codec : codecs) {
            uzip(codec);
        }
    }

    // 无论压缩还是解压，都是对压缩文件的流进行封装
    // 压缩时，对压缩文件的输出流封装；解压时，对压缩文件的输入流封装。

    /**
     * 根据压缩格式类进行压缩
     * @param codecClass
     */
    public static void zip(Class codecClass) throws Exception {

        long start = System.currentTimeMillis();

//        String path = "F:\\bigdata\\test";
        String path = "/home/centos/zip";
        // 被压缩文件的输入流 -- 程序角度（相对于文件为输出流、读取流）
        FileInputStream fis = new FileInputStream(path+"/b.txt");

        // 压缩器
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        // 压缩文件的输出流,获取默认的扩展名
        FileOutputStream fos = new FileOutputStream(path+"/b" + codec.getDefaultExtension());
        // 将被压缩文件的输出流封装为压缩输出流 - 程序角度 （相对于文件为输入流）
        CompressionOutputStream cout = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cout, 1024);

        cout.close();
        long end = System.currentTimeMillis();

        System.out.println(codecClass.getSimpleName() + " : " + (end - start));
    }

    /**
     * 解压
     * @param codecClass
     */
    public static void uzip(Class codecClass) throws IOException {

        long start = System.currentTimeMillis();
//        String path = "F:\\bigdata\\test";
        String path = "/home/centos/zip";

        // 编解码器
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        // 压缩文件
        String zipFile = path+"/b" + codec.getDefaultExtension();
        // 程序输入流 （相对于解压文件角度，为输出流）
        FileInputStream fis = new FileInputStream(zipFile);
        // 对压缩文件的输入流进行解压，得到解压流
        CompressionInputStream cis = codec.createInputStream(fis);

        // 程序输出流 （相对于解压文件为输入流）
        FileOutputStream fos = new FileOutputStream(path+"/uzip_b"+codec.getDefaultExtension()+".txt");
        IOUtils.copyBytes(cis, fos, 1024);
        cis.close();

        long end = System.currentTimeMillis();
        System.out.println(codecClass.getSimpleName() + " : " + (end - start));
    }
}
