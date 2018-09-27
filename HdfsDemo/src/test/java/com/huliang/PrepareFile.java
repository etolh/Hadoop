package com.huliang;

import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 *
 * 生成各种测试文件
 * @author huliang
 * @date 2018/9/27 21:14
 */
public class PrepareFile {

    /**
     * 生成Temperature测试文件
     */
    @Test
    public void genTemperatureFile() {

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter("F:\\bigdata\\test\\maxtemp\\temp.txt");

            // 随机创建1950~2050年 温度数据 10000条
            Random random = new Random();
            for (int i = 0; i < 10000; i++) {
                int year = 1950 + random.nextInt(100);
                int temp = -50 + random.nextInt(100);
                fileWriter.write(year + " "+temp+"\r\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
