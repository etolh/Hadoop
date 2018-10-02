package com.huliang.hdfs.rackaware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义机架感知类
 * @author huliang
 * @date 2018/9/30 16:26
 */
public class MyRackAwareness implements DNSToSwitchMapping {

    /**
     * 解析ip或host，返回网络拓扑路径 /foo/swt
     * @param list
     * @return
     */
    public List<String> resolve(List<String> list) {

        ArrayList<String> networkPaths = new ArrayList<String>();
        FileWriter fw = null; // 记录
        try {
            fw = new FileWriter("/home/centos/rackaware.txt", true);

            for (String str : list) {

                fw.write(str+"\r\n");   // 记录
                String lastIp = null;
                // 获取ip地址主机名
                if(str.startsWith("192"))
                    lastIp = str.substring(str.lastIndexOf('.')+1); // 最后.后面
                else if(str.startsWith("s"))
                    lastIp = str.substring(1);

                int host = Integer.parseInt(lastIp);

                if(host <= 203)
                    networkPaths.add("/rack1/"+ host);  // 机架1
                else
                    networkPaths.add("/rack2/"+host);  // 机架2
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return networkPaths;

    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> list) {

    }
}
