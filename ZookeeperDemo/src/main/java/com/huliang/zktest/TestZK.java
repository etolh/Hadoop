package com.huliang.zktest;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author huliang
 * @date 2018/10/9 16:08
 */
public class TestZK {

    /**
     * api访问zk, 列出根目录下子目录
     */
    public void ls() throws IOException, KeeperException, InterruptedException {
        String connUrl = "192.168.44.201:2181";
        // 连接时关闭服务器
        ZooKeeper zk = new ZooKeeper(connUrl, 5000, null);
        List<String> dirlist = zk.getChildren("/", null);
        for (String path:dirlist)
            System.out.println(path);
    }

    /**
     * api访问zk, 列出指定目录下所有子目录
     */
    public void lsAll(String path) throws IOException, KeeperException, InterruptedException {

        String connUrl = "192.168.44.201:2181";
        // 连接时关闭服务器
        ZooKeeper zk = new ZooKeeper(connUrl, 5000, null);
        List<String> dirlist = zk.getChildren(path, null);

        String newpath = null;
        for (String s : dirlist) {
            System.out.println(s);

            if (path.equals("/"))
                newpath = path + s;
            else
                newpath = path + "/" + s;
            lsAll(newpath);
        }
    }

    /**
     * 设置数据: 路径已存在
     */
    public void setData() throws IOException, KeeperException, InterruptedException {
        String connUrl = "192.168.44.201:2181";
        ZooKeeper zk = new ZooKeeper(connUrl, 5000, null);
        // 设置数据时，当该路径存在时，设置的版本数一定要和zookeeper中版本数一致
        // 乐观锁机制，当其对该路径进行修改时，首先会读取服务器中版本，当两者一致时，才会进行更新，并修改版本
        zk.setData("/t1", "d111".getBytes(), 1);
    }

    /**
     * 创建节点
     * create(final String path, byte data[], List<ACL> acl,
     *             CreateMode createMode)
     * acl: 操作权限 ZooDefs.Ids.OPEN_ACL_UNSAFE 完全开发权限，任何用户都可以执行CRUD操作
     * 节点类型：CreateMode.EPHEMERAL 临时节点  CreateMode.PERSISTENT  持久节点
     */
    public void createData() throws IOException, KeeperException, InterruptedException {
        String connUrl = "192.168.44.201:2181";
        ZooKeeper zk = new ZooKeeper(connUrl, 5000, null);
        zk.create("/t3", "d3".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    /**
     * 测试监听集群回调函数
     */
    public void testWatch() throws IOException, KeeperException, InterruptedException {

        String connUrl = "192.168.44.201:2181";
        final ZooKeeper zk = new ZooKeeper(connUrl, 5000, null);
        final Watcher watcher = new Watcher() {
            // 回调函数，执行响应，并重新注册
            public void process(WatchedEvent event) {
                System.out.println("data change!");
                try {
                    // 重新注册
                    zk.getData("/t1", this, new Stat());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        byte[] data = zk.getData("/t1", watcher, new Stat());
        System.out.println(new String(data));

        while(true){
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) throws Exception {
        TestZK test = new TestZK();
//        test.ls();
//        test.lsAll("/");
//        test.setData();
//        test.createData();
        test.testWatch();
    }
}
