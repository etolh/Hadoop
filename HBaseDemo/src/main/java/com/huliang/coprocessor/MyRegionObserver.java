package com.huliang.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * 实现Observer协处理器：观察HBase执行操作顺序
 * @author huliang
 * @date 2018/10/14 19:24
 */
public class MyRegionObserver extends BaseRegionObserver {

    /**
     * 控制信息输出
     * @param msg
     */
    private void outInfo(String msg) {

        try {
            FileWriter fw = new FileWriter("/home/centos/hbase-coprocessor.txt",true);
            fw.write(msg + "\r\n");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        outInfo("MyRegionObserver.start()");
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        outInfo("MyRegionObserver.stop()");
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> e) throws IOException {
        super.preOpen(e);
        outInfo("MyRegionObserver.preOpen()");
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        super.postOpen(e);
        outInfo("MyRegionObserver.postOpen()");
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.preGetOp(e, get, results);
        // 查看get信息
        String rowKey = Bytes.toString(get.getRow());
        outInfo("MyRegionObserver.preGetOp() : rowkey = " + rowKey);
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.postGetOp(e, get, results);
        String rowKey = Bytes.toString(get.getRow());
        outInfo("MyRegionObserver.postGetOp() : rowkey = " + rowKey);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
        String rowKey = Bytes.toString(put.getRow());
        outInfo("MyRegionObserver.prePut() : rowkey = " + rowKey);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        String rowKey = Bytes.toString(put.getRow());
        outInfo("MyRegionObserver.postPut() : rowkey = " + rowKey);
    }
}
