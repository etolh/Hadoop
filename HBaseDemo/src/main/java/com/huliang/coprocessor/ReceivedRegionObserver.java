package com.huliang.coprocessor;

import com.huliang.hbApi.PrefixUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;

/**
 * 拨打记录感知器：当感知到拨打记录时，读取拨打记录数据，组装插入拨打记录
 *
 * @author huliang
 * @date 2018/10/14 21:45
 */
public class ReceivedRegionObserver extends BaseRegionObserver {

    /**
     * 在拨打记录插入后，回调该函数
     * 读取拨打记录0，读取数据组装插入接通记录1，
     * 同时为了避免循环调用（接通记录插入时，进行回调），需要进行判断只对拨打记录0插入才处理
     */
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);

        FileWriter fw = new FileWriter("/home/centos/callLogs.txt",true);

        // 由上下文获取插入的表名称
        String curTableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();

        // 只对ns2:dialogs表进行的put操作才进行反向插入通话记录
        if(!curTableName.equals("ns2:dialogs"))
            return;

        // 获取Put数据，主要数据在rowKey
        String rowKey = Bytes.toString(put.getRow()); // rowKey信息
        String[] data = rowKey.split(",");
        String direction = data[3];

        fw.write(curTableName+":"+direction+"\r\n");

        if(direction.equals("1"))
            return; // 接通记录，不执行

        String caller = data[1];
        String receiver = data[4];
        String callTime = data[2];
        String duration = data[5];

        // 创建接通记录
        // 接通前缀：接通号码+时间
        String recPrefix = PrefixUtil.getRowPrefix(receiver, callTime);
        // 接通rowKey: [hashcode，接通人，拨打时间，方向1，拨打人，通话时间]
        String recRowKey = recPrefix + "," + receiver + "," + callTime + ",1," + caller + "," + duration;
        // 由上下文环境获取表对象
        Table table = e.getEnvironment().getTable(TableName.valueOf("ns2:dialogs"));

        Put recLog = new Put(Bytes.toBytes(recRowKey));

        // 注意在 ns2:dialogs 需要创建相应列族recLogs
        recLog.addColumn(Bytes.toBytes("recLogs"), Bytes.toBytes("info"), Bytes.toBytes("no"));
        table.put(recLog);

        fw.close();
    }

    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        super.prePut(e, put, edit, durability);
    }
}
