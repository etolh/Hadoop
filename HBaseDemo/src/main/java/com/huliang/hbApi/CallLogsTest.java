package com.huliang.hbApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * 通话记录rowKey设计：
 * 拨打记录：（hashcode，拨打人电话1 - caller，拨打时间 - callTime，方向0，接通人电话2 - receiver，通话时间10 - duration）
 * 通话记录：（hashcode，接通人电话1，拨打时间，方向1，拨打人电话2，通话时间10）
 *
 * 为通话记录分配100个region：00-99
 *
 * hashcode = ( 电话1 + 拨打时间年月) . hashCode()
 *
 * @author huliang
 * @date 2018/10/14 21:23
 */
public class CallLogsTest {

    /**
     * 测试拨打记录
     */
    public void putCallLogs() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns2:dialogs");
        Table table = connection.getTable(tableName);

        String caller = "13311111111";
        String receiver = "15511111111";
        // 拨打时间，格式化
        SimpleDateFormat dateFormat = new SimpleDateFormat();
        dateFormat.applyPattern("yyyyMMddHHmmss");
        String callTime = dateFormat.format(new Date());
        int duration = 10;    // 通话时间

        // rowKey哈希前缀
        String prefix = PrefixUtil.getRowPrefix(caller, callTime);
        // 组合rowKey [hashcode，拨打人，拨打时间，方向0，接通人，通话时间]
        String rowKey = prefix + "," + caller + "," + callTime + ",0," + receiver + "," + duration;

        // 插入数据
        Put callLog = new Put(Bytes.toBytes(rowKey));
        callLog.addColumn(Bytes.toBytes("telRecords"), Bytes.toBytes("callSite"), Bytes.toBytes("hubei"));
        callLog.addColumn(Bytes.toBytes("telRecords"), Bytes.toBytes("recSite"), Bytes.toBytes("zhejiang"));
        table.put(callLog);

    }


    /**
     * 批量测试拨打记录
     */
    public void putBatchCallLogs() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns2:dialogs");
        Table table = connection.getTable(tableName);

        for(int i = 0; i < 10; i++) {

            String suf = PrefixUtil.getTelSuffix(Integer.toString(i));
            String caller = "133"+suf;
            String receiver = "155"+suf;
            // 拨打时间，格式化
            SimpleDateFormat dateFormat = new SimpleDateFormat();
            dateFormat.applyPattern("yyyyMMddHHmmss");
            String callTime = dateFormat.format(new Date());
            int duration = 10;    // 通话时间

            // rowKey哈希前缀
            String prefix = PrefixUtil.getRowPrefix(caller, callTime);
            // 组合rowKey [hashcode，拨打人，拨打时间，方向0，接通人，通话时间]
            String rowKey = prefix + "," + caller + "," + callTime + ",0," + receiver + "," + duration;

            // 插入数据
            Put callLog = new Put(Bytes.toBytes(rowKey));
            callLog.addColumn(Bytes.toBytes("telRecords"), Bytes.toBytes("callSite"), Bytes.toBytes("hubei"));
            callLog.addColumn(Bytes.toBytes("telRecords"), Bytes.toBytes("recSite"), Bytes.toBytes("zhejiang"));
            table.put(callLog);
            System.out.println("over");
        }
    }

    /**
     * 根据电话号码和时间查询客户的通话记录（拨打记录和接通记录）
     */
    public void selectWithTelDate() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns2:dialogs");
        Table table = connection.getTable(tableName);

        // 由tel和date组合rowKey前缀
        String tel = "13311111111";
        String date = "201810";
        String rowKeyPrefix = PrefixUtil.getRowPrefix(tel, date);
        // rowKey前面部分：[hashcode, tel, date]
        String startKey = rowKeyPrefix + "," + tel + "," + date; // 起始rowKey
        String endKey = rowKeyPrefix + "," + tel + "," + "201811";  // 结尾rowKey

        Scan scan = new Scan();
        // 设置查询起止rowKey，定义查询区间
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));

        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it =  rs.iterator();
        while (it.hasNext()) {
            Result row = it.next();
            System.out.println(Bytes.toString(row.getRow()));
        }
    }

    public static void main(String[] args) throws Exception {
        CallLogsTest test = new CallLogsTest();
//        test.putCallLogs();
//        test.putBatchCallLogs();
        test.selectWithTelDate();
    }
}