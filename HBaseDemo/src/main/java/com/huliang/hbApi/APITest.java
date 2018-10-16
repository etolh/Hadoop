package com.huliang.hbApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * HBase: Java API操作HBase数据库
 * @author huliang
 * @date 2018/10/10 19:49
 */
public class APITest {

    /**
     * 插入数据
     */
    public void testPut() throws IOException {

        // 创建conf对象，由addHbaseResources读取hbase-default.xml和hbase-site.xml配置
        Configuration conf = HBaseConfiguration.create();
        // 创建连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 通过conn查询TableName对象
        TableName tname = TableName.valueOf("ns1:t1");
        // 获取Table
        Table table = conn.getTable(tname);

        Put put = new Put("r3".getBytes()); // 插入内容，所有行id、列族、列、值都以字节数组形式存在
        put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));  // 列和值
        put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("tom3"));  // 列和值

        // 插入
        table.put(put);
    }

    /**
     * 测试获取
     * @throws IOException
     */
    public void testGet() throws IOException {

        // 创建conf对象，由addHbaseResources读取hbase-default.xml和hbase-site.xml配置
        Configuration conf = HBaseConfiguration.create();
        // 创建连接
        Connection conn = ConnectionFactory.createConnection(conf);

        // 通过conn查询TableName对象
        TableName tname = TableName.valueOf("ns1:t1");
        // 获取Table
        Table table = conn.getTable(tname);

        Get get = new Get("r3".getBytes());
        Result rs =  table.get(get);
        Cell cell = rs.getColumnLatestCell("f1".getBytes(), "id".getBytes());
        byte[] idValue = cell.getValueArray();

        byte[] nameValue = rs.getValue("f1".getBytes(), "name".getBytes());
        System.out.println("id:"+Bytes.toInt(idValue)+"\tname:"+Bytes.toString(nameValue));
    }

    /**
     * 测试百万插入,格式化rowkey, rowkey以00000模式格式化，即row00011,使得rowkey有序
     *
     * @throws IOException
     */
    public void testMillionsPut() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tname = TableName.valueOf("ns1:t1");
        HTable table = (HTable) conn.getTable(tname);  // HTable是Table接口的实现类

        // 关闭自动清理缓冲区，默认每插入一条数据到缓存区，自动flush到hbase中去
        table.setAutoFlush(false);

        long start = System.currentTimeMillis() ;

        DecimalFormat format  = new DecimalFormat();
        format.applyPattern("00000");   // 万位数据插入

        for(int i = 4; i < 10000; i++) {   // 100万数据，1万数据
            // 插入内容，所有行id、列族、列、值都以字节数组形式存在
            Put put = new Put(Bytes.toBytes("r"+format.format(i)));
            // 关闭写前日志
            put.setWriteToWAL(false);
//            put.setDurability(Durability.SKIP_WAL);
            put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(i));  // id
            put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("tom"+i));  // name
            put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(i%100));  // age

            // 写入缓存区
            table.put(put);

            if(i % 2000 == 0) {
                // 每隔2000条flush
                table.flushCommits();
            }
        }

        table.flushCommits();
        // 测试写入时间
        System.out.println(System.currentTimeMillis() - start );
    }

    // 数据格式化
    public void formatNum() {
        DecimalFormat format = new DecimalFormat();
        format.applyPattern("00000");   // 设置模式
        System.out.println(format.format(88));
    }

    // 基于admin操作元数据

    // 创建namespace
    public void createNamespace() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取admin对象，可进行hbase元数据操作
        Admin admin = conn.getAdmin();
        // 创建namespace描述符
        NamespaceDescriptor descriptor =  NamespaceDescriptor.create("ns2").build();
        admin.createNamespace(descriptor);

        // 列出所有namespace
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for(NamespaceDescriptor n: namespaceDescriptors) {
            System.out.println(n.getName());
        }
    }

    // 创建table
    public void createTable() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        // 创建表名对象
        TableName tableName = TableName.valueOf("ns2:t2");
        // 创建表描述符
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        // 创建列族描述符
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("f1");
        descriptor.addFamily(columnDescriptor);
        admin.createTable(descriptor);
    }

    // 禁用
    public void disableTable() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        // disableTable()禁用表 enableTable() 启用表
        admin.disableTable(TableName.valueOf("ns2:t2"));
//        admin.enableTable(TableName.valueOf("ns2:t2"));
    }

    // 删除表
    public void dropTable() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("ns2:t2");
        if(admin.isTableEnabled(tableName))
            disableTable(); // 先禁用
        admin.deleteTable(tableName);
    }

    // 删除数据
    public void delteTableData() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns2:t2");

        Table t2 = conn.getTable(tableName);
        Delete del = new Delete("r1".getBytes());
        del.addColumn("f1".getBytes(), "id".getBytes());
        del.addColumn("f1".getBytes(), "name".getBytes());
        t2.delete(del);
    }

    // scan
    public void scanTable() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table t2  = conn.getTable(tableName);
        Scan scanner = new Scan();
        // 设置起止rowkey
        scanner.setStartRow("r05000".getBytes());
        scanner.setStopRow("r05020".getBytes());

        ResultScanner rs =  t2.getScanner(scanner);
        Iterator<Result> it =  rs.iterator();
        while (it.hasNext()) {
            Result result = it.next();  // 每个result对应一行所有数据
            int id = Bytes.toInt(result.getValue("f1".getBytes(), "id".getBytes()));
            String name = new String(result.getValue("f1".getBytes(), "name".getBytes()));
            System.out.println(id+":"+name);
        }

        rs.close(); // 关闭扫描器
    }

    // 动态scan，获取所有信息
    public void scanTable2() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table t2  = conn.getTable(tableName);
        Scan scanner = new Scan();
        // 设置起止rowkey
        scanner.setStartRow("r05000".getBytes());
        scanner.setStopRow("r05020".getBytes());

        ResultScanner rs =  t2.getScanner(scanner);
        Iterator<Result> it =  rs.iterator();
        while (it.hasNext()) {
            Result result = it.next();
            Map<byte[], byte[]> map =  result.getFamilyMap("f1".getBytes());   // 获取列族
            // 遍历列族中所有列
            for(Map.Entry<byte[], byte[]> entry : map.entrySet()) {
                String col = new String(entry.getKey());    // 获取列名
                String colValue = new String(entry.getValue()); // 获取列对应的值
                System.out.print(col+":"+colValue+"\t");
            }
            System.out.println();
        }

        rs.close(); // 关闭扫描器
    }

    // 动态scan，获取所有信息，每次按行获取
    public void scanTable3() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table t2  = conn.getTable(tableName);
        Scan scanner = new Scan();
        // 设置起止rowkey
        scanner.setStartRow("r05000".getBytes());
        scanner.setStopRow("r05020".getBytes());

        ResultScanner rs =  t2.getScanner(scanner);
        Iterator<Result> it =  rs.iterator();
        while (it.hasNext()) {
            Result result = it.next();

            // map对应一行的所有信息：key=f1,value=Map<Col,Map<Timestamp,value>>
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();

            // 遍历列族中所有列
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> colFamily : map.entrySet()) {

                String f = new String(colFamily.getKey());
                NavigableMap<byte[], NavigableMap<Long, byte[]>> colTs = colFamily.getValue();    // 列以及其对应的所有时间戳和值

                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> colTsEntry: colTs.entrySet()) {

                    String c = new String(colTsEntry.getKey()); // 列
                    NavigableMap<Long, byte[]> colVal = colTsEntry.getValue();  // 获取时间戳和值信息

                    for(Map.Entry<Long, byte[]> val: colVal.entrySet()) { // 时间戳:列值
                        Long ts = val.getKey();
                        String value = new String(val.getValue());
                        System.out.print(f+":"+c+":"+ts+"=" +value + ",");
                    }
                }
            }
            System.out.println();
        }

        rs.close();
    }

    public void getDataWithVersion() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t2");
        Table t2  = conn.getTable(tableName);
        Get get = new Get("r2".getBytes());
        get.setMaxVersions(10);   // 设置可以获取的version数目，默认为Int.Max_value,实际最多可以获取指定版本数目的cell

        Result rowVal  = t2.get(get);   // 获取指定行所有数据
        List<Cell> cellList = rowVal.getColumnCells("f1".getBytes(),"name".getBytes());   // 获取(row,CF:col)中所有版本的cell
        System.out.println( "f1:name");
        for(Cell cell:cellList) {
            long ts = cell.getTimestamp();    // 获取时间戳
            String name  = new String(cell.getValueArray());  // 获取名称值
            System.out.println( ts + "=" + name);
        }
    }

    // 批量row读取表
    public void getDataWithCache() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table t6 = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setCaching(5000); // 设置每次next缓存行数
        ResultScanner resultScanner = t6.getScanner(scan);
        Iterator<Result> it =  resultScanner.iterator(); // Result表示行的缓存

        long start = System.currentTimeMillis();
        while (it.hasNext()) {
            Result row = it.next();
            String rowVal = new String(row.getRow());
            String name = new String(row.getColumnLatestCell("f1".getBytes(),"name".getBytes()).getValueArray());
            System.out.println(rowVal + ":" + name);
        }

        System.out.println(System.currentTimeMillis() - start);
    }


    // 缓存row和批量column读取表
    public void getDataWithCacheBatch() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t6");
        Table t6  = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setCaching(2); // 设置每次next缓存行数
        scan.setBatch(3);   // 设置每次next缓存列数
        ResultScanner resultScanner = t6.getScanner(scan);

        Iterator<Result> it =  resultScanner.iterator(); // Result表示行的缓存
        while(it.hasNext()) {
            Result row = it.next(); // Result表示Batch列数表示的行

            String rowId = new String(row.getRow());

            System.out.println("========================================");
            // map对应一行的所有信息：key=列族标志,value=Map<Col,Map<Timestamp,value>>
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = row.getMap();

            // 遍历列族中所有列
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> colFamily : map.entrySet()) {

                String f = new String(colFamily.getKey());
                NavigableMap<byte[], NavigableMap<Long, byte[]>> colTs = colFamily.getValue();    // 列以及其对应的所有时间戳和值

                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> colTsEntry: colTs.entrySet()) {

                    String c = new String(colTsEntry.getKey()); // 列
                    NavigableMap<Long, byte[]> colVal = colTsEntry.getValue();  // 获取时间戳和值信息

                    for(Map.Entry<Long, byte[]> val: colVal.entrySet()) { // 时间戳:列值
                        Long ts = val.getKey();
                        String value = new String(val.getValue());
                        System.out.print(rowId+","+f+":"+c+":"+ ts +"=" +value + ",");
                    }
                }
            }
            System.out.println();

        }


    }

    public static void main(String[] args) throws Exception {
        APITest test = new APITest();
//        test.testPut();
//        test.testGet();
//        test.testMillionsPut();
//        test.createNamespace();
//        test.createTable();
//        test.disableTable();
//        test.dropTable();
//        test.delteTableData();
//        test.scanTable();
//        test.scanTable2();
//        test.scanTable3();
//        test.getDataWithVersion();
        test.getDataWithCache();
//        test.getDataWithCacheBatch();


//        test.formatNum();
    }
}