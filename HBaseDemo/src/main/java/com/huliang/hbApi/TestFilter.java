package com.huliang.hbApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * 测试HBase过滤器Filter
 *
 * @author huliang
 * @date 2018/10/13 19:32
 */
public class TestFilter {

    /**
     *  测试行过滤器：返回符合条件的所有行
     */
    public void testRowFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t1");
        Table t1 = conn.getTable(tableName);

        Scan scan = new Scan();
        // 创建行比较器，比较符设为<=, 比较器设置二进制比较器，返回所有符合条件的结果
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator("r00100".getBytes()));

        scan.setFilter(rowFilter);
        ResultScanner scanner = t1.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        while (it.hasNext()) {
            Result rs = it.next();
            // 输出符合条件的每一行
            System.out.println(Bytes.toString(rs.getRow()));
        }
    }

    /**
     * 列族过滤器：返回符合条件的列族
     */
    public void testFamilyFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();
        // 添加列过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS,
                new BinaryComparator(Bytes.toBytes("f2")));
        scan.setFilter(familyFilter);

        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        while (it.hasNext()) {
            Result rs = it.next();
            // 分别输出f1和f2的name
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            System.out.println( new String(f1name) + " : " + f2name);
        }
    }

    /**
     * 列过滤器：返回符合条件的列
     */
    public void testColumnFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();
        // 添加列过滤器: 寻找包含name的列
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("name"));
        scan.setFilter(qualifierFilter);

        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        while (it.hasNext()) {
            Result rs = it.next();
            // 分别获取id列和name列
            byte[] f1id = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2id = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));

            System.out.println(f1id  +" : " +  Bytes.toString(f1name) + " / " + f2id + " : " + Bytes.toString(f2name));
        }
    }

    /**
     * 值过滤器：返回符合条件的列
     */
    public void testValueFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();
        // 添加值过滤器：返回值包含tom的列
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("tom2"));
        scan.setFilter(valueFilter);

        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        while (it.hasNext()) {
            Result rs = it.next();
            // 分别获取id列和name列
            byte[] f1id = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2id = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));

            System.out.println(f1id  +" : " +  Bytes.toString(f1name) + " / " + f2id + " : " + Bytes.toString(f2name));
        }
    }

    /**
     * 依赖列过滤器：返回满足条件的依赖列，
     * dropDependentColumn需要设置为false，否则会丢弃满足条件的列
     */
    public void testDependFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();
        // 依赖列过滤器：
        DependentColumnFilter depFilter = new DependentColumnFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                true,
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("tom2")
        );
        scan.setFilter(depFilter);

        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();

        while (it.hasNext()) {
            Result rs = it.next();
            // 分别获取f1和f2的id、name列
            byte[] f1id = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2id = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));

            System.out.println(f1id  +" : " +  Bytes.toString(f1name) + " / " + f2id + " : " + Bytes.toString(f2name));
        }
    }


    /**
     * 单列值过滤器：选择符合单列值条件的行（整行）
     */
    public void testSingleColumnValueFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();
        // 单列值过滤器：选择所有f1:name不含tom2的行
        SingleColumnValueFilter singleFilter = new SingleColumnValueFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.NOT_EQUAL,
                new SubstringComparator("tom2")
        );
        scan.setFilter(singleFilter);

        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();

        while (it.hasNext()) {
            Result rs = it.next();
            // 分别获取f1和f2的id、name列
            byte[] f1id = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2id = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));

            System.out.println(f1id  +" : " +  Bytes.toString(f1name) + " / " + f2id + " : " + Bytes.toString(f2name));
        }
    }

    /**
     * 单列值排除过滤器：选择满足参考列条件的行，但参考列不作为返回结果
     */
    public void testSingleColumnValueExcludeFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();
        // 单列值过滤器：选择所有f1:name不含tom2的行
        SingleColumnValueExcludeFilter singleExcludeFilter = new SingleColumnValueExcludeFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("tom2")
        );
        scan.setFilter(singleExcludeFilter);

        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();

        while (it.hasNext()) {
            Result rs = it.next();
            // 分别获取f1和f2的id、name列
            byte[] f1id = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2id = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));

            System.out.println(f1id  +" : " +  Bytes.toString(f1name) + " / " + f2id + " : " + Bytes.toString(f2name));
        }
    }

    /**
     * 复杂查询：select * from t7 where ((f1.age < 22) and (f1.name like '%t') or (f1.age >= 22) and (f1.name like 'in%'))
     *
     */
    public void testComplexFilter() throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("ns1:t7");
        Table t7 = conn.getTable(tableName);
        Scan scan = new Scan();

        // where f1.age < 22
        SingleColumnValueExcludeFilter sf1Left = new SingleColumnValueExcludeFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.LESS,
                new BinaryComparator(Bytes.toBytes("21"))
        );

        // where f1.name like '%t'  以t开头
        SingleColumnValueExcludeFilter sf1Right = new SingleColumnValueExcludeFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("^t")  // 正则比较器，以t开头
        );

        // sf1Left & sf1Right组合
        FilterList fList1 = new FilterList(FilterList.Operator.MUST_PASS_ALL);  // AND组合
        fList1.addFilter(sf1Left);
        fList1.addFilter(sf1Right);


        // where f1.age >= 22
        SingleColumnValueExcludeFilter sf2Left = new SingleColumnValueExcludeFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("22"))
        );

        // where f1.name like 'in%'  以in结尾
        SingleColumnValueExcludeFilter sf2Right = new SingleColumnValueExcludeFilter(
                Bytes.toBytes("f1"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("in$")  // 正则比较器，以t开头
        );

        // sf2Left & sf2Right组合
        FilterList fList2 = new FilterList(FilterList.Operator.MUST_PASS_ALL);  // AND组合
        fList1.addFilter(sf2Left);
        fList1.addFilter(sf2Right);

        // 两个FilterList组合 | or
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);  // OR
        filterList.addFilter(fList1);
        filterList.addFilter(fList2);

        scan.setFilter(filterList); // 添加过滤器列表
        ResultScanner scanner = t7.getScanner(scan);
        Iterator<Result> it = scanner.iterator();

        while (it.hasNext()) {
            Result rs = it.next();
            // 分别获取f1和f2的id、name列
            byte[] f1id = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
            byte[] f1name = rs.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
            byte[] f2id = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("id"));
            byte[] f2name = rs.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));

            System.out.println(f1id  +" : " +  Bytes.toString(f1name) + " / " + f2id + " : " + Bytes.toString(f2name));
        }
    }



    public static void main(String[] args) throws Exception {

        TestFilter test = new TestFilter();
//        test.testRowFilter();
//        test.testFamilyFilter();
//        test.testColumnFilter();
//        test.testValueFilter();
//        test.testDependFilter();
//        test.testSingleColumnValueFilter();
//        test.testSingleColumnValueExcludeFilter();
        test.testComplexFilter();
    }
}
