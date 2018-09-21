package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;

/**
 * 定制写入的副本数、块大小
 * @author huliang
 * @date 2018/8/25 21:31
 */
public class HdfsTest {

    @Test
    public void readFileByAPI() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.44.201:8020/");
//        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.HDFS");

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/centos/hadoop/index.html");
        FSDataInputStream fsDataInputStream = fs.open(path);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fsDataInputStream, baos, 1024);
        System.out.println(new String(baos.toByteArray()));
    }

    @Test
    public void testWrite() throws IOException {

        String uri = "hdfs://192.168.44.201";
        String path = "/user/centos/admin/test.txt";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataOutputStream fdos = fs.create(new Path(path));
        fdos.write("BigTable".getBytes());
        fdos.close();
    }

    // 定制副本数和block size
    @Test
    public void testLimitWrite() throws IOException {

        String uri = "hdfs://192.168.44.201";
        String path = "/user/centos/admin/test2.txt";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        // 输出流: 倒数第二个参数是副本数，倒数最后为block大小
        FSDataOutputStream fous = fs.create(new Path(path), true, 1024*4, (short)2, 1024);

        URL url = HdfsTest.class.getClassLoader().getResource("test.txt");
        FileInputStream fis = new FileInputStream(url.getFile());
        IOUtils.copyBytes(fis, fous, 1024*4);
        fous.close();
        fis.close();
    }
}
