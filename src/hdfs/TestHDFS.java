package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * java api 操作hdfs文件系统
 * @author huliang
 * @date 2018/8/22 21:26
 */
public class TestHDFS {

    /**
     *  通过Hadoop URL读取文件
     */
    @Test
    protected void readFileByURL() {
        // 注册url流处理器工厂(hdfs)
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        InputStream in = null;
        try {
            in = new URL("hdfs://192.168.44.201:8020/user/centos/hadoop/file.txt").openConnection().getInputStream();
            byte[] buf = new byte[in.available()];
            in.read(buf);
            System.out.println(new String(buf));

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过Hadoop API读取文件
     * @throws IOException
     */
    @Test
    public void readFileByAPI() throws IOException {

        Configuration conf  = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.44.201:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/centos/hadoop/index.html");
        FSDataInputStream fsDataInputStream = fs.open(path);

        byte[] buf = new byte[1024];
        int len = -1 ;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while((len = fsDataInputStream.read(buf)) != -1){
            baos.write(buf, 0, len);
        }
        fsDataInputStream.close();
        baos.close();
        System.out.println(new String(baos.toByteArray()));
    }

    /**
     * 通过API-2读取文件
     * @throws IOException
     */
    @Test
    public void readFileByAPI2() throws IOException {

        String uri = "hdfs://192.168.44.201:8020";
        String path = "/user/centos/hadoop/index.html";
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
        InputStream inputStream = null;
        inputStream = fileSystem.open(new Path(path));
        IOUtils.copyBytes(inputStream, System.out, 1024, false);

        IOUtils.closeStream(inputStream);

    }

    /**
     * API 写文件
     * @throws IOException
     */
    @Test
    public void writeFileByAPI() throws IOException {

        // 写入地址
        String uri = "hdfs://192.168.44.201:8020";
        String path = "/user/centos/hadoop/index.html";
        Configuration conf = new Configuration();

        // 指向hdfs地址的写入流
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        OutputStream out = fs.create(new Path(path), new Progressable() {
            public void progress() {
                System.out.print(".");  // 写入时打印.
            }
        });

        // 本地读取流
        String localsrc = "src/index.html";
        InputStream in = new BufferedInputStream(new FileInputStream(localsrc));

        IOUtils.copyBytes(in, out,4096, true);
    }

    /**
     * 创建目录 mkdirs
     * @throws IOException
     */
    @Test
    public void mkidrsByAPI() throws IOException {

        // 写入地址
        String uri = "hdfs://192.168.44.201:8020";
        String path = "/user/centos/hadoop2";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.mkdirs(new Path(path));
    }

    /**
     * 删除
     * @throws IOException
     */
    @Test
    public void deleteByAPI() throws IOException {

        String uri = "hdfs://192.168.44.201:8020";
        String path = "/user/centos/hadoop2";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.delete(new Path(path));
    }

    /**
     * 递归列出文件或目录信息 ---
     * @throws IOException
     */
    @Test
    public void listStatusByAPI() throws IOException {

        String uri = "hdfs://192.168.44.201:8020";
        String path = "/";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));  // 返回path目录下包含的目录或文件的FileStatus

        for(FileStatus status:fileStatuses) {

            System.out.println(status.getPath()+"\t"+status.getOwner()+":"+status.getGroup()+"\t"+status.getPermission().toString());

            FileStatus stat = status;
            while(stat.isDirectory()) {
                FileStatus[] statuses = fs.listStatus(stat.getPath());
                for(int i = 0; i < statuses.length; i++){
                    stat = statuses[i];
                    System.out.println(stat.getPath()+"\t"+stat.getOwner()+":"+stat.getGroup()+"\t"+stat.getPermission().toString());
                }
            }
        }
    }
}
