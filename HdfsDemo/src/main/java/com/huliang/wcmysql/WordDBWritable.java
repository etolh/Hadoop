package com.huliang.wcmysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 表words(id, word)映射类：用于map输入value
 *
 * @author huliang
 * @date 2018/9/29 17:48
 */
public class WordDBWritable implements DBWritable,Writable {

    private int id;
    private String word;
    private int count;

    // 串行化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(word);
        dataOutput.writeInt(count);
    }

    // 反串行化
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        word = dataInput.readUTF();
        count = dataInput.readInt();
    }

    // 写入到数据库
    public void write(PreparedStatement statement) throws SQLException {
//        statement.setInt(1, id);
//        statement.setString(2, word);
            statement.setString(1, word);
            statement.setInt(2, count);
    }

    // 从数据库读取
    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        word = resultSet.getString(2);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
