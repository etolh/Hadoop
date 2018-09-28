package com.huliang.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 组合Key：结合year和temp，并可比较，即实现WritableComparable<T>
 * @author huliang
 * @date 2018/9/28 17:14
 */
public class ComboKey implements WritableComparable<ComboKey> {

    private int year;
    private int temp;

    /**
     * ComboKey比较机制：先比较year,升序；再比较temp，降序
     * 使得同一year中第一个就是最大温度，无需再遍历所有temp
     */
    public int compareTo(ComboKey o) {
        int other_year = o.getYear();
        int other_temp = o.getTemp();

        if( year == other_year )
            return other_temp - temp;
        else
            return year - other_year;
    }

    // 串行化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(temp);
    }

    // 反串行化
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        temp = dataInput.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public String toString() {
        return year + "/" + temp;
    }
}
