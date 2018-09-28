package com.huliang.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器：根据ComboKey(year,temp)中同一year划分为一组
 * @author huliang
 * @date 2018/9/28 17:46
 */
public class YearGroupComparator extends WritableComparator {

    protected YearGroupComparator() {
        super(ComboKey.class, true);
    }


    @Override
    // 一定要注意重写的类：WritableComparable，否则无法进行分组
    public int compare(WritableComparable a, WritableComparable b) {

        ComboKey aCombo = (ComboKey)a;
        ComboKey bCombo = (ComboKey)b;
        System.out.println("YearGroupComparator:"+aCombo+":"+bCombo);
        return aCombo.getYear() - bCombo.getYear();
    }

//    public int compare(Object a, Object b) {}
}
