package com.huliang.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce任务按key排序比较器对接收的(k,v)进行排序
 * 基于year升序，temp降序，即ComboKey自身排序
 * @author huliang
 * @date 2018/9/28 17:35
 */
public class ComboKeyComparator extends WritableComparator {

    // 注册比较类
    protected ComboKeyComparator() {
        super(ComboKey.class, true);
    }

    @Override
    // 注意重写方法
    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey acombo = (ComboKey)a;
        ComboKey bcombo = (ComboKey)b;

        System.out.println("ComboKeyComparator"+acombo+":"+bcombo);

        return acombo.compareTo(bcombo);
    }

//    public int compare(Object a, Object b) { }

}
