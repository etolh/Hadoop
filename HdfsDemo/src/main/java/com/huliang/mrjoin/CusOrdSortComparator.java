package com.huliang.mrjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 对分区后的CusOrdKey进行排序，按其自定义排序函数进行排序
 *
 * @author huliang
 * @date 2018/10/2 18:13
 */
public class CusOrdSortComparator extends WritableComparator {

    // 注册类
    protected CusOrdSortComparator() {
        super(CusOrdKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CusOrdKey aKey = (CusOrdKey)a;
        CusOrdKey bKey = (CusOrdKey)b;
        return aKey.compareTo(bKey);
    }
}
