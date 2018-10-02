package com.huliang.mrjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组比较器：使得同cid的用户和一组订单CusOrdKey划分为同一组
 *
 * @author huliang
 * @date 2018/10/2 18:17
 */
public class CusOrdKeyGroupComparator extends WritableComparator {

    protected CusOrdKeyGroupComparator() {
        super(CusOrdKey.class, true);
    }

    @Override
    // 根据cid排序
    public int compare(WritableComparable a, WritableComparable b) {
        CusOrdKey aKey = (CusOrdKey)a;
        CusOrdKey bKey = (CusOrdKey)b;
        return aKey.getCid() - bKey.getCid();
    }
}
