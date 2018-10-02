package com.huliang.mrjoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author huliang
 * @date 2018/10/2 17:07
 */
public class CusOrdKey implements WritableComparable<CusOrdKey> {

    private int type;  // 0 - customer 1 - order
    private int cid;
    private int oid;
    private String cinfo = "";
    private String oinfo = "";

    // 比较：使得整体按客户id排序，第一个是客户，后面是该客户对应的订单

    public int compareTo(CusOrdKey o) {
        int other_type = o.getType();
        int other_cid = o.getCid();
        int other_oid = o.getOid();

        /* // 先是客户，客户根据cid排序；再是订单，顶点也根据cid排序,注意客户空映射
        if(type != other_type) { // type不同，用户在前，订单在后
            if(type == 0) {
                // 自身是用户，在前
                return -1;
            }else // 自身是订单
                return 1;

        }else {
            // type相同，无论是用户0，还是订单，都按cid排序
            return cid - other_cid;
        }
        */

        if(other_cid == cid) { // 同一个用户

            if(other_type == type) { // 类型相同，当为0，即相同用户，无需比较，1，不同订单，根据订单号
                return oid - other_oid;
            }else  {
                // 不同类型，用户在前，订单在后
                if(type == 0)
                    return -1;
                else
                    return 1;
            }
        }else { // 不同用户，由cid排序
            return cid - other_cid;
        }
    }

    // 串行化
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(cid);
        out.writeInt(oid);
        out.writeUTF(cinfo);
        out.writeUTF(oinfo);
    }

    // 反串行化
    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        cid = in.readInt();
        oid = in.readInt();
        cinfo = in.readUTF();
        oinfo = in.readUTF();
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public String getCinfo() {
        return cinfo;
    }

    public void setCinfo(String cinfo) {
        this.cinfo = cinfo;
    }

    public String getOinfo() {
        return oinfo;
    }

    public void setOinfo(String oinfo) {
        this.oinfo = oinfo;
    }
}
