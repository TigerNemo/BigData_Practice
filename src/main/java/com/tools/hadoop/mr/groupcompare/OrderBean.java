package com.tools.hadoop.mr.groupcompare;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private String pId;
    private Double account;

    public OrderBean() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public Double getAccount() {
        return account;
    }

    public void setAccount(Double account) {
        this.account = account;
    }

    @Override
    public String toString() {
        return orderId + "\t" + pId + "\t" + account;
    }

    // 二次排序
    @Override
    public int compareTo(OrderBean o) {
        int result = this.orderId.compareTo(o.getOrderId());
        // 先按照 orderId 排序（升降序都可以）
        if (result == 0) {
            // 再按照 account（降序）排序
            result = -this.account.compareTo(o.getAccount());
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pId);
        out.writeDouble(account);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        pId = in.readUTF();
        account = in.readDouble();
    }
}
