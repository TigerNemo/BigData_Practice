package com.tools.hadoop.mr.mapjoin;


public class JoinBean {

    private String orderId;
    private String pid;
    private String pname;
    private String amount;

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }


    @Override
    public String toString() {
        return orderId + "\t" + pname + "\t" + amount;
    }

}
