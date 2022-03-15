package com.hyj.flink.usrbehavior.bean;

/**
 * @program: flink_analysis
 * @description:
 * @author: huang
 **/


public class UserClickModel {
    private String date;
    private Long product;
    private long uid;
    private long pv;
    private long uv;


    public UserClickModel(String date, Long product, long uid, long pv, long uv) {
        this.date = date;
        this.product = product;
        this.uid = uid;
        this.pv = pv;
        this.uv = uv;
    }

    public UserClickModel() {
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getProduct() {
        return product;
    }

    public void setProduct(Long product) {
        this.product = product;
    }

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }

    public long getPv() {
        return pv;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    public long getUv() {
        return uv;
    }

    public void setUv(long uv) {
        this.uv = uv;
    }

    @Override
    public String toString() {
        return "UserClickModel{" +
                "date='" + date + '\'' +
                ", product='" + product + '\'' +
                ", uid=" + uid +
                ", pv=" + pv +
                ", uv=" + uv +
                '}';
    }
}