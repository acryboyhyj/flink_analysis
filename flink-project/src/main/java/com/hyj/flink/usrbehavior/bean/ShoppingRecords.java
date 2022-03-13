package com.hyj.flink.usrbehavior.bean;

public class ShoppingRecords {
    private Long user_id;
    private Long itemId;
    private Long categoryId;
    private String behavior;
    private Long ts;

    public ShoppingRecords() {
    }

    public ShoppingRecords(Long uid, Long itemId, Long categoryId, String behaviortype, Long timestamp) {
        this.user_id = uid;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior= behaviortype;
        this.ts = timestamp;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behaviortype) {
        this.behavior = behaviortype;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "ShoppingRecords{" +
                "uid=" + user_id +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behaviortype='" + behavior + '\'' +
                ", timestamp=" + ts +
                '}';
    }
}
