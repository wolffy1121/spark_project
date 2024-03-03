package com.wolffy.spark.sparkcore.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class CategoryCountInfo implements Serializable, Comparable<CategoryCountInfo> {

    private String categoryId;
    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public CategoryCountInfo() {
    }

    public CategoryCountInfo(String categoryId, Long clickCount, Long orderCount, Long payCount) {
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    /*
    * 参数： 出入的数据
    * return 0    当前和传入相等
    * return 1  当前大
    * return -1  当前小
    * */
    @Override
    public int compareTo(CategoryCountInfo o) {
// 小于返回-1,等于返回0,大于返回1
        if (this.getClickCount().equals(o.getClickCount())) {
            if (this.getOrderCount().equals(o.getOrderCount())) {
                if (this.getPayCount().equals(o.getPayCount())) {
                    return 0;
                } else {
                    return this.getPayCount() < o.getPayCount() ? -1 : 1;
                }
            } else {
                return this.getOrderCount() < o.getOrderCount() ? -1 : 1;
            }
        } else {
            return this.getClickCount() < o.getClickCount() ? -1 : 1;
        }

    }
}
