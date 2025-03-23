package com.hlxllo.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author hlxllo
 * @description 订单类
 * @date 2025/3/23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private int orderId;
    private int orderNumber;
    private int price;
    private Date createTime;
    private String desc;
}
