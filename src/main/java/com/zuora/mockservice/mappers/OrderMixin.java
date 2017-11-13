package com.zuora.mockservice.mappers;

import com.zuora.mockservice.mappers.TpchMappers.TpchDateSerializer;
import com.zuora.mockservice.mappers.TpchMappers.TpchMoneySerializer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public abstract class OrderMixin {

    OrderMixin(
            @JsonProperty("rowNumber") long rowNumber,
            @JsonProperty("orderKey") long orderKey,
            @JsonProperty("customerKey") long customerKey,
            @JsonProperty("orderStatus") char orderStatus,
            @JsonProperty("totalPrice") long totalPrice,
            @JsonProperty("orderDate") int orderDate,
            @JsonProperty("orderPriority") String orderPriority,
            @JsonProperty("clerk") String clerk,
            @JsonProperty("shipPriority") int shipPriority,
            @JsonProperty("comment") String comment) {}

    @JsonProperty
    abstract long getRowNumber();

    @JsonProperty
    abstract long getOrderKey();

    @JsonProperty
    abstract long getCustomerKey();

    @JsonProperty
    abstract char getOrderStatus();

    @JsonProperty("totalPrice")
    @JsonSerialize(using=TpchMoneySerializer.class)
    abstract long getTotalPriceInCents();

    @JsonProperty
    @JsonSerialize(using=TpchDateSerializer.class)
    abstract int getOrderDate();

    @JsonProperty
    abstract String getOrderPriority();

    @JsonProperty
    abstract String getClerk();

    @JsonProperty
    abstract int getShipPriority();

    @JsonProperty
    abstract String getComment();
}
