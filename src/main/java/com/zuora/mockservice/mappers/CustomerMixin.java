package com.zuora.mockservice.mappers;

import com.zuora.mockservice.mappers.TpchMappers.TpchMoneySerializer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public abstract class CustomerMixin {
    public CustomerMixin(
            @JsonProperty("rowNumber") long rowNumber,
            @JsonProperty("customerKey") long customerKey,
            @JsonProperty("name") String name,
            @JsonProperty("address") String address,
            @JsonProperty("nationKey") long nationKey,
            @JsonProperty("phone") String phone,
            @JsonProperty("accountBalance") long accountBalance,
            @JsonProperty("marketSegment") String marketSegment,
            @JsonProperty("comment") String comment) {}

    @JsonProperty
    abstract long getRowNumber();

    @JsonProperty
    abstract long getCustomerKey();

    @JsonProperty
    abstract String getName();

    @JsonProperty
    abstract String getAddress();

    @JsonProperty
    abstract long getNationKey();

    @JsonProperty
    abstract String getPhone();

    @JsonProperty("accountBalance")
    @JsonSerialize(using = TpchMoneySerializer.class)
    abstract long getAccountBalanceInCents();

    @JsonProperty
    abstract String getMarketSegment();

    @JsonProperty
    abstract String getComment();
}
