package com.zuora.mockservice.mappers;

import com.zuora.mockservice.mappers.TpchMappers.TpchDateSerializer;
import com.zuora.mockservice.mappers.TpchMappers.TpchMoneySerializer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public final class TpchMixins {

    public abstract class CustomerMixin {
        CustomerMixin(
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

    public static abstract class OrderMixin {
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
        @JsonSerialize(using = TpchMoneySerializer.class)
        abstract long getTotalPriceInCents();

        @JsonProperty
        @JsonSerialize(using = TpchDateSerializer.class)
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

    public static abstract class LineItemMixin {
        LineItemMixin(
                @JsonProperty("rowNumber") long rowNumber,
                @JsonProperty("orderKey") long orderKey,
                @JsonProperty("partKey") long partKey,
                @JsonProperty("supplierKey") long supplierKey,
                @JsonProperty("lineNumber") int lineNumber,
                @JsonProperty("quantity") long quantity,
                @JsonProperty("extendedPrice") long extendedPrice,
                @JsonProperty("discount") long discount,
                @JsonProperty("tax") long tax,
                @JsonProperty("returnFlag") String returnFlag,
                @JsonProperty("status") String status,
                @JsonProperty("shipDate") int shipDate,
                @JsonProperty("commitDate") int commitDate,
                @JsonProperty("receiptDate") int receiptDate,
                @JsonProperty("shipInstructions") String shipInstructions,
                @JsonProperty("shipMode") String shipMode,
                @JsonProperty("comment") String comment) {}

        @JsonProperty
        abstract long getRowNumber();

        @JsonProperty
        abstract long getOrderKey();

        @JsonProperty
        abstract long getPartKey();

        @JsonProperty
        abstract long getSupplierKey();

        @JsonProperty
        abstract int getLineNumber();

        @JsonProperty
        abstract long getQuantity();

        @JsonProperty("extendedPrice")
        @JsonSerialize(using = TpchMoneySerializer.class)
        abstract long getExtendedPriceInCents();

        @JsonProperty("discount")
        @JsonSerialize(using = TpchMoneySerializer.class)
        abstract long getDiscountPercent();

        @JsonProperty("tax")
        @JsonSerialize(using = TpchMoneySerializer.class)
        abstract long getTaxPercent();

        @JsonProperty
        abstract String getReturnFlag();

        @JsonProperty
        abstract String getStatus();

        @JsonProperty
        @JsonSerialize(using = TpchDateSerializer.class)
        abstract int getShipDate();

        @JsonProperty
        @JsonSerialize(using = TpchDateSerializer.class)
        abstract int getCommitDate();

        @JsonProperty
        @JsonSerialize(using = TpchDateSerializer.class)
        abstract int getReceiptDate();

        @JsonProperty
        abstract String getShipInstructions();

        @JsonProperty
        abstract String getShipMode();

        @JsonProperty
        abstract String getComment();
    }

    public static abstract class PartMixin {
        PartMixin(
                @JsonProperty("rowNumber") long rowNumber,
                @JsonProperty("partKey") long partKey,
                @JsonProperty("name") String name,
                @JsonProperty("manufacturer") String manufacturer,
                @JsonProperty("brand") String brand,
                @JsonProperty("type") String type,
                @JsonProperty("size") int size,
                @JsonProperty("container") String container,
                @JsonProperty("retailPrice") long retailPrice,
                @JsonProperty("comment") String comment) {}

        @JsonProperty
        abstract long getRowNumber();

        @JsonProperty
        abstract long getPartKey();

        @JsonProperty
        abstract String getName();

        @JsonProperty
        abstract String getManufacturer();

        @JsonProperty
        abstract String getBrand();

        @JsonProperty
        abstract String getType();

        @JsonProperty
        abstract int getSize();

        @JsonProperty
        abstract String getContainer();

        @JsonProperty("retailPrice")
        @JsonSerialize(using = TpchMoneySerializer.class)
        abstract long getRetailPriceInCents();

        @JsonProperty
        abstract String getComment();
    }

    public static abstract class PartSupplierMixin {
        PartSupplierMixin(
                @JsonProperty("rowNumber") long rowNumber,
                @JsonProperty("partKey") long partKey,
                @JsonProperty("supplierKey") long supplierKey,
                @JsonProperty("availableQuantity") int availableQuantity,
                @JsonProperty("supplyCost") long supplyCost,
                @JsonProperty("comment") String comment) {}

        @JsonProperty
        abstract long getRowNumber();

        @JsonProperty
        abstract long getPartKey();

        @JsonProperty
        abstract long getSupplierKey();

        @JsonProperty
        abstract int getAvailableQuantity();

        @JsonProperty("supplyCost")
        @JsonSerialize(using = TpchMoneySerializer.class)
        abstract long getSupplyCostInCents();

        @JsonProperty
        abstract String getComment();
    }

    public static abstract class SupplierMixin {
        SupplierMixin(
                @JsonProperty("rowNumber") long rowNumber,
                @JsonProperty("supplierKey") long supplierKey,
                @JsonProperty("name") String name,
                @JsonProperty("address") String address,
                @JsonProperty("nationKey") long nationKey,
                @JsonProperty("phone") String phone,
                @JsonProperty("accountBalance") long accountBalance,
                @JsonProperty("comment") String comment) {}

        @JsonProperty
        abstract long getRowNumber();

        @JsonProperty
        abstract long getSupplierKey();

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
        abstract String getComment();
    }

    public static abstract class RegionMixin {
        RegionMixin(
                @JsonProperty("rowNumber") long rowNumber,
                @JsonProperty("regionKey") long regionKey,
                @JsonProperty("name") String name,
                @JsonProperty("comment") String comment) {}

        @JsonProperty
        abstract long getRowNumber();

        @JsonProperty
        abstract long getRegionKey();

        @JsonProperty
        abstract String getName();

        @JsonProperty
        abstract String getComment();
    }

    public static abstract class NationMixin {
        NationMixin(
                @JsonProperty("rowNumber") long rowNumber,
                @JsonProperty("nationKey") long nationKey,
                @JsonProperty("name") String name,
                @JsonProperty("regionKey") long regionKey,
                @JsonProperty("comment") String comment) {}

        @JsonProperty
        abstract long getRowNumber();

        @JsonProperty
        abstract long getNationKey();

        @JsonProperty
        abstract String getName();

        @JsonProperty
        abstract long getRegionKey();

        @JsonProperty
        abstract String getComment();
    }
}
