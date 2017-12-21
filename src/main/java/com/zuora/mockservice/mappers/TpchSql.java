package com.zuora.mockservice.mappers;

import io.airlift.tpch.Customer;
import io.airlift.tpch.GenerateUtils;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.Nation;
import io.airlift.tpch.Order;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartSupplier;
import io.airlift.tpch.Region;
import io.airlift.tpch.Supplier;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

import java.io.Closeable;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TpchSql {
    private final DBI dbi;
    private final TpchSqlInt tpchSqlInt;

    public TpchSql(final DBI dbi) {
        this.dbi = dbi;
        this.tpchSqlInt = dbi.onDemand(TpchSqlInt.class);
    }

    private static final String INSERT_PART =
            "INSERT INTO PART (ROW_KEY,    PARTKEY, NAME, MFGR,       BRAND, TYPE, SIZE, CONTAINER, RETAILPRICE, COMMENT)"
                    + " VALUES (:rowNumber, :partKey,  :name, :manufacturer, :brand,  :type,  :size,  :container,  :retailPrice,  :comment)";

    private static final String INSERT_SUPPLIER = "INSERT INTO SUPPLIER (row_key, SUPPKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL, COMMENT)"
            + " VALUES (:rowNumber, :supplierKey, :name, :address, :nationKey, :phone, :accountBalance, :comment)";

    private static final String INSERT_PARTSUPP = "INSERT INTO PARTSUPP (row_key, PARTKEY, SUPPKEY, AVAILQTY, SUPPLYCOST, COMMENT)"
            + "      VALUES          (:rowNumber, :partKey, :supplierKey, :availableQuantity, :supplyCost, :comment)";

    private static final String INSERT_CUSTOMER =
            "INSERT INTO CUSTOMER (row_key, CUSTKEY, NAME, ADDRESS, NATIONKEY, PHONE, ACCTBAL, MKTSEGMENT, COMMENT)"
                    + "      VALUES          (:rowNumber, :customerKey, :name, :address, :nationKey, :phone, :accountBalance, :marketSegment, :comment)";

    private static final String INSERT_ORDERS =
            "INSERT INTO ORDERS (row_key, ORDERKEY, CUSTKEY, ORDERSTATUS, TOTALPRICE, ORDERDATE, ORDERPRIORITY, CLERK, SHIPPRIORITY, COMMENT)"
                    + "      VALUES          (:rowNumber, :orderKey, :customerKey, :orderStatus, :totalPrice, :orderDate, :orderPriority, :clerk, :shipPriority, :comment)";

    private static final String INSERT_LINEITEM =
            "INSERT INTO LINEITEM (row_key, ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX, RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE, COMMENT)"
                    + " VALUES (:rowNumber, :orderKey, :partKey, :supplierKey, :lineNumber, :quantity, :extendedPrice, :discount, :tax, :returnFlag, :lineStatus, :shipDate, :commitDate, :receiptDate, :shipInstructions, :shipMode, :comment)";


    private BigDecimal formatPrice(long value) {
        return new BigDecimal(value).divide(new BigDecimal(100)).setScale(2);
    }

    private final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd");

    private Date formatDate(int date) throws Exception {
        return SDF.parse(GenerateUtils.formatDate(date));
    }

    public int insertParts(List<Part> parts) {
        return dbi.inTransaction((handle, status) -> {
            int result = 0;
            for (Part part : parts) {
                result += handle.createStatement(INSERT_PART)
                        .bind("rowNumber", part.getRowNumber())
                        .bind("partKey", part.getPartKey())
                        .bind("name", part.getName())
                        .bind("manufacturer", part.getManufacturer())
                        .bind("brand", part.getBrand())
                        .bind("type", part.getType())
                        .bind("size", part.getSize())
                        .bind("container", part.getContainer())
                        .bind("retailPrice", formatPrice(part.getRetailPriceInCents()))
                        .bind("comment", part.getComment())
                        .execute();
            }
            return result;
        });
    }

    public int insertSuppliers(List<Supplier> suppliers) {
        return dbi.inTransaction((handle, status) -> {
            int result = 0;
            for (Supplier supplier : suppliers) {
                result += handle.createStatement(INSERT_SUPPLIER)
                        .bind("rowNumber", supplier.getRowNumber())
                        .bind("supplierKey", supplier.getSupplierKey())
                        .bind("name", supplier.getName())
                        .bind("address", supplier.getAddress())
                        .bind("nationKey", supplier.getNationKey())
                        .bind("phone", supplier.getPhone())
                        .bind("accountBalance", formatPrice(supplier.getAccountBalanceInCents()))
                        .bind("comment", supplier.getComment())
                        .execute();
            }
            return result;
        });
    }


    public int insertPartSuppliers(List<PartSupplier> partSuppliers) {
        return dbi.inTransaction((handle, status) -> {
            int result = 0;
            for (PartSupplier partSupplier : partSuppliers) {
                result += handle.createStatement(INSERT_PARTSUPP)
                        .bind("rowNumber", partSupplier.getRowNumber())
                        .bind("partKey", partSupplier.getPartKey())
                        .bind("supplierKey", partSupplier.getSupplierKey())
                        .bind("availableQuantity", partSupplier.getAvailableQuantity())
                        .bind("supplyCost", formatPrice(partSupplier.getSupplyCostInCents()))
                        .bind("comment", partSupplier.getComment())
                        .execute();
            }
            return result;
        });
    }

    public int insertCustomers(List<Customer> customers) {
        return dbi.inTransaction((handle, status) -> {
            int result = 0;
            for (Customer customer : customers) {
                result += handle.createStatement(INSERT_CUSTOMER)
                        .bind("rowNumber", customer.getRowNumber())
                        .bind("customerKey", customer.getCustomerKey())
                        .bind("name", customer.getName())
                        .bind("address", customer.getAddress())
                        .bind("nationKey", customer.getNationKey())
                        .bind("phone", customer.getPhone())
                        .bind("accountBalance", formatPrice(customer.getAccountBalanceInCents()))
                        .bind("marketSegment", customer.getMarketSegment())
                        .bind("comment", customer.getComment())
                        .execute();
            }
            return result;
        });
    }


    public int insertOrders(List<Order> orders) {
        return dbi.inTransaction((handle, status) -> {
            int result = 0;
            for (Order order : orders) {
                result += handle.createStatement(INSERT_ORDERS)
                        .bind("rowNumber", order.getRowNumber())
                        .bind("orderKey", order.getOrderKey())
                        .bind("customerKey", order.getCustomerKey())
                        .bind("orderStatus", order.getOrderStatus())
                        .bind("totalPrice", formatPrice(order.getTotalPriceInCents()))
                        .bind("orderDate", formatDate(order.getOrderDate()))
                        .bind("orderPriority", order.getOrderPriority())
                        .bind("clerk", order.getClerk())
                        .bind("shipPriority", order.getShipPriority())
                        .bind("comment", order.getComment())
                        .execute();
            }
            return result;
        });
    }


    public int insertLineItems(List<LineItem> lineItems) {
        return dbi.inTransaction((handle, status) -> {
            int result = 0;
            for (LineItem lineItem : lineItems) {
                result += handle.createStatement(INSERT_LINEITEM)
                        .bind("rowNumber", lineItem.getRowNumber())
                        .bind("orderKey", lineItem.getOrderKey())
                        .bind("partKey", lineItem.getPartKey())
                        .bind("supplierKey", lineItem.getSupplierKey())
                        .bind("lineNumber", lineItem.getLineNumber())
                        .bind("quantity", lineItem.getQuantity())
                        .bind("extendedPrice", formatPrice(lineItem.getExtendedPriceInCents()))
                        .bind("discount", formatPrice(lineItem.getDiscountPercent()))
                        .bind("tax", formatPrice(lineItem.getTaxPercent()))
                        .bind("returnFlag", lineItem.getReturnFlag())
                        .bind("lineStatus", lineItem.getStatus())
                        .bind("shipDate", formatDate(lineItem.getShipDate()))
                        .bind("commitDate", formatDate(lineItem.getCommitDate()))
                        .bind("receiptDate", formatDate(lineItem.getReceiptDate()))
                        .bind("shipInstructions", lineItem.getShipInstructions())
                        .bind("shipMode", lineItem.getShipMode())
                        .bind("comment", lineItem.getComment())
                        .execute();
            }
            return result;
        });
    }


    public int insertNation(Nation nation) {
        return tpchSqlInt.insertNation(nation);
    }

    public int insertRegion(Region region) {
        return tpchSqlInt.insertRegion(region);
    }

    public interface TpchSqlInt extends Closeable {
        @SqlUpdate("INSERT INTO NATION (row_key, NATIONKEY, NAME, REGIONKEY, COMMENT) VALUES (:rowNumber, :nationKey, :name, :regionKey, :comment)")
        int insertNation(@BindBean Nation nation);

        @SqlUpdate("INSERT INTO REGION (row_key, REGIONKEY, NAME, COMMENT) VALUES (:rowNumber, :regionKey, :name, :comment)")
        int insertRegion(@BindBean Region region);

        @Override
        public void close();
    }

}
