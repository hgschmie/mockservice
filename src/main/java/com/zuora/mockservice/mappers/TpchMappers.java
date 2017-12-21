package com.zuora.mockservice.mappers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.tpch.Customer;
import io.airlift.tpch.CustomerColumn;
import io.airlift.tpch.GenerateUtils;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.Nation;
import io.airlift.tpch.NationColumn;
import io.airlift.tpch.Order;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartColumn;
import io.airlift.tpch.PartSupplier;
import io.airlift.tpch.PartSupplierColumn;
import io.airlift.tpch.Region;
import io.airlift.tpch.RegionColumn;
import io.airlift.tpch.Supplier;
import io.airlift.tpch.SupplierColumn;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public abstract class TpchMappers {
    public static final ResultSetMapper<Customer> CUSTOMER_MAPPER = new CustomerMapper();
    public static final ResultSetMapper<Order> ORDER_MAPPER = new OrderMapper();
    public static final ResultSetMapper<Part> PART_MAPPER = new PartMapper();
    public static final ResultSetMapper<PartSupplier> PART_SUPPLIER_MAPPER = new PartSupplierMapper();
    public static final ResultSetMapper<LineItem> LINE_ITEM_MAPPER = new LineItemMapper();
    public static final ResultSetMapper<Supplier> SUPPLIER_MAPPER = new SupplierMapper();
    public static final ResultSetMapper<Region> REGION_MAPPER = new RegionMapper();
    public static final ResultSetMapper<Nation> NATION_MAPPER = new NationMapper();

    private static int convertDate(Date date) {
        long instant = date.getTime();
        long day = instant / 86_400_000L; // A day worth of millis
        return (int) day - (GenerateUtils.MIN_GENERATE_DATE - GenerateUtils.GENERATED_DATE_EPOCH_OFFSET);
    }

    private static long convertMoney(BigDecimal value) {
        long result = value.movePointRight(2).longValue();
        return result;
    }

    public static class TpchDateSerializer extends JsonSerializer<Integer> {
        @Override
        public void serialize(Integer value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
            gen.writeString(GenerateUtils.formatDate(value + (GenerateUtils.MIN_GENERATE_DATE - GenerateUtils.GENERATED_DATE_EPOCH_OFFSET)));
        }
    }

    public static class TpchMoneySerializer extends JsonSerializer<Long> {
        @Override
        public void serialize(Long value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
            gen.writeNumber(BigDecimal.valueOf(value).movePointLeft(2));
        }
    }

    public static final class CustomerMapper implements ResultSetMapper<Customer> {

        private CustomerMapper() {}

        @Override
        public Customer map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Customer(
                    rs.getLong("row_key"),
                    rs.getLong(CustomerColumn.CUSTOMER_KEY.getSimplifiedColumnName()),
                    rs.getString(CustomerColumn.NAME.getSimplifiedColumnName()),
                    rs.getString(CustomerColumn.ADDRESS.getSimplifiedColumnName()),
                    rs.getLong(CustomerColumn.NATION_KEY.getSimplifiedColumnName()),
                    rs.getString(CustomerColumn.PHONE.getSimplifiedColumnName()),
                    convertMoney(rs.getBigDecimal(CustomerColumn.ACCOUNT_BALANCE.getSimplifiedColumnName())),
                    rs.getString(CustomerColumn.MARKET_SEGMENT.getSimplifiedColumnName()),
                    rs.getString(CustomerColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class OrderMapper implements ResultSetMapper<Order> {
        private OrderMapper() {}

        @Override
        public Order map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Order(
                    rs.getLong("row_key"),
                    rs.getLong(OrderColumn.ORDER_KEY.getSimplifiedColumnName()),
                    rs.getLong(OrderColumn.CUSTOMER_KEY.getSimplifiedColumnName()),
                    rs.getString(OrderColumn.ORDER_STATUS.getSimplifiedColumnName()).charAt(0),
                    convertMoney(rs.getBigDecimal(OrderColumn.TOTAL_PRICE.getSimplifiedColumnName())),
                    convertDate(rs.getDate(OrderColumn.ORDER_DATE.getSimplifiedColumnName())),
                    rs.getString(OrderColumn.ORDER_PRIORITY.getSimplifiedColumnName()),
                    rs.getString(OrderColumn.CLERK.getSimplifiedColumnName()),
                    rs.getInt(OrderColumn.SHIP_PRIORITY.getSimplifiedColumnName()),
                    rs.getString(OrderColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class PartMapper implements ResultSetMapper<Part> {

        private PartMapper() {}

        @Override
        public Part map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Part(rs.getLong("row_key"),
                    rs.getLong(PartColumn.PART_KEY.getSimplifiedColumnName()),
                    rs.getString(PartColumn.NAME.getSimplifiedColumnName()),
                    rs.getString(PartColumn.MANUFACTURER.getSimplifiedColumnName()),
                    rs.getString(PartColumn.BRAND.getSimplifiedColumnName()),
                    rs.getString(PartColumn.TYPE.getSimplifiedColumnName()),
                    rs.getInt(PartColumn.SIZE.getSimplifiedColumnName()),
                    rs.getString(PartColumn.CONTAINER.getSimplifiedColumnName()),
                    convertMoney(rs.getBigDecimal(PartColumn.RETAIL_PRICE.getSimplifiedColumnName())),
                    rs.getString(PartColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class LineItemMapper implements ResultSetMapper<LineItem> {

        private LineItemMapper() {}

        @Override
        public LineItem map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new LineItem(rs.getLong("row_key"),
                    rs.getLong(LineItemColumn.ORDER_KEY.getSimplifiedColumnName()),
                    rs.getLong(LineItemColumn.PART_KEY.getSimplifiedColumnName()),
                    rs.getLong(LineItemColumn.SUPPLIER_KEY.getSimplifiedColumnName()),
                    rs.getInt(LineItemColumn.LINE_NUMBER.getSimplifiedColumnName()),
                    rs.getLong(LineItemColumn.QUANTITY.getSimplifiedColumnName()),
                    convertMoney(rs.getBigDecimal(LineItemColumn.EXTENDED_PRICE.getSimplifiedColumnName())),
                    convertMoney(rs.getBigDecimal(LineItemColumn.DISCOUNT.getSimplifiedColumnName())),
                    convertMoney(rs.getBigDecimal(LineItemColumn.TAX.getSimplifiedColumnName())),
                    rs.getString(LineItemColumn.RETURN_FLAG.getSimplifiedColumnName()),
                    rs.getString(LineItemColumn.STATUS.getSimplifiedColumnName()),
                    convertDate(rs.getDate(LineItemColumn.SHIP_DATE.getSimplifiedColumnName())),
                    convertDate(rs.getDate(LineItemColumn.COMMIT_DATE.getSimplifiedColumnName())),
                    convertDate(rs.getDate(LineItemColumn.RECEIPT_DATE.getSimplifiedColumnName())),
                    rs.getString(LineItemColumn.SHIP_INSTRUCTIONS.getSimplifiedColumnName()),
                    rs.getString(LineItemColumn.SHIP_MODE.getSimplifiedColumnName()),
                    rs.getString(LineItemColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class PartSupplierMapper implements ResultSetMapper<PartSupplier> {

        private PartSupplierMapper() {}

        @Override
        public PartSupplier map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new PartSupplier(rs.getLong("row_key"),
                    rs.getLong(PartSupplierColumn.PART_KEY.getSimplifiedColumnName()),
                    rs.getLong(PartSupplierColumn.SUPPLIER_KEY.getSimplifiedColumnName()),
                    rs.getInt(PartSupplierColumn.AVAILABLE_QUANTITY.getSimplifiedColumnName()),
                    convertMoney(rs.getBigDecimal(PartSupplierColumn.SUPPLY_COST.getSimplifiedColumnName())),
                    rs.getString(PartSupplierColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class SupplierMapper implements ResultSetMapper<Supplier> {

        private SupplierMapper() {}

        @Override
        public Supplier map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Supplier(rs.getLong("row_key"),
                    rs.getLong(SupplierColumn.SUPPLIER_KEY.getSimplifiedColumnName()),
                    rs.getString(SupplierColumn.NAME.getSimplifiedColumnName()),
                    rs.getString(SupplierColumn.ADDRESS.getSimplifiedColumnName()),
                    rs.getLong(SupplierColumn.NATION_KEY.getSimplifiedColumnName()),
                    rs.getString(SupplierColumn.PHONE.getSimplifiedColumnName()),
                    convertMoney(rs.getBigDecimal(SupplierColumn.ACCOUNT_BALANCE.getSimplifiedColumnName())),
                    rs.getString(SupplierColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class RegionMapper implements ResultSetMapper<Region> {

        private RegionMapper() {}

        @Override
        public Region map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Region(rs.getLong("row_key"),
                    rs.getLong(RegionColumn.REGION_KEY.getSimplifiedColumnName()),
                    rs.getString(RegionColumn.NAME.getSimplifiedColumnName()),
                    rs.getString(RegionColumn.COMMENT.getSimplifiedColumnName()));
        }
    }

    public static final class NationMapper implements ResultSetMapper<Nation> {

        private NationMapper() {}

        @Override
        public Nation map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Nation(rs.getLong("row_key"),
                    rs.getLong(NationColumn.NATION_KEY.getSimplifiedColumnName()),
                    rs.getString(NationColumn.NAME.getSimplifiedColumnName()),
                    rs.getLong(NationColumn.REGION_KEY.getSimplifiedColumnName()),
                    rs.getString(NationColumn.COMMENT.getSimplifiedColumnName()));
        }
    }
}
