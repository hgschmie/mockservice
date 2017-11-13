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
                    rs.getLong(CustomerColumn.CUSTOMER_KEY.getColumnName()),
                    rs.getString(CustomerColumn.NAME.getColumnName()),
                    rs.getString(CustomerColumn.ADDRESS.getColumnName()),
                    rs.getLong(CustomerColumn.NATION_KEY.getColumnName()),
                    rs.getString(CustomerColumn.PHONE.getColumnName()),
                    convertMoney(rs.getBigDecimal(CustomerColumn.ACCOUNT_BALANCE.getColumnName())),
                    rs.getString(CustomerColumn.MARKET_SEGMENT.getColumnName()),
                    rs.getString(CustomerColumn.COMMENT.getColumnName()));
        }
    }

    public static final class OrderMapper implements ResultSetMapper<Order> {
        private OrderMapper() {}

        @Override
        public Order map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Order(
                    rs.getLong("row_key"),
                    rs.getLong(OrderColumn.ORDER_KEY.getColumnName()),
                    rs.getLong(OrderColumn.CUSTOMER_KEY.getColumnName()),
                    rs.getString(OrderColumn.ORDER_STATUS.getColumnName()).charAt(0),
                    convertMoney(rs.getBigDecimal(OrderColumn.TOTAL_PRICE.getColumnName())),
                    convertDate(rs.getDate(OrderColumn.ORDER_DATE.getColumnName())),
                    rs.getString(OrderColumn.ORDER_PRIORITY.getColumnName()),
                    rs.getString(OrderColumn.CLERK.getColumnName()),
                    rs.getInt(OrderColumn.SHIP_PRIORITY.getColumnName()),
                    rs.getString(OrderColumn.COMMENT.getColumnName()));
        }
    }

    public static final class PartMapper implements ResultSetMapper<Part> {

        private PartMapper() {}

        @Override
        public Part map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Part(rs.getLong("row_key"),
                    rs.getLong(PartColumn.PART_KEY.getColumnName()),
                    rs.getString(PartColumn.NAME.getColumnName()),
                    rs.getString(PartColumn.MANUFACTURER.getColumnName()),
                    rs.getString(PartColumn.BRAND.getColumnName()),
                    rs.getString(PartColumn.TYPE.getColumnName()),
                    rs.getInt(PartColumn.SIZE.getColumnName()),
                    rs.getString(PartColumn.CONTAINER.getColumnName()),
                    convertMoney(rs.getBigDecimal(PartColumn.RETAIL_PRICE.getColumnName())),
                    rs.getString(PartColumn.COMMENT.getColumnName()));
        }
    }

    public static final class LineItemMapper implements ResultSetMapper<LineItem> {

        private LineItemMapper() {}

        @Override
        public LineItem map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new LineItem(rs.getLong("row_key"),
                    rs.getLong(LineItemColumn.ORDER_KEY.getColumnName()),
                    rs.getLong(LineItemColumn.PART_KEY.getColumnName()),
                    rs.getLong(LineItemColumn.SUPPLIER_KEY.getColumnName()),
                    rs.getInt(LineItemColumn.LINE_NUMBER.getColumnName()),
                    rs.getLong(LineItemColumn.QUANTITY.getColumnName()),
                    convertMoney(rs.getBigDecimal(LineItemColumn.EXTENDED_PRICE.getColumnName())),
                    convertMoney(rs.getBigDecimal(LineItemColumn.DISCOUNT.getColumnName())),
                    convertMoney(rs.getBigDecimal(LineItemColumn.TAX.getColumnName())),
                    rs.getString(LineItemColumn.RETURN_FLAG.getColumnName()),
                    rs.getString(LineItemColumn.STATUS.getColumnName()),
                    convertDate(rs.getDate(LineItemColumn.SHIP_DATE.getColumnName())),
                    convertDate(rs.getDate(LineItemColumn.COMMIT_DATE.getColumnName())),
                    convertDate(rs.getDate(LineItemColumn.RECEIPT_DATE.getColumnName())),
                    rs.getString(LineItemColumn.SHIP_INSTRUCTIONS.getColumnName()),
                    rs.getString(LineItemColumn.SHIP_MODE.getColumnName()),
                    rs.getString(LineItemColumn.COMMENT.getColumnName()));
        }
    }

    public static final class PartSupplierMapper implements ResultSetMapper<PartSupplier> {

        private PartSupplierMapper() {}

        @Override
        public PartSupplier map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new PartSupplier(rs.getLong("row_key"),
                    rs.getLong(PartSupplierColumn.PART_KEY.getColumnName()),
                    rs.getLong(PartSupplierColumn.SUPPLIER_KEY.getColumnName()),
                    rs.getInt(PartSupplierColumn.AVAILABLE_QUANTITY.getColumnName()),
                    convertMoney(rs.getBigDecimal(PartSupplierColumn.SUPPLY_COST.getColumnName())),
                    rs.getString(PartSupplierColumn.COMMENT.getColumnName()));
        }
    }

    public static final class SupplierMapper implements ResultSetMapper<Supplier> {

        private SupplierMapper() {}

        @Override
        public Supplier map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Supplier(rs.getLong("row_key"),
                    rs.getLong(SupplierColumn.SUPPLIER_KEY.getColumnName()),
                    rs.getString(SupplierColumn.NAME.getColumnName()),
                    rs.getString(SupplierColumn.ADDRESS.getColumnName()),
                    rs.getLong(SupplierColumn.NATION_KEY.getColumnName()),
                    rs.getString(SupplierColumn.PHONE.getColumnName()),
                    convertMoney(rs.getBigDecimal(SupplierColumn.ACCOUNT_BALANCE.getColumnName())),
                    rs.getString(SupplierColumn.COMMENT.getColumnName()));
        }
    }

    public static final class RegionMapper implements ResultSetMapper<Region> {

        private RegionMapper() {}

        @Override
        public Region map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Region(rs.getLong("row_key"),
                    rs.getLong(RegionColumn.REGION_KEY.getColumnName()),
                    rs.getString(RegionColumn.NAME.getColumnName()),
                    rs.getString(RegionColumn.COMMENT.getColumnName()));
        }
    }

    public static final class NationMapper implements ResultSetMapper<Nation> {

        private NationMapper() {}

        @Override
        public Nation map(int index, ResultSet rs, StatementContext context) throws SQLException {
            return new Nation(rs.getLong("row_key"),
                    rs.getLong(NationColumn.NATION_KEY.getColumnName()),
                    rs.getString(NationColumn.NAME.getColumnName()),
                    rs.getLong(NationColumn.REGION_KEY.getColumnName()),
                    rs.getString(NationColumn.COMMENT.getColumnName()));
        }
    }
}
