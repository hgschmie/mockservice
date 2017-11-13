package com.zuora.mockservice.mappers;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.airlift.tpch.Customer;
import io.airlift.tpch.CustomerColumn;
import io.airlift.tpch.GenerateUtils;
import io.airlift.tpch.Order;
import io.airlift.tpch.OrderColumn;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public abstract class TpchMappers
{
    public static final ResultSetMapper<Customer> CUSTOMER_MAPPER = new CustomerMapper();
    public static final ResultSetMapper<Order> ORDER_MAPPER = new OrderMapper();

    private static int convertDate(Date date) {
        long instant = date.getTime();
        long day = instant / 86_400_000L; // A day worth of millis
        return (int) day -(GenerateUtils.MIN_GENERATE_DATE - GenerateUtils.GENERATED_DATE_EPOCH_OFFSET);
    }

    private static long convertMoney(BigDecimal value) {
        long result =  value.movePointRight(2).longValue();
        return result;
    }

    public static class TpchDateSerializer extends JsonSerializer<Integer>
    {
        @Override
        public void serialize(Integer value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
            gen.writeString(GenerateUtils.formatDate(value + (GenerateUtils.MIN_GENERATE_DATE - GenerateUtils.GENERATED_DATE_EPOCH_OFFSET)));
      }
    }

    public static class TpchMoneySerializer extends JsonSerializer<Long>
    {
        @Override
        public void serialize(Long value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
            gen.writeNumber(BigDecimal.valueOf(value).movePointLeft(2));
      }
    }

    public static final class CustomerMapper implements ResultSetMapper<Customer> {
        private CustomerMapper() {
        }

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
                    rs.getString(CustomerColumn.COMMENT.getColumnName())
                    );
        }
    }

    public static final class OrderMapper implements ResultSetMapper<Order> {
        private OrderMapper() {
        }

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
                    rs.getString(OrderColumn.COMMENT.getColumnName())
                    );
        }
    }
}
