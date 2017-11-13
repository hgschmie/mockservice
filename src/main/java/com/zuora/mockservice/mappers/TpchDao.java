package com.zuora.mockservice.mappers;

import io.airlift.tpch.Customer;
import io.airlift.tpch.Order;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.ResultIterator;

public class TpchDao
{
    private static int FETCH_SIZE = 100;

    private static final String QUERY_ORDERS = "SELECT * from ORDERS";
    private static final String QUERY_CUSTOMER = "SELECT * from CUSTOMER";

    private final DBI dbi;

    public TpchDao(DBI dbi) {
        this.dbi = dbi;
    }

    public ResultIterator<Order> streamOrders() throws Exception
    {
        return dbi.open()
                .createQuery(QUERY_ORDERS)
                .map(TpchMappers.ORDER_MAPPER)
                .setFetchSize(FETCH_SIZE)
                .cleanupHandle()
                .iterator();
    }

    public ResultIterator<Customer> streamCustomer() throws Exception
    {
        return dbi.open()
                .createQuery(QUERY_CUSTOMER)
                .map(TpchMappers.CUSTOMER_MAPPER)
                .setFetchSize(FETCH_SIZE)
                .cleanupHandle()
                .iterator();
    }
}
