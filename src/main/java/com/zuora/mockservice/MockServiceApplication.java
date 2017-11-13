package com.zuora.mockservice;

import com.zuora.mockservice.mappers.CustomerMixin;
import com.zuora.mockservice.mappers.OrderMixin;
import com.zuora.mockservice.mappers.TpchDao;
import com.zuora.mockservice.resources.CustomerResource;
import com.zuora.mockservice.resources.OrderResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.tpch.Customer;
import io.airlift.tpch.Order;
import io.dropwizard.Application;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.skife.jdbi.v2.DBI;

public class MockServiceApplication extends Application<MockServiceConfiguration> {

    public static void main(final String[] args) throws Exception {
        new MockServiceApplication().run(args);
    }

    @Override
    public String getName() {
        return "MockService";
    }

    @Override
    public void initialize(final Bootstrap<MockServiceConfiguration> bootstrap) {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider(Jackson.newObjectMapper());
        bootstrap.setObjectMapper(objectMapperProvider.get());
    }

    @Override
    public void run(final MockServiceConfiguration configuration,
            final Environment environment) {
        ObjectMapper objectMapper = environment.getObjectMapper();
        objectMapper.addMixIn(Order.class, OrderMixin.class);
        objectMapper.addMixIn(Customer.class, CustomerMixin.class);

        final DBIFactory factory = new DBIFactory();
        final DBI jdbi = factory.build(environment, configuration.getDataSourceFactory(), "mysql");

        final TpchDao tpchDao = new TpchDao(jdbi);
        final OrderResource orderResource = new OrderResource(objectMapper, tpchDao);
        final CustomerResource customerResource = new CustomerResource(objectMapper, tpchDao);
        environment.jersey().register(orderResource);
        environment.jersey().register(customerResource);
    }
}
