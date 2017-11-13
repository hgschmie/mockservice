package com.zuora.mockservice;

import com.zuora.mockservice.mappers.TpchDao;
import com.zuora.mockservice.mappers.TpchMixins.CustomerMixin;
import com.zuora.mockservice.mappers.TpchMixins.LineItemMixin;
import com.zuora.mockservice.mappers.TpchMixins.NationMixin;
import com.zuora.mockservice.mappers.TpchMixins.OrderMixin;
import com.zuora.mockservice.mappers.TpchMixins.PartMixin;
import com.zuora.mockservice.mappers.TpchMixins.PartSupplierMixin;
import com.zuora.mockservice.mappers.TpchMixins.RegionMixin;
import com.zuora.mockservice.mappers.TpchMixins.SupplierMixin;
import com.zuora.mockservice.resources.AbstractTpchResource.CustomerResource;
import com.zuora.mockservice.resources.AbstractTpchResource.LineItemResource;
import com.zuora.mockservice.resources.AbstractTpchResource.NationResource;
import com.zuora.mockservice.resources.AbstractTpchResource.OrderResource;
import com.zuora.mockservice.resources.AbstractTpchResource.PartResource;
import com.zuora.mockservice.resources.AbstractTpchResource.PartSupplierResource;
import com.zuora.mockservice.resources.AbstractTpchResource.RegionResource;
import com.zuora.mockservice.resources.AbstractTpchResource.SupplierResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.tpch.Customer;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.Nation;
import io.airlift.tpch.Order;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartSupplier;
import io.airlift.tpch.Region;
import io.airlift.tpch.Supplier;
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
        objectMapper.addMixIn(Part.class, PartMixin.class);
        objectMapper.addMixIn(PartSupplier.class, PartSupplierMixin.class);
        objectMapper.addMixIn(Supplier.class, SupplierMixin.class);
        objectMapper.addMixIn(LineItem.class, LineItemMixin.class);
        objectMapper.addMixIn(Region.class, RegionMixin.class);
        objectMapper.addMixIn(Nation.class, NationMixin.class);

        final DBIFactory factory = new DBIFactory();
        final DBI jdbi = factory.build(environment, configuration.getDataSourceFactory(), "mysql");

        final TpchDao tpchDao = new TpchDao(jdbi);
        final OrderResource orderResource = new OrderResource(objectMapper, tpchDao);
        final CustomerResource customerResource = new CustomerResource(objectMapper, tpchDao);
        final PartResource partResource = new PartResource(objectMapper, tpchDao);
        final PartSupplierResource partSupplierResource = new PartSupplierResource(objectMapper, tpchDao);
        final SupplierResource supplierResource = new SupplierResource(objectMapper, tpchDao);
        final LineItemResource lineItemResource = new LineItemResource(objectMapper, tpchDao);
        final RegionResource regionResource = new RegionResource(objectMapper, tpchDao);
        final NationResource nationResource = new NationResource(objectMapper, tpchDao);
        environment.jersey().register(orderResource);
        environment.jersey().register(customerResource);
        environment.jersey().register(partResource);
        environment.jersey().register(partSupplierResource);
        environment.jersey().register(supplierResource);
        environment.jersey().register(lineItemResource);
        environment.jersey().register(regionResource);
        environment.jersey().register(nationResource);
    }
}
