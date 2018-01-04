package com.zuora.mockservice;

import static com.google.common.collect.Iterables.partition;

import com.zuora.mockservice.mappers.TpchSql;

import io.airlift.tpch.TpchTable;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.skife.jdbi.v2.DBI;

public class DataGeneratorCommand extends EnvironmentCommand<MockServiceConfiguration> {

    public static final double SCALE = 1; // 0.01;

    public DataGeneratorCommand(Application<MockServiceConfiguration> application) {
        super(application, "generate", "generates test data");
    }


    @Override
    protected void run(Environment environment, Namespace namespace, MockServiceConfiguration configuration) throws Exception {
        final DBIFactory factory = new DBIFactory();
        final DBI dbi = factory.build(environment, configuration.getDataSourceFactory(), "postgresql");

        TpchSql tpchSql = new TpchSql(dbi);

        System.out.println("LineItem");
        partition(TpchTable.LINE_ITEM.createGenerator(SCALE, 1, 1), 1000).forEach((lineItems) -> tpchSql.insertLineItems(lineItems));

        System.out.println("Nation");
        TpchTable.NATION.createGenerator(SCALE, 1, 1).forEach((nation) -> tpchSql.insertNation(nation));

        System.out.println("Region");
        TpchTable.REGION.createGenerator(SCALE, 1, 1).forEach((region) -> tpchSql.insertRegion(region));

        System.out.println("Part");
        partition(TpchTable.PART.createGenerator(SCALE, 1, 1), 100).forEach((parts) -> tpchSql.insertParts(parts));

        System.out.println("Customer");
        partition(TpchTable.CUSTOMER.createGenerator(SCALE, 1, 1), 1000).forEach((customers) -> tpchSql.insertCustomers(customers));

        System.out.println("Orders");
        partition(TpchTable.ORDERS.createGenerator(SCALE, 1, 1), 1000).forEach((orders) -> tpchSql.insertOrders(orders));

        System.out.println("Part Supplier");
        partition(TpchTable.PART_SUPPLIER.createGenerator(SCALE, 1, 1), 1000).forEach((partSuppliers) -> tpchSql.insertPartSuppliers(partSuppliers));

        System.out.println("Supplier");
        partition(TpchTable.SUPPLIER.createGenerator(SCALE, 1, 1), 100).forEach((suppliers) -> tpchSql.insertSuppliers(suppliers));

    }
}
