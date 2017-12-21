package com.zuora.mockservice;

import static com.google.common.collect.Iterables.partition;

import com.zuora.mockservice.mappers.TpchSql;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import io.airlift.tpch.TpchTable;
import org.skife.jdbi.v2.DBI;

public class DataGeneratorMain {
    public static final void main(String... args) throws Exception {
        DataGeneratorMain dataGenerator = new DataGeneratorMain();
        dataGenerator.generate();
    }

    private DataGeneratorMain() {}


    public void generate() throws Exception {

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl("jdbc:mysql://localhost:3306/tpch");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        dataSource.setUseSSL(false);

        DBI dbi = new DBI(dataSource);

        TpchSql tpchSql = new TpchSql(dbi);

        double scale = 1; // 0.01;


        System.out.println("LineItem");
        partition(TpchTable.LINE_ITEM.createGenerator(scale, 1, 1), 1000).forEach((lineItems) -> tpchSql.insertLineItems(lineItems));

        System.out.println("Nation");
        TpchTable.NATION.createGenerator(scale, 1, 1).forEach((nation) -> tpchSql.insertNation(nation));

        System.out.println("Region");
        TpchTable.REGION.createGenerator(scale, 1, 1).forEach((region) -> tpchSql.insertRegion(region));

        System.out.println("Part");
        partition(TpchTable.PART.createGenerator(scale, 1, 1), 100).forEach((parts) -> tpchSql.insertParts(parts));

        System.out.println("Customer");
        partition(TpchTable.CUSTOMER.createGenerator(scale, 1, 1), 1000).forEach((customers) -> tpchSql.insertCustomers(customers));

        System.out.println("Orders");
        partition(TpchTable.ORDERS.createGenerator(scale, 1, 1), 1000).forEach((orders) -> tpchSql.insertOrders(orders));

        System.out.println("Part Supplier");
        partition(TpchTable.PART_SUPPLIER.createGenerator(scale, 1, 1), 1000).forEach((partSuppliers) -> tpchSql.insertPartSuppliers(partSuppliers));

        System.out.println("Supplier");
        partition(TpchTable.SUPPLIER.createGenerator(scale, 1, 1), 100).forEach((suppliers) -> tpchSql.insertSuppliers(suppliers));

    }
}
