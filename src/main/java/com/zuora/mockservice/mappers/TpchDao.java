package com.zuora.mockservice.mappers;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.Customer;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.Nation;
import io.airlift.tpch.Order;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartSupplier;
import io.airlift.tpch.Region;
import io.airlift.tpch.Supplier;
import io.airlift.tpch.TpchEntity;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Locale;

public class TpchDao
{
    private static final Logger LOG = LoggerFactory.getLogger(TpchDao.class);

    public static final int FETCH_SIZE = 1_000;


    private final DBI dbi;

    private static final ImmutableMap<Class<? extends TpchEntity>, ResultSetMapper<? extends TpchEntity>> ENTITY_MAP;
    private static final ImmutableMap<Class<? extends TpchEntity>, String> QUERY_MAP;

    static {
        ImmutableMap.Builder<Class<? extends TpchEntity>, ResultSetMapper<? extends TpchEntity>> builder = ImmutableMap.builder();
        builder.put(Part.class, TpchMappers.PART_MAPPER);
        builder.put(PartSupplier.class, TpchMappers.PART_SUPPLIER_MAPPER);
        builder.put(Supplier.class, TpchMappers.SUPPLIER_MAPPER);
        builder.put(Order.class, TpchMappers.ORDER_MAPPER);
        builder.put(Customer.class, TpchMappers.CUSTOMER_MAPPER);
        builder.put(LineItem.class, TpchMappers.LINE_ITEM_MAPPER);
        builder.put(Region.class, TpchMappers.REGION_MAPPER);
        builder.put(Nation.class, TpchMappers.NATION_MAPPER);

        ENTITY_MAP = builder.build();

        ImmutableMap.Builder<Class<? extends TpchEntity>, String> queryBuilder = ImmutableMap.builder();
        queryBuilder.put(Order.class, "SELECT * from ORDERS");
        queryBuilder.put(Customer.class, "SELECT * from CUSTOMER");
        queryBuilder.put(Part.class,  "SELECT * from PART");
        queryBuilder.put(PartSupplier.class,  "SELECT * from PARTSUPP");
        queryBuilder.put(Supplier.class,  "SELECT * from SUPPLIER");
        queryBuilder.put(LineItem.class,  "SELECT * from LINEITEM");
        queryBuilder.put(Region.class,  "SELECT * from REGION");
        queryBuilder.put(Nation.class,  "SELECT * from NATION");

        QUERY_MAP = queryBuilder.build();
    }


    public TpchDao(DBI dbi) {
        this.dbi = dbi;
    }

    @SuppressWarnings("unchecked")
    public <E extends TpchEntity> ResultIterator<E> streamEntity(Class<E> entityClass) throws SQLException
    {
        checkState(ENTITY_MAP.containsKey(entityClass), "Class %s is not an entity class", entityClass.getSimpleName());

        Handle handle = dbi.open();
        LOG.info(format(Locale.ENGLISH, "Found autocommit: %s", handle.getConnection().getAutoCommit()));
        handle.getConnection().setAutoCommit(false);

        return (ResultIterator<E>) handle
                .createQuery(QUERY_MAP.get(entityClass))
                .cleanupHandle()
                .setFetchSize(FETCH_SIZE)
                .fetchForward()
                .map(ENTITY_MAP.get(entityClass))
                .iterator();
    }
}
