package com.zuora.mockservice.resources;

import com.zuora.mockservice.mappers.TpchDao;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.Customer;
import io.airlift.tpch.CustomerColumn;
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
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnTypes;
import io.airlift.tpch.TpchEntity;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

public abstract class AbstractTpchResource<E extends TpchEntity>
{
    private static final ImmutableMap<Class<? extends TpchEntity>, Class<? extends TpchColumn<? extends TpchEntity>>> COLUMN_MAP;

    static {
        ImmutableMap.Builder<Class<? extends TpchEntity>, Class<? extends TpchColumn<? extends TpchEntity>>> builder = ImmutableMap.builder();
        builder.put(Part.class, PartColumn.class);
        builder.put(PartSupplier.class, PartSupplierColumn.class);
        builder.put(Supplier.class, SupplierColumn.class);
        builder.put(Order.class, OrderColumn.class);
        builder.put(Customer.class, CustomerColumn.class);
        builder.put(LineItem.class, LineItemColumn.class);
        builder.put(Region.class, RegionColumn.class);
        builder.put(Nation.class, NationColumn.class);

        COLUMN_MAP = builder.build();
    }

    @Path("/customer")
    public static class CustomerResource extends AbstractTpchResource<Customer>
    {
        public CustomerResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(Customer.class, objectMapper, tpchDao);
        }
    }

    @Path("/order")
    public static class OrderResource extends AbstractTpchResource<Order>
    {
        public OrderResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(Order.class, objectMapper, tpchDao);
        }
    }

    @Path("/part")
    public static class PartResource extends AbstractTpchResource<Part>
    {
        public PartResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(Part.class, objectMapper, tpchDao);
        }
    }

    @Path("/part-supplier")
    public static class PartSupplierResource extends AbstractTpchResource<PartSupplier>
    {
        public PartSupplierResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(PartSupplier.class, objectMapper, tpchDao);
        }
    }

    @Path("/supplier")
    public static class SupplierResource extends AbstractTpchResource<Supplier>
    {
        public SupplierResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(Supplier.class, objectMapper, tpchDao);
        }
    }

    @Path("/line-item")
    public static class LineItemResource extends AbstractTpchResource<LineItem>
    {
        public LineItemResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(LineItem.class, objectMapper, tpchDao);
        }
    }

    @Path("/region")
    public static class RegionResource extends AbstractTpchResource<Region>
    {
        public RegionResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(Region.class, objectMapper, tpchDao);
        }
    }

    @Path("/nation")
    public static class NationResource extends AbstractTpchResource<Nation>
    {
        public NationResource(ObjectMapper objectMapper,
                TpchDao tpchDao)
        {
            super(Nation.class, objectMapper, tpchDao);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTpchResource.class);

    private final ObjectMapper objectMapper;
    private final TpchDao tpchDao;
    private final Class<E> entityClass;

    public AbstractTpchResource(Class<E> entityClass,
            ObjectMapper objectMapper,
            TpchDao tpchDao)
    {
        this.entityClass = entityClass;
        this.objectMapper = objectMapper;
        this.tpchDao = tpchDao;
    }

    @GET
    @Path("/query")
    @Produces(MediaType.APPLICATION_JSON)
    public Response query() throws Exception
    {
        ResultIterator<E> stream = tpchDao.streamEntity(entityClass);
        return Response.ok(streamResult(stream)).build();
    }

    @GET
    @Path("/meta")
    @Produces(MediaType.APPLICATION_JSON)
    public Response meta() throws Exception
    {
        Class<? extends TpchColumn<? extends TpchEntity>> columnClass = COLUMN_MAP.get(entityClass);

        ImmutableMap.Builder<String, String> resultBuilder = ImmutableMap.builder();
        resultBuilder.put("rowKey", TpchColumnTypes.INTEGER.getBase().toString());

        for (TpchColumn<? extends TpchEntity> column : columnClass.getEnumConstants()) {
            String name = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.toString());
            resultBuilder.put(name, column.getType().getBase().toString());
        }

        return Response.ok(resultBuilder.build()).build();
    }

    private StreamingOutput streamResult(ResultIterator<E> stream) {

        return new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                JsonGenerator generator = objectMapper.getFactory().createGenerator(output);
                generator.writeStartObject();
                generator.writeArrayFieldStart("data");

                AtomicInteger count = new AtomicInteger();

                try {
                    while (stream.hasNext()) {
                        E entity = stream.next();
                        try {
                            generator.writeObject(entity);
                            count.incrementAndGet();
                            output.flush();
                        } catch (EOFException e) {
                            LOG.warn("Stream closed");
                            break;

                        } catch (IOException e) {
                            LOG.warn("Oops", e);
                        }
                    }
                }
                finally {
                    stream.close();
                    generator.writeEndArray();
                    generator.writeNumberField("count", count.get());
                    generator.writeEndObject();
                    generator.close();
                }
            }
        };
    }
}
