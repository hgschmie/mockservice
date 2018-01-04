package com.zuora.mockservice.resources;

import static com.google.common.base.Preconditions.checkState;

import com.zuora.mockservice.DataGeneratorCommand;
import com.zuora.mockservice.mappers.TpchDao;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
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
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchColumnTypes;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

public abstract class AbstractTpchResource<E extends TpchEntity> {

    private static final ImmutableMap<Class<? extends TpchEntity>, Class<? extends TpchColumn<? extends TpchEntity>>> COLUMN_MAP;

    public static final String SMILE_MEDIATYPE = "application/x-jackson-smile";

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
    public static class CustomerResource extends AbstractTpchResource<Customer> {

        public CustomerResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(Customer.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.CUSTOMER);
        }
    }

    @Path("/orders")
    public static class OrderResource extends AbstractTpchResource<Order> {

        public OrderResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(Order.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.ORDERS);
        }
    }

    @Path("/part")
    public static class PartResource extends AbstractTpchResource<Part> {

        public PartResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(Part.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.PART);
        }
    }

    @Path("/partsupp")
    public static class PartSupplierResource extends AbstractTpchResource<PartSupplier> {

        public PartSupplierResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(PartSupplier.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.PART_SUPPLIER);
        }
    }

    @Path("/supplier")
    public static class SupplierResource extends AbstractTpchResource<Supplier> {

        public SupplierResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(Supplier.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.SUPPLIER);
        }
    }

    @Path("/lineitem")
    public static class LineItemResource extends AbstractTpchResource<LineItem> {

        public LineItemResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(LineItem.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.LINE_ITEM);
        }
    }

    @Path("/region")
    public static class RegionResource extends AbstractTpchResource<Region> {

        public RegionResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(Region.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.REGION);
        }
    }

    @Path("/nation")
    public static class NationResource extends AbstractTpchResource<Nation> {

        public NationResource(ObjectMapper jsonObjectMapper,
                ObjectMapper smileObjectMapper,
                TpchDao tpchDao) {
            super(Nation.class, jsonObjectMapper, smileObjectMapper, tpchDao, TpchTable.NATION);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTpchResource.class);

    private final ObjectMapper jsonObjectMapper;
    private final ObjectMapper smileObjectMapper;
    private final TpchDao tpchDao;
    private final Class<E> entityClass;
    private final TpchTable<E> tpchTable;

    public AbstractTpchResource(Class<E> entityClass,
            ObjectMapper jsonObjectMapper,
            ObjectMapper smileObjectMapper,
            TpchDao tpchDao,
            TpchTable<E> tpchTable) {
        this.entityClass = entityClass;
        this.jsonObjectMapper = jsonObjectMapper;
        this.smileObjectMapper = smileObjectMapper;
        this.tpchDao = tpchDao;
        this.tpchTable = tpchTable;
    }

    @GET
    @Path("/count")
    @Produces(MediaType.TEXT_PLAIN)
    public Response count() throws Exception {
        AtomicLong count = new AtomicLong();
        try (ResultIterator<E> stream = tpchDao.streamEntity(entityClass)) {
            stream.forEachRemaining(consumer -> count.incrementAndGet());
        }
        return Response.ok(count.longValue()).build();

    }

    @GET
    @Path("/query")
    @Produces({MediaType.APPLICATION_JSON, SMILE_MEDIATYPE})
    public Response query(@Context HttpHeaders headers) throws Exception {
        String accept = headers.getHeaderString(HttpHeaders.ACCEPT);
        List<MediaType> acceptableType = headers.getAcceptableMediaTypes();
        checkState(acceptableType.size() == 1);

        ObjectMapper objectMapper = SMILE_MEDIATYPE.equals(acceptableType.get(0)) ? smileObjectMapper : jsonObjectMapper;

        ResultIterator<E> stream = tpchDao.streamEntity(entityClass);
        return Response.ok(streamResult(objectMapper, stream)).build();
    }

    @GET
    @Path("/tpch")
    @Produces({MediaType.APPLICATION_JSON, SMILE_MEDIATYPE})
    public Response tpch(@Context HttpHeaders headers) throws Exception {
        String accept = headers.getHeaderString(HttpHeaders.ACCEPT);
        List<MediaType> acceptableType = headers.getAcceptableMediaTypes();
        checkState(acceptableType.size() == 1);

        ObjectMapper objectMapper = SMILE_MEDIATYPE.equals(acceptableType.get(0)) ? smileObjectMapper : jsonObjectMapper;

        Iterable<E> stream = tpchTable.createGenerator(DataGeneratorCommand.SCALE, 1, 1);
        return Response.ok(streamResult(objectMapper, stream.iterator())).build();
    }


    @GET
    @Path("/meta")
    @Produces({MediaType.APPLICATION_JSON, SMILE_MEDIATYPE})
    public Response meta() throws Exception {
        Class<? extends TpchColumn<? extends TpchEntity>> columnClass = COLUMN_MAP.get(entityClass);

        ImmutableList.Builder<ColumnDescriptor> resultBuilder = ImmutableList.builder();
        ColumnDescriptor columnDescriptor = new ColumnDescriptor("rowNumber", RowKeyColumn.ROW_KEY, true);
        resultBuilder.add(columnDescriptor);

        for (TpchColumn<? extends TpchEntity> column : columnClass.getEnumConstants()) {
            String name = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.toString());
            resultBuilder.add(new ColumnDescriptor(name, column, false));
        }

        return Response.ok(ImmutableMap.of("data", resultBuilder.build())).build();
    }

    public enum RowKeyColumn
            implements TpchColumn<RowKeyColumn>, TpchEntity {
        ROW_KEY() {
            @Override
            public long getRowNumber() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String toLine() {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getDouble(RowKeyColumn rowKeyColumn) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getIdentifier(RowKeyColumn rowKeyColumn) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getInteger(RowKeyColumn rowKeyColumn) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getString(RowKeyColumn rowKeyColumn) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getDate(RowKeyColumn rowKeyColumn) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getColumnName() {
                return "_rowkey";
            }

            @Override
            public TpchColumnType getType() {
                return TpchColumnTypes.IDENTIFIER;
            }
        }
    }


    private StreamingOutput streamResult(ObjectMapper objectMapper, Iterator<E> stream) {

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
                } finally {
                    if (stream instanceof Closeable) {
                        ((Closeable) stream).close();
                    }
                    generator.writeEndArray();
                    generator.writeNumberField("count", count.get());
                    generator.writeEndObject();
                    generator.close();
                }
            }
        };
    }
}
