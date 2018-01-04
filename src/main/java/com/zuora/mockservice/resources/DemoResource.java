package com.zuora.mockservice.resources;

import static com.zuora.mockservice.mappers.TpchDao.FETCH_SIZE;
import static java.lang.String.format;

import com.zuora.mockservice.mappers.TpchMappers;

import com.google.common.util.concurrent.AtomicDouble;
import io.airlift.tpch.LineItem;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.DoubleColumnMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/demo")
public class DemoResource {

    private static final Logger LOG = LoggerFactory.getLogger(DemoResource.class);

    private final DBI dbi;

    public DemoResource(DBI dbi) {
        this.dbi = dbi;
    }

    @GET
    @Path("/1")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo1() throws Exception {
        Double result = dbi.withHandle(new HandleCallback<Double>() {

            @Override
            public Double withHandle(Handle handle) throws Exception {
                return handle.createQuery("SELECT SUM(extendedprice) FROM lineitem")
                        .map(DoubleColumnMapper.WRAPPER)
                        .first();
            }
        });

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

    @GET
    @Path("/2")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo2() throws Exception {
        AtomicDouble result = new AtomicDouble();

        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception {
                handle.getConnection().setAutoCommit(false);
                handle.createQuery("SELECT extendedprice FROM lineitem")
                        .setFetchSize(FETCH_SIZE)
                        .fetchForward()
                        .map(DoubleColumnMapper.WRAPPER)
                        .forEach(result::addAndGet);

                return null;
            }
        });

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

    @GET
    @Path("/3")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo3() throws Exception {
        AtomicDouble result = new AtomicDouble();

        Handle handle = dbi.open();
        handle.getConnection().setAutoCommit(false);

        try (ResultIterator<Double> it = handle
                .createQuery("SELECT extendedprice FROM lineitem")
                .setFetchSize(FETCH_SIZE)
                .fetchForward()
                .cleanupHandle()
                .map(DoubleColumnMapper.WRAPPER)
                .iterator()) {
            it.forEachRemaining(result::addAndGet);
        }

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

    @GET
    @Path("/4")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo4() throws Exception {
        AtomicDouble result = new AtomicDouble();

        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception {
                handle.getConnection().setAutoCommit(false);
                handle.createQuery("SELECT * FROM lineitem")
                        .setFetchSize(FETCH_SIZE)
                        .fetchForward()
                        .map(TpchMappers.LINE_ITEM_MAPPER)
                        .forEach(lineItem -> result.addAndGet(lineItem.getExtendedPrice()));

                return null;
            }
        });

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

    @GET
    @Path("/5")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo5() throws Exception {
        AtomicDouble result = new AtomicDouble();

        Handle handle = dbi.open();
        handle.getConnection().setAutoCommit(false);

        try (ResultIterator<LineItem> it = handle
                .createQuery("SELECT * FROM lineitem")
                .setFetchSize(FETCH_SIZE)
                .fetchForward()
                .cleanupHandle()
                .map(TpchMappers.LINE_ITEM_MAPPER)
                .iterator()) {
            it.forEachRemaining(lineItem -> result.addAndGet(lineItem.getExtendedPrice()));
        }

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

    @GET
    @Path("/6")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo6() throws Exception {
        AtomicDouble result = new AtomicDouble();

        dbi.withHandle(new HandleCallback<Void>() {

            @Override
            public Void withHandle(Handle handle) throws Exception {
                handle.getConnection().setAutoCommit(false);
                handle.createQuery("SELECT * FROM lineitem")
                        .setFetchSize(FETCH_SIZE)
                        .fetchForward()
                        .map(TpchMappers.LINE_ITEM_MAPPER)
                        .fold(result, (accumulator, lineItem, control, ctx) -> {
                            accumulator.addAndGet(lineItem.getExtendedPrice());
                            return accumulator;
                        });

                return null;
            }
        });

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

    @GET
    @Path("/7")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getDemo7() throws Exception {
        AtomicDouble result = new AtomicDouble();

        Handle handle = dbi.open();
        LOG.info("Autocommit is {}", handle.getConnection().getAutoCommit());
        handle.getConnection().setAutoCommit(false);
        try (ResultIterator<LineItem> it = handle
                .createQuery("SELECT * FROM lineitem")
                .setFetchSize(FETCH_SIZE)
                .fetchForward()
                .cleanupHandle()
                .map(TpchMappers.LINE_ITEM_MAPPER)
                .iterator()) {
            while (it.hasNext()) {
                LineItem lineItem = it.next();
                result.addAndGet(lineItem.getExtendedPrice());
            }
        }

        return Response.ok(format("%8.2f", result.doubleValue())).build();
    }

}

