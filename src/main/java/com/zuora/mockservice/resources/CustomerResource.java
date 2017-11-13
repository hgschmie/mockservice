package com.zuora.mockservice.resources;

import com.zuora.mockservice.mappers.TpchDao;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.tpch.Customer;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

@Path("/customer")
public class CustomerResource
{
    private static final Logger LOG = LoggerFactory.getLogger(CustomerResource.class);

    private final ObjectMapper objectMapper;
    private final TpchDao tpchDao;

    public CustomerResource(ObjectMapper objectMapper,
            TpchDao tpchDao)
    {
        this.objectMapper = objectMapper;
        this.tpchDao = tpchDao;
    }

    @GET
    @Path("/query")
    public Response query() throws Exception
    {
        ResultIterator<Customer> stream = tpchDao.streamCustomer();

        return Response.ok(streamResult(stream)).build();
    }

    private StreamingOutput streamResult(ResultIterator<Customer> stream) {

        return new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                JsonGenerator generator = objectMapper.getFactory().createGenerator(output);
                generator.writeStartObject();
                generator.writeArrayFieldStart("data");

                AtomicInteger count = new AtomicInteger();

                try {
                    while (stream.hasNext()) {
                        Customer customer = stream.next();
                        try {
                            generator.writeObject(customer);
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
