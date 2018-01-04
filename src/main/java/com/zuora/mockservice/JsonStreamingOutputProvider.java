package com.zuora.mockservice;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.ext.MessageBodyWriter;

import javax.inject.Singleton;


@Produces({MediaType.APPLICATION_JSON})
@Singleton
public final class JsonStreamingOutputProvider implements MessageBodyWriter<StreamingOutput> {

    @Override
    public boolean isWriteable(Class<?> t, Type gt, Annotation[] as, MediaType mediaType) {
        return StreamingOutput.class.isAssignableFrom(t);
    }

    @Override
    public long getSize(StreamingOutput o, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(StreamingOutput o, Class<?> t, Type gt, Annotation[] as,
            MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
            OutputStream entity) throws IOException {
        o.write(entity);
    }
}
