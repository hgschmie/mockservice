package com.zuora.mockservice;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class ObjectMapperProvider
{
    private final ObjectMapper objectMapper;

    public ObjectMapperProvider(ObjectMapper objectMapper)
    {
        this.objectMapper = checkNotNull(objectMapper, "objectMapper is null");
    }

    public ObjectMapper get()
    {
        ObjectMapper objectMapper = this.objectMapper.copy();
        configure(objectMapper);
        return objectMapper;
    }

    public static final void configure(final ObjectMapper objectMapper)
    {
        checkNotNull(objectMapper, "objectMapper is null");

        // ignore unknown fields (for backwards compatibility)
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // use ISO dates
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // skip fields that are null instead of writing an explicit json null value
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);

        // disable auto detection of json properties... all properties must be explicit
        objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
        objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
        objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());
    }
}
