/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.springdata.repository.query;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Custom conversions implementation.
 * An application can define its own converter by defining the following bean:
 * <pre>
 * {@code
 *     @Bean
 *     public CustomConversions customConversions() {
 *         return new IgniteCustomConversions(Arrays.asList(new LocalDateTimeWriteConverter()));
 *     }
 * }
 * </pre>
 */
public class IgniteCustomConversions extends CustomConversions {

    private static final StoreConversions STORE_CONVERSIONS;

    static {
        List<Object> converters = new ArrayList<>();
        converters.add(new TimestampToLocalDateTimeConverter());
        converters.add(new TimestampToDateConverter());

        List<Object> storeConverters = Collections.unmodifiableList(converters);
        STORE_CONVERSIONS = StoreConversions.of(SimpleTypeHolder.DEFAULT, storeConverters);
    }

    public IgniteCustomConversions() {
        this(Collections.emptyList());
    }

    public IgniteCustomConversions(List<?> converters) {
        super(STORE_CONVERSIONS, converters);
    }

    @WritingConverter
    static class TimestampToLocalDateTimeConverter implements Converter<Timestamp, LocalDateTime> {
        @Override public LocalDateTime convert(Timestamp source) {
            return source.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        }
    }

    @WritingConverter
    static class TimestampToDateConverter implements Converter<Timestamp, Date> {
        @Override public Date convert(Timestamp source) {
            return new Date((source).getTime());
        }
    }
}
