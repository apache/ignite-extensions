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

package org.apache.ignite.springdata.misc;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.apache.ignite.springdata.repository.query.IgniteCustomConversions;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.CustomConversions;

import static java.util.Collections.singletonList;

/**
 *  Test configuration that overrides default converter for LocalDateTime type.
 */
public class CustomConvertersApplicationConfiguration extends ApplicationConfiguration {
    /**
     * @return Custom conversions bean.
     */
    @Bean
    public CustomConversions customConversions() {
        return new IgniteCustomConversions(singletonList(new LocalDateTimeWriteConverter()));
    }

    /**
     * Converter for {@link Timestamp} to {@link LocalDateTime}.
     */
    static class LocalDateTimeWriteConverter implements Converter<Timestamp, LocalDateTime> {
        /** {@inheritDoc} */
        @Override public LocalDateTime convert(Timestamp source) {
            return null;
        }
    }
}
