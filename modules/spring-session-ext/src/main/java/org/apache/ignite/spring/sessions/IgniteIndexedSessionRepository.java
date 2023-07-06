package org.apache.ignite.spring.sessions;

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

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.spring.sessions.proxy.SessionProxy;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.session.DelegatingIndexResolver;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.FlushMode;
import org.springframework.session.IndexResolver;
import org.springframework.session.MapSession;
import org.springframework.session.PrincipalNameIndexResolver;
import org.springframework.session.SaveMode;
import org.springframework.session.Session;
import org.springframework.session.events.AbstractSessionEvent;
import org.springframework.session.events.SessionCreatedEvent;
import org.springframework.session.events.SessionDeletedEvent;
import org.springframework.session.events.SessionExpiredEvent;
import org.springframework.util.Assert;

/**
 * A {@link org.springframework.session.SessionRepository} implementation that stores
 * sessions in Apache Ignite distributed {@link SessionProxy}.
 *
 * <p>
 * An example of how to create a new instance can be seen below:
 * <pre class="code">
 * IgniteConfiguration config = new IgniteConfiguration();
 * // ... configure Ignite ...
 * Ignite ignite = IgnitionEx.start(config);
 * IgniteIndexedSessionRepository sessionRepository =
 *         new IgniteIndexedSessionRepository(ignite);
 * </pre>
 *
 * In order to support finding sessions by principal name using
 * {@link #findByIndexNameAndIndexValue(String, String)} method, custom configuration of
 * {@link SessionProxy} supplied to this implementation is required.
 * This implementation listens for events on the Ignite-backed SessionRepository and
 * translates those events into the corresponding Spring Session events. Publish the
 * Spring Session events with the given {@link ApplicationEventPublisher}.
 *
 * <ul>
 * <li>entryAdded - {@link SessionCreatedEvent}</li>
 * <li>entryEvicted - {@link SessionExpiredEvent}</li>
 * <li>entryRemoved - {@link SessionDeletedEvent}</li>
 * </ul>
 *
 */
public class IgniteIndexedSessionRepository
        implements FindByIndexNameSessionRepository<IgniteIndexedSessionRepository.IgniteSession>,
        CacheEntryCreatedListener<String, IgniteIndexedSessionRepository.IgniteSession>,
        CacheEntryRemovedListener<String, IgniteIndexedSessionRepository.IgniteSession>,
        CacheEntryExpiredListener<String, IgniteIndexedSessionRepository.IgniteSession> {
    /**
     * The default name of map used by Spring Session to store sessions.
     */
    public static final String DEFAULT_SESSION_MAP_NAME = "spring:session:sessions";

    /** */
    private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

    /** */
    private static final Log logger = LogFactory.getLog(IgniteIndexedSessionRepository.class);

    /** */
    private ApplicationEventPublisher eventPublisher = (event) -> {
    };

    /**
     * If non-null, this value is used to override
     * {@link MapSession#setMaxInactiveInterval(Duration)}.
     */
    private Integer defaultMaxInactiveInterval;

    /** */
    private IndexResolver<Session> indexResolver = new DelegatingIndexResolver<>(new PrincipalNameIndexResolver<>());

    /** */
    private FlushMode flushMode = FlushMode.ON_SAVE;

    /** */
    private SaveMode saveMode = SaveMode.ON_SET_ATTRIBUTE;

    /** */
    private final SessionProxy sessions;

    /** */
    private CacheEntryListenerConfiguration<String, IgniteSession> listenerConfiguration;

    /**
     * Create a new {@link IgniteIndexedSessionRepository} instance.
     * @param sessions the {@link SessionProxy} instance to use for managing sessions
     */
    public IgniteIndexedSessionRepository(SessionProxy sessions) {
        Assert.notNull(sessions, "Session proxy must not be null");
        this.sessions = sessions;
        
        this.listenerConfiguration = new CacheEntryListenerConfiguration<String, IgniteSession>() {
            /** {@inheritDoc} */
            @Override public Factory<CacheEntryListener<? super String, ? super IgniteSession>> getCacheEntryListenerFactory() {
                return () -> IgniteIndexedSessionRepository.this;
            }

            /** {@inheritDoc} */
            @Override public boolean isOldValueRequired() {
                return true;
            }

            /** {@inheritDoc} */
            @Override public Factory<CacheEntryEventFilter<? super String, ? super IgniteSession>> getCacheEntryEventFilterFactory() {
                return null;
            }

            /** {@inheritDoc} */
            @Override public boolean isSynchronous() {
                return false;
            }
        };

        this.sessions.registerCacheEntryListener(this.listenerConfiguration);
    }

    /** */
    @PreDestroy
    public void close() {
        this.sessions.deregisterCacheEntryListener(this.listenerConfiguration);
    }

    /**
     * Sets the {@link ApplicationEventPublisher} that is used to publish
     * {@link AbstractSessionEvent session events}. The default is to not publish session
     * events.
     * @param applicationEventPublisher the {@link ApplicationEventPublisher} that is used
     * to publish session events. Cannot be null.
     */
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        Assert.notNull(applicationEventPublisher, "ApplicationEventPublisher cannot be null");
        this.eventPublisher = applicationEventPublisher;
    }

    /**
     * Set the maximum inactive interval in seconds between requests before newly created
     * sessions will be invalidated. A negative time indicates that the session will never
     * timeout. The default is 1800 (30 minutes).
     * @param defaultMaxInactiveInterval the maximum inactive interval in seconds
     */
    public void setDefaultMaxInactiveInterval(Integer defaultMaxInactiveInterval) {
        this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
    }

    /**
     * Set the {@link IndexResolver} to use.
     * @param indexResolver the index resolver
     */
    public void setIndexResolver(IndexResolver<Session> indexResolver) {
        Assert.notNull(indexResolver, "indexResolver cannot be null");
        this.indexResolver = indexResolver;
    }

    /**
     * Sets the flush mode. Default flush mode is {@link FlushMode#ON_SAVE}.
     * @param flushMode the new flush mode
     */
    public void setFlushMode(FlushMode flushMode) {
        Assert.notNull(flushMode, "flushMode cannot be null");
        this.flushMode = flushMode;
    }

    /**
     * Set the save mode.
     * @param saveMode the save mode
     */
    public void setSaveMode(SaveMode saveMode) {
        Assert.notNull(saveMode, "saveMode must not be null");
        this.saveMode = saveMode;
    }

    /** {@inheritDoc} */
    @Override public IgniteSession createSession() {
        MapSession cached = new MapSession();
        if (this.defaultMaxInactiveInterval != null)
            cached.setMaxInactiveInterval(Duration.ofSeconds(this.defaultMaxInactiveInterval));

        IgniteSession session = new IgniteSession(cached, true);
        session.flushImmediateIfNecessary();
        return session;
    }

    /** {@inheritDoc} */
    @Override public void save(IgniteSession session) {
        if (session.isNew)
            ttlSessions(session.getMaxInactiveInterval()).put(session.getId(), session);

        else if (session.sessionIdChanged) {
            this.sessions.remove(session.originalId);
            session.originalId = session.getId();
            ttlSessions(session.getMaxInactiveInterval()).put(session.getId(), session);
        }
        else if (session.hasChanges()) {
            if (session.maxInactiveIntervalChanged)
                ttlSessions(session.getMaxInactiveInterval()).replace(session.getId(), session);
            else
                this.sessions.replace(session.getId(), session);
        }
        session.clearChangeFlags();
    }

    /** {@inheritDoc} */
    @Override public IgniteSession findById(String id) {
        IgniteSession saved = this.sessions.get(id);
        if (saved == null)
            return null;

        if (saved.isExpired()) {
            deleteById(saved.getId());
            return null;
        }
        saved.isNew = false;
        return saved;
    }

    /** {@inheritDoc} */
    @Override public void deleteById(String id) {
        this.sessions.remove(id);
    }

    /** {@inheritDoc} */
    @Override public Map<String, IgniteSession> findByIndexNameAndIndexValue(String indexName, String indexValue) {
        if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName))
            return Collections.emptyMap();

        final QueryCursor<List<?>> cursor = this.sessions
                .query(new SqlFieldsQuery("SELECT * FROM IgniteSession WHERE principal='" + indexValue + "'"));

        if (cursor == null)
            return Collections.emptyMap();

        final List<List<?>> sessions = cursor.getAll();

        Map<String, IgniteSession> sessionMap = new HashMap<>(sessions.size());

        sessions.forEach((List<?> res) -> {
            final MapSession session = (MapSession)res.get(1);
            final IgniteSession value = new IgniteSession(session, false);
            value.principal = (String)res.get(2);
            sessionMap.put(session.getId(), value);
        });

        return sessionMap;
    }

    /** {@inheritDoc} */
    @Override public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends IgniteSession>> events)
            throws CacheEntryListenerException {
        events.forEach((event) -> {
            IgniteSession session = event.getValue();
            if (session.getId().equals(session.getDelegate().getOriginalId())) {
                if (logger.isDebugEnabled())
                    logger.debug("Session created with id: " + session.getId());

                this.eventPublisher.publishEvent(new SessionCreatedEvent(this, session));
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends IgniteSession>> events)
        throws CacheEntryListenerException {
        events.forEach((event) -> {
            if (logger.isDebugEnabled())
                logger.debug("Session expired with id: " + event.getOldValue().getId());

            this.eventPublisher.publishEvent(new SessionExpiredEvent(this, event.getOldValue()));
        });
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends IgniteSession>> events)
            throws CacheEntryListenerException {
        events.forEach((event) -> {
            IgniteSession session = event.getOldValue();
            if (session != null) {
                if (logger.isDebugEnabled())
                    logger.debug("Session deleted with id: " + session.getId());

                this.eventPublisher.publishEvent(new SessionDeletedEvent(this, session));
            }
        });
    }

    /**
     * Get cache view with custom duration expiry policy.
     * @param duration expiry duration for IgniteSession.
     * @return cache with custom duration expiry policy.
     */
    private SessionProxy ttlSessions(Duration duration) {
        return this.sessions.withExpiryPolicy(createPolicy(duration));
    }

    /**
     * Create expiry policy from {@link Duration}.
     * @param duration expiry duration.
     * @return expiry policy.
     */
    private static TouchedExpiryPolicy createPolicy(Duration duration) {
        return new TouchedExpiryPolicy(new javax.cache.expiry.Duration(TimeUnit.SECONDS, duration.getSeconds()));
    }

    /**
     * A custom implementation of {@link Session} that uses a {@link MapSession} as the
     * basis for its mapping. It keeps track if changes have been made since last save.
     */
    public class IgniteSession implements Session {
        /** */
        private final MapSession delegate;

        /** */
        private boolean isNew;

        /** */
        private boolean sessionIdChanged;

        /** */
        private boolean lastAccessedTimeChanged;

        /** */
        private boolean maxInactiveIntervalChanged;

        /** */
        private String originalId;

        /** */
        private Map<String, Object> delta = new HashMap<>();

        /** */
        private String principal;

        /**
         * @param cached Map session.
         * @param isNew Is new flag.
         */
        IgniteSession(MapSession cached, boolean isNew) {
            this.delegate = cached;
            this.isNew = isNew;
            this.originalId = cached.getId();
            if (this.isNew || (IgniteIndexedSessionRepository.this.saveMode == SaveMode.ALWAYS))
                getAttributeNames()
                    .forEach((attributeName) -> this.delta.put(attributeName, cached.getAttribute(attributeName)));

        }

        /** {@inheritDoc} */
        @Override public void setLastAccessedTime(Instant lastAccessedTime) {
            this.delegate.setLastAccessedTime(lastAccessedTime);
            this.lastAccessedTimeChanged = true;
            flushImmediateIfNecessary();
        }

        /** {@inheritDoc} */
        @Override public boolean isExpired() {
            return this.delegate.isExpired();
        }

        /** {@inheritDoc} */
        @Override public Instant getCreationTime() {
            return this.delegate.getCreationTime();
        }

        /** {@inheritDoc} */
        @Override public String getId() {
            return this.delegate.getId();
        }

        /** {@inheritDoc} */
        @Override public String changeSessionId() {
            String newSessionId = this.delegate.changeSessionId();
            this.sessionIdChanged = true;
            return newSessionId;
        }

        /** {@inheritDoc} */
        @Override public Instant getLastAccessedTime() {
            return this.delegate.getLastAccessedTime();
        }

        /** {@inheritDoc} */
        @Override public void setMaxInactiveInterval(Duration interval) {
            this.delegate.setMaxInactiveInterval(interval);
            this.maxInactiveIntervalChanged = true;
            flushImmediateIfNecessary();
        }

        /** {@inheritDoc} */
        @Override public Duration getMaxInactiveInterval() {
            return this.delegate.getMaxInactiveInterval();
        }

        /** {@inheritDoc} */
        @Override public <T> T getAttribute(String attributeName) {
            T attributeValue = this.delegate.getAttribute(attributeName);
            if (attributeValue != null
                    && IgniteIndexedSessionRepository.this.saveMode.equals(SaveMode.ON_GET_ATTRIBUTE))
                this.delta.put(attributeName, attributeValue);

            return attributeValue;
        }

        /** {@inheritDoc} */
        @Override public Set<String> getAttributeNames() {
            return this.delegate.getAttributeNames();
        }

        /** {@inheritDoc} */
        @Override public void setAttribute(String attributeName, Object attributeValue) {
            this.delegate.setAttribute(attributeName, attributeValue);
            this.delta.put(attributeName, attributeValue);
            if (SPRING_SECURITY_CONTEXT.equals(attributeName)) {
                Map<String, String> indexes = IgniteIndexedSessionRepository.this.indexResolver.resolveIndexesFor(this);
                String principal = (attributeValue != null) ? indexes.get(PRINCIPAL_NAME_INDEX_NAME) : null;
                this.delegate.setAttribute(PRINCIPAL_NAME_INDEX_NAME, principal);
                this.principal = principal;
            }
            flushImmediateIfNecessary();
        }

        /** {@inheritDoc} */
        @Override public void removeAttribute(String attributeName) {
            setAttribute(attributeName, null);
        }

        /** */
        MapSession getDelegate() {
            return this.delegate;
        }

        /** */
        boolean hasChanges() {
            return (this.lastAccessedTimeChanged || this.maxInactiveIntervalChanged || !this.delta.isEmpty());
        }

        /** */
        void clearChangeFlags() {
            this.isNew = false;
            this.lastAccessedTimeChanged = false;
            this.sessionIdChanged = false;
            this.maxInactiveIntervalChanged = false;
            this.delta.clear();
        }

        /** */
        private void flushImmediateIfNecessary() {
            if (IgniteIndexedSessionRepository.this.flushMode == FlushMode.IMMEDIATE)
                IgniteIndexedSessionRepository.this.save(this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            IgniteSession session = (IgniteSession)o;
            return this.delegate.equals(session.delegate);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(this.delegate);
        }
    }
}
