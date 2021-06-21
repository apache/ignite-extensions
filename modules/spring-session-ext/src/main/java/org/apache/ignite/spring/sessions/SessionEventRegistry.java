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


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.context.ApplicationListener;
import org.springframework.session.events.AbstractSessionEvent;

/**
 *
 */
class SessionEventRegistry implements ApplicationListener<AbstractSessionEvent> {

    /** */
    private Map<String, AbstractSessionEvent> events = new HashMap<>();

    /** */
    private ConcurrentMap<String, Object> locks = new ConcurrentHashMap<>();

    /** */
    @Override public void onApplicationEvent(AbstractSessionEvent event) {
        String sessionId = event.getSessionId();
        this.events.put(sessionId, event);
        Object lock = getLock(sessionId);
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /** */
    void clear() {
        this.events.clear();
        this.locks.clear();
    }

    /** */
    boolean receivedEvent(String sessionId) throws InterruptedException {
        return waitForEvent(sessionId) != null;
    }

    /** */
    <E extends AbstractSessionEvent> E getEvent(String sessionId) throws InterruptedException {
        return (E) waitForEvent(sessionId);
    }

    /** */
    private <E extends AbstractSessionEvent> E waitForEvent(String sessionId) throws InterruptedException {
        Object lock = getLock(sessionId);
        synchronized (lock) {
            if (!this.events.containsKey(sessionId))
                lock.wait(10000);

        }
        return (E) this.events.get(sessionId);
    }

    /** */
    private Object getLock(String sessionId) {
        return this.locks.computeIfAbsent(sessionId, (k) -> new Object());
    }
}

