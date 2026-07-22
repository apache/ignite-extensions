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

package org.apache.ignite.osgi.blueprint.ioc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.ioc.Registry;
import org.apache.ignite.lang.IgniteExperimental;
import org.osgi.framework.Filter;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.osgi.service.blueprint.container.BlueprintContainer;
import org.osgi.service.blueprint.container.NoSuchComponentException;
import org.osgi.service.blueprint.reflect.ComponentMetadata;

/**
 * Injection registry which integrates OSGi Blueprint with Ignite IoC infrastructure.
 *
 * This registry is able to link beans defined in Blueprint XML - either by name or type.
 * Type evaluation is performed through comparison of bean instances. Only first bean matching given
 * type will be selected for injection.
 */
@IgniteExperimental
public class BlueprintRegistry implements Registry {

  private final BlueprintContainer container;

  public BlueprintRegistry(BlueprintContainer container) {
    this.container = container;
  }

  @Override
  public <T> T lookup(Class<T> type) {
    for (String id : container.getComponentIds()) {
      Object bean = container.getComponentInstance(id);
      if (type.isInstance(bean)) {
        return type.cast(bean);
      }
    }
    return null;
  }

  @Override
  public Object lookup(String name) {
    try {
      return container.getComponentInstance(name);
    } catch (NoSuchComponentException e) {
      return null;
    }
  }

  @Override
  public Object unwrapTarget(Object target) throws IgniteCheckedException {
    return target;
  }

}
