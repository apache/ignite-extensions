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

package org.apache.ignite.osgi.ioc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.ioc.Registry;
import org.apache.ignite.lang.IgniteExperimental;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

/**
 * A basic injection registry backed by OSGi service registry.
 *
 * Semantics of this registry is limited to lookup of services based on type (objectClass) or
 * complete filter expression taken directly from @{@link org.apache.ignite.resources.InjectResource}'s
 * name attribute.
 *
 * This class is marked as experimental, and it is not intended for production use yet.
 */
@IgniteExperimental
public class OsgiRegistry implements Registry {

  private final BundleContext bundleContext;

  public OsgiRegistry(BundleContext bundleContext) {
    this.bundleContext = bundleContext;
  }

  @Override
  public <T> T lookup(Class<T> type) {
    ServiceReference<T> serviceReference = bundleContext.getServiceReference(type);
    if (serviceReference == null) {
      return null;
    }

    return wrap(serviceReference);
  }

  @Override
  public Object lookup(String name) {
    try {
      ServiceReference<?>[] serviceReference = bundleContext.getServiceReferences((String) null, name);
      if (serviceReference == null || serviceReference.length == 0) {
        return null;
      }

      // TODO consider proxying returned service to keep its lifecycle in sync with context itself
      return wrap(serviceReference[0]);
    } catch (InvalidSyntaxException e) {
      return null;
    }
  }

  private <T> T wrap(ServiceReference<T> serviceReference) {
    // TODO proxying of returned service should keep its lifecycle in synAddc with bundle context,
    // ideally we should be calling bundleContext.ungetService after we're done with it
    return bundleContext.getService(serviceReference);
  }

  @Override
  public Object unwrapTarget(Object target) throws IgniteCheckedException {
    return target;
  }

}
