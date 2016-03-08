/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.mapping;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.datastax.driver.core.Cluster;
import com.google.common.base.Throwables;
import com.google.common.cache.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Metrics exposed by the object mapper.
 * <p/>
 * The metrics exposed by this class use the <a href="http://metrics.codahale.com/">Metrics</a>
 * library and you should refer its <a href="http://metrics.codahale.com/manual/">documentation</a>
 * for details on how to handle the exposed metric objects.
 * <p/>
 * By default, metrics are exposed through JMX, which is very useful for
 * development and browsing, but for production environments you may want to
 * have a look at the <a href="http://metrics.codahale.com/manual/core/#reporters">reporters</a>
 * provided by the Metrics library which could be more efficient/adapted.
 */
public class MapperMetrics {

    /**
     * Statistics and metrics about a given entity.
     */
    public static class EntityMetrics {

        private final Class<?> entityClass;

        private final Histogram nullFieldsHistogram;

        public EntityMetrics(Class<?> entityClass, Histogram nullFieldsHistogram) {
            this.entityClass = entityClass;
            this.nullFieldsHistogram = nullFieldsHistogram;
        }

        /**
         * @return The entity class.
         */
        public Class<?> getEntityClass() {
            return entityClass;
        }

        /**
         * @return the histogram for saved null fields.
         */
        public Histogram getNullFieldsHistogram() {
            return nullFieldsHistogram;
        }
    }

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private class EntityMetricsLoader extends CacheLoader<Class<?>, EntityMetrics> {

        @Override
        public EntityMetrics load(Class<?> entityClass) throws Exception {
            // TODO configurable sliding window?
            EntityMetrics metrics = new EntityMetrics(entityClass, new Histogram(new SlidingTimeWindowReservoir(5, MINUTES)));
            registry.register(String.format("mapper-%s-%s-null-fields", id, entityClass.getName()), metrics.nullFieldsHistogram);
            return metrics;
        }

    }

    private class EntityMetricsRemovalListener implements RemovalListener<Class<?>, EntityMetrics> {

        @Override
        public void onRemoval(RemovalNotification<Class<?>, EntityMetrics> notification) {
            Class<?> entityClass = notification.getKey();
            if (entityClass != null)
                registry.remove(String.format("mapper-%s-%s-null-fields", id, entityClass.getName()));
        }

    }

    private final MetricRegistry registry;

    // distinguish between different instances of this class
    private final int id;

    private final LoadingCache<Class<?>, EntityMetrics> metricsByEntity = CacheBuilder.newBuilder()
            .expireAfterAccess(5, MINUTES)
            .removalListener(new EntityMetricsRemovalListener())
            .build(new EntityMetricsLoader());

    MapperMetrics(Cluster cluster) {
        registry = cluster.getMetrics().getRegistry();
        id = COUNTER.incrementAndGet();
    }

    /**
     * Gathers metrics about the given entity class.
     *
     * @param entityClass the entity class to gather metrics for.
     * @return An {@link EntityMetrics} with metrics for the given entity class.
     */
    public EntityMetrics getEntityMetrics(Class<?> entityClass) {
        EntityMetrics metrics = null;
        try {
            metrics = metricsByEntity.get(entityClass);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
        }
        return metrics;
    }

}
