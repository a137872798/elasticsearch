/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.UnaryOperator;

/**
 * Script level doc values, the assumption is that any implementation will
 * implement a {@link Longs#getValue getValue} method.
 *
 * Implementations should not internally re-use objects for the values that they
 * return as a single {@link ScriptDocValues} instance can be reused to return
 * values form multiple documents.
 * 什么叫脚本docValue呢 ???
 */
public abstract class ScriptDocValues<T> extends AbstractList<T> {

    /**
     * Set the current doc ID.
     * 应该跟lucene是一个套路 每当调用这个方法时 就更新内部的doc数据
     */
    public abstract void setNextDocId(int docId) throws IOException;

    // 这个list不允许插入元素 也无法移除元素

    // Throw meaningful exceptions if someone tries to modify the ScriptDocValues.
    @Override
    public final void add(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final T set(int index, T element) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    @Override
    public final void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException("doc values are unmodifiable");
    }

    /**
     * 代表该列表内部都是 Long类型的数值
     */
    public static final class Longs extends ScriptDocValues<Long> {

        /**
         * 这个是lucene 内置的long型数值迭代器
         * 同时SortedNumericDocValues 还代表着doc顺序已经按照数值大小进行排序了
         */
        private final SortedNumericDocValues in;
        /**
         * 填充当前doc下的所有long值
         */
        private long[] values = new long[0];
        /**
         * 当前doc下有多少long数值
         */
        private int count;

        /**
         * Standard constructor.
         */
        public Longs(SortedNumericDocValues in) {
            this.in = in;
        }

        /**
         * 通过指定docId 定位到某个doc
         * @param docId
         * @throws IOException
         */
        @Override
        public void setNextDocId(int docId) throws IOException {
            // 确保存在该doc
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         * 代表当前doc下 有多少个数值
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public long getValue() {
            return get(0);
        }

        @Override
        public Long get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    /**
     * 代表每个doc 下存储的都是一组时间
     */
    public static final class Dates extends ScriptDocValues<JodaCompatibleZonedDateTime> {

        private final SortedNumericDocValues in;

        /**
         * 时间是否以纳秒为单位
         */
        private final boolean isNanos;

        /**
         * Values wrapped in {@link java.time.ZonedDateTime} objects.
         * 也是跟上面一样 一个doc下可能有一组date
         */
        private JodaCompatibleZonedDateTime[] dates;
        private int count;

        public Dates(SortedNumericDocValues in, boolean isNanos) {
            this.in = in;
            this.isNanos = isNanos;
        }

        /**
         * Fetch the first field value or 0 millis after epoch if there are no
         * in.
         */
        public JodaCompatibleZonedDateTime getValue() {
            return get(0);
        }

        @Override
        public JodaCompatibleZonedDateTime get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            if (index >= count) {
                throw new IndexOutOfBoundsException(
                        "attempted to fetch the [" + index + "] date when there are only ["
                                + count + "] dates.");
            }
            return dates[index];
        }

        @Override
        public int size() {
            return count;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                count = in.docValueCount();
            } else {
                count = 0;
            }
            refreshArray();
        }

        /**
         * Refresh the backing array. Package private so it can be called when {@link Longs} loads dates.
         */
        void refreshArray() throws IOException {
            // 这里没有缩容数组是因为  在count=0的时候调用get 会抛出异常
            if (count == 0) {
                return;
            }
            // 需要对数组进行扩容
            if (dates == null || count > dates.length) {
                // Happens for the document. We delay allocating dates so we can allocate it with a reasonable size.
                dates = new JodaCompatibleZonedDateTime[count];
            }
            // 这里是开始填充数据， 根据不同的时间单位使用不同的构造函数
            for (int i = 0; i < count; ++i) {
                if (isNanos) {
                    dates[i] = new JodaCompatibleZonedDateTime(DateUtils.toInstant(in.nextValue()), ZoneOffset.UTC);
                } else {
                    dates[i] = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(in.nextValue()), ZoneOffset.UTC);
                }
            }
        }
    }

    /**
     * 代表每个doc内部都是一组double
     */
    public static final class Doubles extends ScriptDocValues<Double> {

        /**
         * 这是ES自己封装的类  区别nextValue 返回的是double类型
         */
        private final SortedNumericDoubleValues in;
        private double[] values = new double[0];
        private int count;

        public Doubles(SortedNumericDoubleValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue();
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = ArrayUtil.grow(values, count);
        }

        public SortedNumericDoubleValues getInternalValues() {
            return this.in;
        }

        public double getValue() {
            return get(0);
        }

        @Override
        public Double get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }
    }

    /**
     * 标记地理位置的一组docValues
     */
    public static final class GeoPoints extends ScriptDocValues<GeoPoint> {

        /**
         * gen docValue迭代器
         */
        private final MultiGeoPointValues in;
        /**
         * 每个GenPoint对象都包含一个经纬度
         */
        private GeoPoint[] values = new GeoPoint[0];
        private int count;

        public GeoPoints(MultiGeoPointValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    GeoPoint point = in.nextValue();
                    values[i] = new GeoPoint(point.lat(), point.lon());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         * 对数组进行扩容
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                values[i] = new GeoPoint();
                }
            }
        }

        public GeoPoint getValue() {
            return get(0);
        }

        public double getLat() {
            return getValue().lat();
        }

        /**
         * 将当前doc下所有的经/纬度取出来
         * @return
         */
        public double[] getLats() {
            double[] lats = new double[size()];
            for (int i = 0; i < size(); i++) {
                lats[i] = get(i).lat();
            }
            return lats;
        }

        public double[] getLons() {
            double[] lons = new double[size()];
            for (int i = 0; i < size(); i++) {
                lons[i] = get(i).lon();
            }
            return lons;
        }

        public double getLon() {
            return getValue().lon();
        }

        @Override
        public GeoPoint get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            final GeoPoint point = values[index];
            return new GeoPoint(point.lat(), point.lon());
        }

        @Override
        public int size() {
            return count;
        }

        /**
         * 根据当前读取到的经纬度 与 传入的经纬度 计算两点间的距离
         * @param lat
         * @param lon
         * @return
         */
        public double arcDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), lat, lon);
        }

        public double arcDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return arcDistance(lat, lon);
        }

        public double planeDistance(double lat, double lon) {
            GeoPoint point = getValue();
            return GeoUtils.planeDistance(point.lat(), point.lon(), lat, lon);
        }

        public double planeDistanceWithDefault(double lat, double lon, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return planeDistance(lat, lon);
        }

        public double geohashDistance(String geohash) {
            GeoPoint point = getValue();
            return GeoUtils.arcDistance(point.lat(), point.lon(), Geohash.decodeLatitude(geohash),
                Geohash.decodeLongitude(geohash));
        }

        public double geohashDistanceWithDefault(String geohash, double defaultValue) {
            if (isEmpty()) {
                return defaultValue;
            }
            return geohashDistance(geohash);
        }
    }

    /**
     * 每个doc内部存储了一组boolean
     */
    public static final class Booleans extends ScriptDocValues<Boolean> {

        private final SortedNumericDocValues in;
        private boolean[] values = new boolean[0];
        private int count;

        public Booleans(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    values[i] = in.nextValue() == 1;
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            values = grow(values, count);
        }

        public boolean getValue() {
            return get(0);
        }

        @Override
        public Boolean get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index];
        }

        @Override
        public int size() {
            return count;
        }

        private static boolean[] grow(boolean[] array, int minSize) {
            assert minSize >= 0 : "size must be positive (got " + minSize
                    + "): likely integer overflow?";
            if (array.length < minSize) {
                return Arrays.copyOf(array, ArrayUtil.oversize(minSize, 1));
            } else
                return array;
        }

    }

    /**
     * doc下存储了一组 bytes
     * @param <T>
     */
    abstract static class BinaryScriptDocValues<T> extends ScriptDocValues<T> {

        private final SortedBinaryDocValues in;

        /**
         * BytesRefBuilder内置了一个ByteRef
         */
        protected BytesRefBuilder[] values = new BytesRefBuilder[0];
        protected int count;

        BinaryScriptDocValues(SortedBinaryDocValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    // We need to make a copy here, because BytesBinaryDVLeafFieldData's SortedBinaryDocValues
                    // implementation reuses the returned BytesRef. Otherwise we would end up with the same BytesRef
                    // instance for all slots in the values array.
                    // 将数据转移到builder内部
                    values[i].copyBytes(in.nextValue());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                final int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }

        @Override
        public int size() {
            return count;
        }

    }

    /**
     * 该对象与上面的区别就是 在获取数据时 会将byte[] 加工成string
     */
    public static final class Strings extends BinaryScriptDocValues<String> {

        public Strings(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public String get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            return values[index].get().utf8ToString();
        }

        public String getValue() {
            return get(0);
        }
    }

    public static final class BytesRefs extends BinaryScriptDocValues<BytesRef> {

        public BytesRefs(SortedBinaryDocValues in) {
            super(in);
        }

        @Override
        public BytesRef get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            /**
             * We need to make a copy here because {@link BinaryScriptDocValues} might reuse the
             * returned value and the same instance might be used to
             * return values from multiple documents.
             **/
            return values[index].toBytesRef();
        }

        public BytesRef getValue() {
            return get(0);
        }

    }
}
