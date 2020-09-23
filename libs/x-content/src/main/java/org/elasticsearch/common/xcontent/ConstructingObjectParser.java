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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Like {@link ObjectParser} but works with objects that have constructors whose arguments are mixed in with its other settings. Queries are
 * like this, for example <code>ids</code> requires <code>types</code> but always parses the <code>values</code> field on the same level. If
 * this doesn't sounds like what you want to parse have a look at
 * {@link ObjectParser#declareNamedObjects(BiConsumer, ObjectParser.NamedObjectParser, Consumer, ParseField)} which solves a slightly
 * different but similar sounding problem.
 * <p>
 * Anyway, {@linkplain ConstructingObjectParser} parses the fields in the order that they are in the XContent, collecting constructor
 * arguments and parsing and queueing normal fields until all constructor arguments are parsed. Then it builds the target object and replays
 * the queued fields. Any fields that come in after the last constructor arguments are parsed and immediately applied to the target object
 * just like {@linkplain ObjectParser}.
 * </p>
 * <p>
 * Declaring a {@linkplain ConstructingObjectParser} is intentionally quite similar to declaring an {@linkplain ObjectParser}. The only
 * differences being that constructor arguments are declared with the consumer returned by the static {@link #constructorArg()} method and
 * that {@linkplain ConstructingObjectParser}'s constructor takes a lambda that must build the target object from a list of constructor
 * arguments:
 * </p>
 * <pre>{@code
 *   private static final ConstructingObjectParser<Thing, SomeContext> PARSER = new ConstructingObjectParser<>("thing",
 *           a -> new Thing((String) a[0], (String) a[1], (Integer) a[2]));
 *   static {
 *       PARSER.declareString(constructorArg(), new ParseField("animal"));
 *       PARSER.declareString(constructorArg(), new ParseField("vegetable"));
 *       PARSER.declareInt(optionalConstructorArg(), new ParseField("mineral"));
 *       PARSER.declareInt(Thing::setFruit, new ParseField("fruit"));
 *       PARSER.declareInt(Thing::setBug, new ParseField("bug"));
 *   }
 * }</pre>
 * <p>
 * This does add some overhead compared to just using {@linkplain ObjectParser} directly. On a 2.2 GHz Intel Core i7 MacBook Air running on
 * battery power in a reasonably unscientific microbenchmark it is about 100 microseconds for a reasonably large object, less if the
 * constructor arguments are first. On this platform with the same microbenchmarks just creating the XContentParser is around 900
 * microseconds and using {#linkplain ObjectParser} directly adds another 300 or so microseconds. In the best case
 * {@linkplain ConstructingObjectParser} allocates two additional objects per parse compared to {#linkplain ObjectParser}. In the worst case
 * it allocates <code>3 + 2 * param_count</code> objects per parse. If this overhead is too much for you then feel free to have ObjectParser
 * parse a secondary object and have that one call the target object's constructor. That ought to be rare though.
 * </p>
 * <p>
 * Note: if optional constructor arguments aren't specified then the number of allocations is always the worst case.
 * </p>
 * 他跟 ObjectParser的不同点在于  ObjectParser在初始化时要传入一个获取实例对象的函数  比如 Index::new   而该对象无法直接生成实例 因为生成实例需要的参数可能也是从结构体数据中获取的
 */
public final class ConstructingObjectParser<Value, Context> extends AbstractObjectParser<Value, Context> implements
    BiFunction<XContentParser, Context, Value>, ContextParser<Context, Value>{

    /**
     * Consumer that marks a field as a required constructor argument instead of a real object field.
     */
    private static final BiConsumer<?, ?> REQUIRED_CONSTRUCTOR_ARG_MARKER = (a, b) -> {
        throw new UnsupportedOperationException("I am just a marker I should never be called.");
    };

    /**
     * Consumer that marks a field as an optional constructor argument instead of a real object field.
     */
    private static final BiConsumer<?, ?> OPTIONAL_CONSTRUCTOR_ARG_MARKER = (a, b) -> {
        throw new UnsupportedOperationException("I am just a marker I should never be called.");
    };

    /**
     * List of constructor names used for generating the error message if not all arrive.
     * 描述构建对象需要的构造函数参数
     */
    private final List<ConstructorArgInfo> constructorArgInfos = new ArrayList<>();

    /**
     * 该对象还内置了一个 ObjectParser
     */
    private final ObjectParser<Target, Context> objectParser;
    private final BiFunction<Object[], Context, Value> builder;
    /**
     * The number of fields on the targetObject. This doesn't include any constructor arguments and is the size used for the array backing
     * the field queue.
     * 记录在对应的 ObjectParser中已经定义了多少处理field的逻辑
     */
    private int numberOfFields = 0;

    /**
     * Build the parser.
     *
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param builder A function that builds the object from an array of Objects. Declare this inline with the parser, casting the elements
     *        of the array to the arguments so they work with your favorite constructor. The objects in the array will be in the same order
     *        that you declared the {@link #constructorArg()}s and none will be null. If any of the constructor arguments aren't defined in
     *        the XContent then parsing will throw an error. We use an array here rather than a {@code Map<String, Object>} to save on
     *        allocations.    builder 实际上就是构造函数 不过它需要一组参数
     */
    public ConstructingObjectParser(String name, Function<Object[], Value> builder) {
        this(name, false, builder);
    }

    /**
     * Build the parser.
     *
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param ignoreUnknownFields Should this parser ignore unknown fields? This should generally be set to true only when parsing responses
     *        from external systems, never when parsing requests from users.
     * @param builder A function that builds the object from an array of Objects. Declare this inline with the parser, casting the elements
     *        of the array to the arguments so they work with your favorite constructor. The objects in the array will be in the same order
     *        that you declared the {@link #constructorArg()}s and none will be null. If any of the constructor arguments aren't defined in
     *        the XContent then parsing will throw an error. We use an array here rather than a {@code Map<String, Object>} to save on
     *        allocations.
     *                在处理时 增加了一个context参数
     */
    public ConstructingObjectParser(String name, boolean ignoreUnknownFields, Function<Object[], Value> builder) {
        this(name, ignoreUnknownFields, (args, context) -> builder.apply(args));
    }

    /**
     * Build the parser.
     *
     * @param name The name given to the delegate ObjectParser for error identification. Use what you'd use if the object worked with
     *        ObjectParser.
     * @param ignoreUnknownFields Should this parser ignore unknown fields? This should generally be set to true only when parsing responses
     *        from external systems, never when parsing requests from users.
     * @param builder A binary function that builds the object from an array of Objects and the parser context.  Declare this inline with
     *        the parser, casting the elements of the array to the arguments so they work with your favorite constructor. The objects in
     *        the array will be in the same order that you declared the {@link #constructorArg()}s and none will be null. The second
     *        argument is the value of the context provided to the {@link #parse(XContentParser, Object) parse function}. If any of the
     *        constructor arguments aren't defined in the XContent then parsing will throw an error. We use an array here rather than a
     *        {@code Map<String, Object>} to save on allocations.
     */
    public ConstructingObjectParser(String name, boolean ignoreUnknownFields, BiFunction<Object[], Context, Value> builder) {
        // 因为实例的创建由该对象完成 所以ObjectParser 就不需要传入实例构造器了
        objectParser = new ObjectParser<>(name, ignoreUnknownFields, null);
        this.builder = builder;

    }

    /**
     * Call this to do the actual parsing. This implements {@link BiFunction} for conveniently integrating with ObjectParser.
     */
    @Override
    public Value apply(XContentParser parser, Context context) {
        try {
            return parse(parser, context);
        } catch (IOException e) {
            throw new XContentParseException(parser.getTokenLocation(), "[" + objectParser.getName()  + "] failed to parse object", e);
        }
    }

    /**
     * 这里指定了解析的对象为 Target 在该对象内部应该封装了 构建真正需要的实例 以及注入属性的逻辑
     * @param parser
     * @param context
     * @return
     * @throws IOException
     */
    @Override
    public Value parse(XContentParser parser, Context context) throws IOException {
        // 还需要将要使用的 fieldParser 提前设置到 ObjectParser中
        return objectParser.parse(parser, new Target(parser, context), context).finish();
    }

    /**
     * Pass the {@linkplain BiConsumer} this returns the declare methods to declare a required constructor argument. See this class's
     * javadoc for an example. The order in which these are declared matters: it is the order that they come in the array passed to
     * {@link #builder} and the order that missing arguments are reported to the user if any are missing. When all of these parameters are
     * parsed from the {@linkplain XContentParser} the target object is immediately built.
     */
    @SuppressWarnings("unchecked") // Safe because we never call the method. This is just trickery to make the interface pretty.
    public static <Value, FieldT> BiConsumer<Value, FieldT> constructorArg() {
        return (BiConsumer<Value, FieldT>) REQUIRED_CONSTRUCTOR_ARG_MARKER;
    }

    /**
     * Pass the {@linkplain BiConsumer} this returns the declare methods to declare an optional constructor argument. See this class's
     * javadoc for an example. The order in which these are declared matters: it is the order that they come in the array passed to
     * {@link #builder} and the order that missing arguments are reported to the user if any are missing. When all of these parameters are
     * parsed from the {@linkplain XContentParser} the target object is immediately built.
     */
    @SuppressWarnings("unchecked") // Safe because we never call the method. This is just trickery to make the interface pretty.
    public static <Value, FieldT> BiConsumer<Value, FieldT> optionalConstructorArg() {
        return (BiConsumer<Value, FieldT>) OPTIONAL_CONSTRUCTOR_ARG_MARKER;
    }

    /**
     * 定义处理某个field的逻辑
     * @param consumer 如何消费解析到的数值
     * @param parser 当解析到目标token时 如何获取数值的方法
     * @param parseField  声明本次目标token
     * @param type  表示当前指定的数值类型
     * @param <T>
     */
    @Override
    public <T> void declareField(BiConsumer<Value, T> consumer, ContextParser<Context, T> parser, ParseField parseField, ValueType type) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (parser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        if (parseField == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("[type] is required");
        }

        // 当传入的consumer是某种特殊的标识时 走这条逻辑
        if (isConstructorArg(consumer)) {
            /*
             * Build a new consumer directly against the object parser that
             * triggers the "constructor arg just arrived behavior" of the
             * parser. Conveniently, we can close over the position of the
             * constructor in the argument list so we don't need to do any fancy
             * or expensive lookups whenever the constructor args come in.
             */
            int position = addConstructorArg(consumer, parseField);
            // 定义将参数先存储到 target的构造参数中， 在满足条件时 会在Target中生成对象
            objectParser.declareField((target, v) -> target.constructorArg(position, v), parser, parseField, type);
        } else {
            // 其余情况按照 ObjectParser的逻辑进行处理
            numberOfFields += 1;
            // 这里指定 普通field信息是存储到 target的 queueField中
            objectParser.declareField(queueingConsumer(consumer, parseField), parser, parseField, type);
        }
    }

    @Override
    public <T> void declareNamedObject(BiConsumer<Value, T> consumer, NamedObjectParser<T, Context> namedObjectParser,
                                                ParseField parseField) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (namedObjectParser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        if (parseField == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }

        if (isConstructorArg(consumer)) {
            /*
             * Build a new consumer directly against the object parser that
             * triggers the "constructor arg just arrived behavior" of the
             * parser. Conveniently, we can close over the position of the
             * constructor in the argument list so we don't need to do any fancy
             * or expensive lookups whenever the constructor args come in.
             */
            int position = addConstructorArg(consumer, parseField);
            objectParser.declareNamedObject((target, v) -> target.constructorArg(position, v), namedObjectParser, parseField);
        } else {
            numberOfFields += 1;
            objectParser.declareNamedObject(queueingConsumer(consumer, parseField), namedObjectParser, parseField);
        }
    }

    @Override
    public <T> void declareNamedObjects(BiConsumer<Value, List<T>> consumer, NamedObjectParser<T, Context> namedObjectParser,
            ParseField parseField) {

        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (namedObjectParser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        if (parseField == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }

        if (isConstructorArg(consumer)) {
            /*
             * Build a new consumer directly against the object parser that
             * triggers the "constructor arg just arrived behavior" of the
             * parser. Conveniently, we can close over the position of the
             * constructor in the argument list so we don't need to do any fancy
             * or expensive lookups whenever the constructor args come in.
             */
            int position = addConstructorArg(consumer, parseField);
            objectParser.declareNamedObjects((target, v) -> target.constructorArg(position, v), namedObjectParser, parseField);
        } else {
            numberOfFields += 1;
            objectParser.declareNamedObjects(queueingConsumer(consumer, parseField), namedObjectParser, parseField);
        }
    }

    @Override
    public <T> void declareNamedObjects(BiConsumer<Value, List<T>> consumer, NamedObjectParser<T, Context> namedObjectParser,
            Consumer<Value> orderedModeCallback, ParseField parseField) {
        if (consumer == null) {
            throw new IllegalArgumentException("[consumer] is required");
        }
        if (namedObjectParser == null) {
            throw new IllegalArgumentException("[parser] is required");
        }
        if (orderedModeCallback == null) {
            throw new IllegalArgumentException("[orderedModeCallback] is required");
        }
        if (parseField == null) {
            throw new IllegalArgumentException("[parseField] is required");
        }

        if (isConstructorArg(consumer)) {
            /*
             * Build a new consumer directly against the object parser that
             * triggers the "constructor arg just arrived behavior" of the
             * parser. Conveniently, we can close over the position of the
             * constructor in the argument list so we don't need to do any fancy
             * or expensive lookups whenever the constructor args come in.
             */
            int position = addConstructorArg(consumer, parseField);
            objectParser.declareNamedObjects((target, v) -> target.constructorArg(position, v), namedObjectParser,
                    wrapOrderedModeCallBack(orderedModeCallback), parseField);
        } else {
            numberOfFields += 1;
            objectParser.declareNamedObjects(queueingConsumer(consumer, parseField), namedObjectParser,
                    wrapOrderedModeCallBack(orderedModeCallback), parseField);
        }
    }

    int getNumberOfFields() {
        return this.constructorArgInfos.size();
    }

    /**
     * Constructor arguments are detected by this "marker" consumer. It
     * keeps the API looking clean even if it is a bit sleezy.
     */
    private boolean isConstructorArg(BiConsumer<?, ?> consumer) {
        return consumer == REQUIRED_CONSTRUCTOR_ARG_MARKER || consumer == OPTIONAL_CONSTRUCTOR_ARG_MARKER;
    }

    /**
     * Add a constructor argument
     * @param consumer Either {@link #REQUIRED_CONSTRUCTOR_ARG_MARKER} or {@link #REQUIRED_CONSTRUCTOR_ARG_MARKER}
     * @param parseField Parse field
     * @return The argument position
     * 代表本次设置的是一个构造器参数
     */
    private int addConstructorArg(BiConsumer<?, ?> consumer, ParseField parseField) {
        int position = constructorArgInfos.size();
        boolean required = consumer == REQUIRED_CONSTRUCTOR_ARG_MARKER;
        constructorArgInfos.add(new ConstructorArgInfo(parseField, required));
        return position;
    }

    @Override
    public String getName() {
        return objectParser.getName();
    }

    @Override
    public void declareRequiredFieldSet(String... requiredSet) {
        objectParser.declareRequiredFieldSet(requiredSet);
    }

    @Override
    public void declareExclusiveFieldSet(String... exclusiveSet) {
        objectParser.declareExclusiveFieldSet(exclusiveSet);
    }

    private Consumer<Target> wrapOrderedModeCallBack(Consumer<Value> callback) {
        return (target) -> {
            if (target.targetObject != null) {
                // The target has already been built. Call the callback now.
                callback.accept(target.targetObject);
                return;
            }
            /*
             * The target hasn't been built. Queue the callback.
             */
            target.queuedOrderedModeCallback = callback;
        };
    }

    /**
     * Creates the consumer that does the "field just arrived" behavior. If the targetObject hasn't been built then it queues the value.
     * Otherwise it just applies the value just like {@linkplain ObjectParser} does.
     * @param consumer 原消费对象
     * @param parseField 代表本次注册的是哪个field
     *                   定义如何处理 普通属性的注入(非构造参数)
     */
    private <T> BiConsumer<Target, T> queueingConsumer(BiConsumer<Value, T> consumer, ParseField parseField) {
        return (target, v) -> {
            // 当targetObject 已经创建时 直接作用
            if (target.targetObject != null) {
                // The target has already been built. Just apply the consumer now.
                consumer.accept(target.targetObject, v);
                return;
            }
            /*
             * The target hasn't been built. Queue the consumer. The next two lines are the only allocations that ConstructingObjectParser
             * does during parsing other than the boxing the ObjectParser might do. The first one is to preserve a snapshot of the current
             * location so we can add it to the error message if parsing fails. The second one (the lambda) is the actual operation being
             * queued. Note that we don't do any of this if the target object has already been built.
             */
            XContentLocation location = target.parser.getTokenLocation();
            // 当对象还未创建时  将解析逻辑存储到 target中
            target.queue(targetObject -> {
                try {
                    consumer.accept(targetObject, v);
                } catch (Exception e) {
                    throw new XContentParseException(location,
                            "[" + objectParser.getName() + "] failed to parse field [" + parseField.getPreferredName() + "]", e);
                }
            });
        };
    }

    /**
     * The target of the {@linkplain ConstructingObjectParser}. One of these is built every time you call
     * {@linkplain ConstructingObjectParser#apply(XContentParser, Object)} Note that it is not static so it inherits
     * {@linkplain ConstructingObjectParser}'s type parameters.
     * 该对象封装了 解析field作为构造参数 构造对象后并继续解析结构化数据 并注入属性的逻辑
     */
    private class Target {
        /**
         * Array of constructor args to be passed to the {@link ConstructingObjectParser#builder}.
         * 构造实例需要的参数信息
         */
        private final Object[] constructorArgs = new Object[constructorArgInfos.size()];
        /**
         * The parser this class is working against. We store it here so we can fetch it conveniently when queueing fields to lookup the
         * location of each field so that we can give a useful error message when replaying the queue.
         * 该对象包含了解析结构化数据的api
         */
        private final XContentParser parser;

        /**
         * The parse context that is used for this invocation. Stored here so that it can be passed to the {@link #builder}.
         * 用户指定在解析过程中使用的上下文信息   只是为了增加拓展性 大多数情况下该属性为空
         */
        private Context context;

        /**
         * How many of the constructor parameters have we collected? We keep track of this so we don't have to count the
         * {@link #constructorArgs} array looking for nulls when we receive another constructor parameter. When this is equal to the size of
         * {@link #constructorArgs} we build the target object.
         * 此时已经获取了多少构造参数
         */
        private int constructorArgsCollected = 0;
        /**
         * Fields to be saved to the target object when we can build it. This is only allocated if a field has to be queued.
         */
        private Consumer<Value>[] queuedFields;
        /**
         * OrderedModeCallback to be called with the target object when we can
         * build it. This is only allocated if the callback has to be queued.
         * 当对象创建时 触发的回调函数
         */
        private Consumer<Value> queuedOrderedModeCallback;
        /**
         * The count of fields already queued.
         * 此时存储了多少用于填充生成后的数据的对象  因为属性可能先于构造参数被解析到 此时无法直接使用 所以需要先存起来
         */
        private int queuedFieldsCount = 0;
        /**
         * The target object. This will be instantiated with the constructor arguments are all parsed.
         * 当构造参数足够时 就会创建该对象
         */
        private Value targetObject;

        Target(XContentParser parser, Context context) {
            this.parser = parser;
            this.context = context;
        }

        /**
         * Set a constructor argument and build the target object if all constructor arguments have arrived.
         * 填充某个构造参数
         */
        private void constructorArg(int position, Object value) {
            constructorArgs[position] = value;
            constructorArgsCollected++;
            if (constructorArgsCollected == constructorArgInfos.size()) {
                buildTarget();
            }
        }

        /**
         * Queue a consumer that we'll call once the targetObject is built. If targetObject has been built this will fail because the caller
         * should have just applied the consumer immediately.
         * 此时解析出一个用于填充实例对象属性的field信息  选择存储起来
         */
        private void queue(Consumer<Value> queueMe) {
            assert targetObject == null: "Don't queue after the targetObject has been built! Just apply the consumer directly.";
            if (queuedFields == null) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                Consumer<Value>[] queuedFields = new Consumer[numberOfFields];
                this.queuedFields = queuedFields;
            }
            queuedFields[queuedFieldsCount] = queueMe;
            queuedFieldsCount++;
        }

        /**
         * Finish parsing the object.
         * 返回构造完并填充属性后的对象
         */
        private Value finish() {
            if (targetObject != null) {
                return targetObject;
            }
            /*
             * The object hasn't been built which ought to mean we're missing some constructor arguments. But they could be optional! We'll
             * check if they are all optional and build the error message at the same time - if we don't start the error message then they
             * were all optional!
             */
            StringBuilder message = null;
            for (int i = 0; i < constructorArgs.length; i++) {
                if (constructorArgs[i] != null) continue;
                ConstructorArgInfo arg = constructorArgInfos.get(i);
                if (false == arg.required) continue;
                if (message == null) {
                    message = new StringBuilder("Required [").append(arg.field);
                } else {
                    message.append(", ").append(arg.field);
                }
            }
            if (message != null) {
                // There were non-optional constructor arguments missing.
                throw new IllegalArgumentException(message.append(']').toString());
            }
            /*
             * If there weren't any constructor arguments declared at all then we won't get an error message but this isn't really a valid
             * use of ConstructingObjectParser. You should be using ObjectParser instead. Since this is more of a programmer error and the
             * parser ought to still work we just assert this.
             */
            assert false == constructorArgInfos.isEmpty() : "[" + objectParser.getName() + "] must configure at least one constructor "
                        + "argument. If it doesn't have any it should use ObjectParser instead of ConstructingObjectParser. This is a bug "
                        + "in the parser declaration.";
            // All missing constructor arguments were optional. Just build the target and return it.
            buildTarget();
            return targetObject;
        }

        /**
         * 当构造参数足够时 创建对象
         */
        private void buildTarget() {
            try {
                targetObject = builder.apply(constructorArgs, context);
                // 对象构造完成时  触发回调函数
                if (queuedOrderedModeCallback != null) {
                    queuedOrderedModeCallback.accept(targetObject);
                }
                // 此时已经读取到部分填充用field信息了 设置到实例对象上
                while (queuedFieldsCount > 0) {
                    queuedFieldsCount -= 1;
                    queuedFields[queuedFieldsCount].accept(targetObject);
                }
            } catch (XContentParseException e) {
                throw new XContentParseException(e.getLocation(),
                    "failed to build [" + objectParser.getName() + "] after last required field arrived", e);
            } catch (Exception e) {
                throw new XContentParseException(null,
                        "Failed to build [" + objectParser.getName() + "] after last required field arrived", e);
            }
        }
    }

    /**
     * 描述通过解析结构体内的数据并生成待处理对象时 有关构造函数参数的定义
     */
    private static class ConstructorArgInfo {
        /**
         * 某个构造参数是从哪个field上解析出来的
         */
        final ParseField field;
        final boolean required; // 代表该参数是否是必备的

        ConstructorArgInfo(ParseField field, boolean required) {
            this.field = field;
            this.required = required;
        }
    }
}
