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

package org.elasticsearch.plugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.threadpool.ExecutorBuilder;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.io.FileSystemUtils.isAccessibleDirectory;

/**
 * 管理插件的对象
 * PluginsAndModules 可以将自身信息输出到一个 output对象中
 */
public class PluginsService implements ReportingService<PluginsAndModules> {

    private static final Logger logger = LogManager.getLogger(PluginsService.class);

    private final Settings settings;
    private final Path configPath;

    /**
     * We keep around a list of plugins and modules
     * 存储了实例化后的 plugin
     */
    private final List<Tuple<PluginInfo, Plugin>> plugins;

    /**
     * 维护了从 module ， plugin目录下加载的所有插件信息
     */
    private final PluginsAndModules info;

    /**
     * 代表必须设置某些插件才可以启动 找不到会抛出异常
     */
    public static final Setting<List<String>> MANDATORY_SETTING =
        Setting.listSetting("plugin.mandatory", Collections.emptyList(), Function.identity(), Property.NodeScope);

    public List<Setting<?>> getPluginSettings() {
        return plugins.stream().flatMap(p -> p.v2().getSettings().stream()).collect(Collectors.toList());
    }

    public List<String> getPluginSettingsFilter() {
        return plugins.stream().flatMap(p -> p.v2().getSettingsFilter().stream()).collect(Collectors.toList());
    }

    /**
     * Constructs a new PluginService
     * @param settings The settings of the system   当前已经加载的配置
     * @param modulesDirectory The directory modules exist in, or null if modules should not be loaded from the filesystem   模块目录
     * @param pluginsDirectory The directory plugins exist in, or null if plugins should not be loaded from the filesystem   插件目录
     * @param classpathPlugins Plugins that exist in the classpath which should be loaded              代表某些存在于  classpath的插件
     */
    public PluginsService(
        Settings settings,
        Path configPath,
        Path modulesDirectory,
        Path pluginsDirectory,
        Collection<Class<? extends Plugin>> classpathPlugins
    ) {
        this.settings = settings;
        this.configPath = configPath;

        List<Tuple<PluginInfo, Plugin>> pluginsLoaded = new ArrayList<>();
        List<PluginInfo> pluginsList = new ArrayList<>();
        // we need to build a List of plugins for checking mandatory plugins
        final List<String> pluginsNames = new ArrayList<>();
        // first we load plugins that are on the classpath. this is for tests
        // TODO 先忽略这种情况 一般classpathPlugins为空
        for (Class<? extends Plugin> pluginClass : classpathPlugins) {
            Plugin plugin = loadPlugin(pluginClass, settings, configPath);
            PluginInfo pluginInfo = new PluginInfo(pluginClass.getName(), "classpath plugin", "NA", Version.CURRENT, "1.8",
                                                   pluginClass.getName(), Collections.emptyList(), false);
            if (logger.isTraceEnabled()) {
                logger.trace("plugin loaded from classpath [{}]", pluginInfo);
            }
            pluginsLoaded.add(new Tuple<>(pluginInfo, plugin));
            pluginsList.add(pluginInfo);
            pluginsNames.add(pluginInfo.getName());
        }

        Set<Bundle> seenBundles = new LinkedHashSet<>();
        List<PluginInfo> modulesList = new ArrayList<>();
        // load modules
        // 将.module 目录下 所有module目录 下对应的jar包包装成 bundle对象 并转入到modules中
        if (modulesDirectory != null) {
            try {
                Set<Bundle> modules = getModuleBundles(modulesDirectory);
                for (Bundle bundle : modules) {
                    // 插件描述信息要存储到list中
                    modulesList.add(bundle.plugin);
                }
                seenBundles.addAll(modules);
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize modules", ex);
            }
        }

        // now, find all the ones that are in plugins/
        // 这里处理  plugins下所有插件目录下的数据
        if (pluginsDirectory != null) {
            try {
                // TODO: remove this leniency, but tests bogusly rely on it
                // 检验目录是否可用
                if (isAccessibleDirectory(pluginsDirectory, logger)) {
                    // 如果有文件名包含 -removing  抛出异常
                    checkForFailedPluginRemovals(pluginsDirectory);
                    // 加载.plugin 下所有的文件
                    Set<Bundle> plugins = getPluginBundles(pluginsDirectory);
                    for (final Bundle bundle : plugins) {
                        pluginsList.add(bundle.plugin);
                        pluginsNames.add(bundle.plugin.getName());
                    }
                    seenBundles.addAll(plugins);
                }
            } catch (IOException ex) {
                throw new IllegalStateException("Unable to initialize plugins", ex);
            }
        }

        // 将所有插件实例化
        List<Tuple<PluginInfo, Plugin>> loaded = loadBundles(seenBundles);
        pluginsLoaded.addAll(loaded);

        // 存储插件描述信息的对象
        this.info = new PluginsAndModules(pluginsList, modulesList);
        this.plugins = Collections.unmodifiableList(pluginsLoaded);

        // Checking expected plugins
        // 检测强制性插件是否存在  如果没有这些插件 ES 将无法启动
        List<String> mandatoryPlugins = MANDATORY_SETTING.get(settings);
        if (mandatoryPlugins.isEmpty() == false) {
            Set<String> missingPlugins = new HashSet<>();
            for (String mandatoryPlugin : mandatoryPlugins) {
                if (!pluginsNames.contains(mandatoryPlugin) && !missingPlugins.contains(mandatoryPlugin)) {
                    missingPlugins.add(mandatoryPlugin);
                }
            }
            if (!missingPlugins.isEmpty()) {
                final String message = String.format(
                        Locale.ROOT,
                        "missing mandatory plugins [%s], found plugins [%s]",
                        Strings.collectionToDelimitedString(missingPlugins, ", "),
                        Strings.collectionToDelimitedString(pluginsNames, ", "));
                throw new IllegalStateException(message);
            }
        }

        // we don't log jars in lib/ we really shouldn't log modules,
        // but for now: just be transparent so we can debug any potential issues
        logPluginInfo(info.getModuleInfos(), "module", logger);
        logPluginInfo(info.getPluginInfos(), "plugin", logger);
    }

    private static void logPluginInfo(final List<PluginInfo> pluginInfos, final String type, final Logger logger) {
        assert pluginInfos != null;
        if (pluginInfos.isEmpty()) {
            logger.info("no " + type + "s loaded");
        } else {
            for (final String name : pluginInfos.stream().map(PluginInfo::getName).sorted().collect(Collectors.toList())) {
                logger.info("loaded " + type + " [" + name + "]");
            }
        }
    }

    /**
     * 从插件中获取配置信息 并更新初始化该对象时使用的 settings对象
     * @return
     */
    public Settings updatedSettings() {
        Map<String, String> foundSettings = new HashMap<>();
        final Settings.Builder builder = Settings.builder();
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            Settings settings = plugin.v2().additionalSettings();
            for (String setting : settings.keySet()) {
                // 代表冲突了
                String oldPlugin = foundSettings.put(setting, plugin.v1().getName());
                if (oldPlugin != null) {
                    throw new IllegalArgumentException("Cannot have additional setting [" + setting + "] " +
                        "in plugin [" + plugin.v1().getName() + "], already added in plugin [" + oldPlugin + "]");
                }
            }
            builder.put(settings);
        }
        return builder.put(this.settings).build();
    }

    /**
     * 将每个插件依赖的线程池builder对象返回 这些插件可能就运行在专门的线程池中
     * @param settings
     * @return
     */
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final ArrayList<ExecutorBuilder<?>> builders = new ArrayList<>();
        for (final Tuple<PluginInfo, Plugin> plugin : plugins) {
            builders.addAll(plugin.v2().getExecutorBuilders(settings));
        }
        return builders;
    }

    /**
     * TODO 先忽略该方法的作用
     * 指定所有插件此时的 索引模块
     * @param indexModule
     */
    public void onIndexModule(IndexModule indexModule) {
        for (Tuple<PluginInfo, Plugin> plugin : plugins) {
            plugin.v2().onIndexModule(indexModule);
        }
    }

    /**
     * Get information about plugins and modules
     */
    @Override
    public PluginsAndModules info() {
        return info;
    }

    /**
     * a "bundle" is a group of jars in a single classloader
     * 描述一个插件的信息 以及下面所有jar包   有一类插件在启动时 必须先运行exe文件 而这类插件仅仅以jar包形式使用
     */
    static class Bundle {
        final PluginInfo plugin;
        /**
         * 存储插件目录下所有的 jar包路径
         */
        final Set<URL> urls;

        /**
         *
         * @param plugin   描述插件的信息对象
         * @param dir   插件所在目录
         * @throws IOException
         */
        Bundle(PluginInfo plugin, Path dir) throws IOException {
            this.plugin = Objects.requireNonNull(plugin);
            Set<URL> urls = new LinkedHashSet<>();
            // gather urls for jar files
            try (DirectoryStream<Path> jarStream = Files.newDirectoryStream(dir, "*.jar")) {
                for (Path jar : jarStream) {
                    // normalize with toRealPath to get symlinks out of our hair
                    URL url = jar.toRealPath().toUri().toURL();
                    if (urls.add(url) == false) {
                        throw new IllegalStateException("duplicate codebase: " + url);
                    }
                }
            }
            this.urls = Objects.requireNonNull(urls);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bundle bundle = (Bundle) o;
            return Objects.equals(plugin, bundle.plugin);
        }

        @Override
        public int hashCode() {
            return Objects.hash(plugin);
        }
    }

    /**
     * Extracts all installed plugin directories from the provided {@code rootPath}.
     *
     * @param rootPath the path where the plugins are installed
     * @return a list of all plugin paths installed in the {@code rootPath}
     * @throws IOException if an I/O exception occurred reading the directories
     * 从插件根目录下找到所有插件目录   插件目录可能是按照插件名称命名的
     */
    public static List<Path> findPluginDirs(final Path rootPath) throws IOException {
        final List<Path> plugins = new ArrayList<>();
        // 避免重复
        final Set<String> seen = new HashSet<>();
        if (Files.exists(rootPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
                for (Path plugin : stream) {
                    // 忽略某些文件
                    if (FileSystemUtils.isDesktopServicesStore(plugin) ||
                        plugin.getFileName().toString().startsWith(".removing-")) {
                        continue;
                    }
                    if (seen.add(plugin.getFileName().toString()) == false) {
                        throw new IllegalStateException("duplicate plugin: " + plugin);
                    }
                    plugins.add(plugin);
                }
            }
        }
        return plugins;
    }

    /**
     * Verify the given plugin is compatible with the current Elasticsearch installation.
     */
    static void verifyCompatibility(PluginInfo info) {
        if (info.getElasticsearchVersion().equals(Version.CURRENT) == false) {
            throw new IllegalArgumentException("Plugin [" + info.getName() + "] was built for Elasticsearch version "
                + info.getElasticsearchVersion() + " but version " + Version.CURRENT + " is running");
        }
        JarHell.checkJavaVersion(info.getName(), info.getJavaVersion());
    }

    /**
     * 如果检测到插件目录下有包含 removing- 的目录 抛出异常
     * @param pluginsDirectory
     * @throws IOException
     */
    static void checkForFailedPluginRemovals(final Path pluginsDirectory) throws IOException {
        /*
         * Check for the existence of a marker file that indicates any plugins are in a garbage state from a failed attempt to remove the
         * plugin.
         */
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginsDirectory, ".removing-*")) {
            final Iterator<Path> iterator = stream.iterator();
            if (iterator.hasNext()) {
                final Path removing = iterator.next();
                final String fileName = removing.getFileName().toString();
                final String name = fileName.substring(1 + fileName.indexOf("-"));
                final String message = String.format(
                        Locale.ROOT,
                        "found file [%s] from a failed attempt to remove the plugin [%s]; execute [elasticsearch-plugin remove %2$s]",
                        removing,
                        name);
                throw new IllegalStateException(message);
            }
        }
    }

    /** Get bundles for plugins installed in the given modules directory. */
    static Set<Bundle> getModuleBundles(Path modulesDirectory) throws IOException {
        return findBundles(modulesDirectory, "module");
    }

    /** Get bundles for plugins installed in the given plugins directory. */
    static Set<Bundle> getPluginBundles(final Path pluginsDirectory) throws IOException {
        return findBundles(pluginsDirectory, "plugin");
    }

    /**
     * searches subdirectories under the given directory for plugin directories
     * @param directory 处理的目标目录
     * @param type 本次加载的类型  比如 module, plugin
     */
    private static Set<Bundle> findBundles(final Path directory, String type) throws IOException {
        final Set<Bundle> bundles = new HashSet<>();
        // findPluginDirs 读取.module/.plugin 目录下所有的目录
        for (final Path plugin : findPluginDirs(directory)) {
            // 读取每个插件目录下信息并生成bundle对象
            final Bundle bundle = readPluginBundle(bundles, plugin, type);
            bundles.add(bundle);
        }

        return bundles;
    }

    /**
     * get a bundle for a single plugin dir
     * @param bundles  主要是用来检测是否冲突的
     * @param plugin  某种插件的路径  该路径在总插件路径下
     * @param type  代表module/plugin
     * @return
     * @throws IOException
     */
    private static Bundle readPluginBundle(final Set<Bundle> bundles, final Path plugin, String type) throws IOException {
        LogManager.getLogger(PluginsService.class).trace("--- adding [{}] [{}]", type, plugin.toAbsolutePath());
        final PluginInfo info;
        try {
            // 将目录下的 plugin-descriptor.properties 信息抽取出来 生成pluginInfo
            info = PluginInfo.readFromProperties(plugin);
        } catch (final IOException e) {
            throw new IllegalStateException("Could not load plugin descriptor for " + type +
                                            " directory [" + plugin.getFileName() + "]", e);
        }
        // 将某个插件的目录 与 描述该插件的信息对象合并成一个 bundle 对象
        final Bundle bundle = new Bundle(info, plugin);
        if (bundles.add(bundle) == false) {
            throw new IllegalStateException("duplicate " + type + ": " + info);
        }
        return bundle;
    }

    /**
     * Return the given bundles, sorted in dependency loading order.
     *
     * This sort is stable, so that if two plugins do not have any interdependency,
     * their relative order from iteration of the provided set will not change.
     *
     * @throws IllegalStateException if a dependency cycle is found
     * 因为某些插件可能依赖于其他插件 所以插件的加载有一个顺序
     */
    static List<Bundle> sortBundles(Set<Bundle> bundles) {
        Map<String, Bundle> namedBundles = bundles.stream().collect(Collectors.toMap(b -> b.plugin.getName(), Function.identity()));
        LinkedHashSet<Bundle> sortedBundles = new LinkedHashSet<>();
        LinkedHashSet<String> dependencyStack = new LinkedHashSet<>();
        for (Bundle bundle : bundles) {
            addSortedBundle(bundle, namedBundles, sortedBundles, dependencyStack);
        }
        return new ArrayList<>(sortedBundles);
    }

    /**
     * add the given bundle to the sorted bundles, first adding dependencies
     * @param bundle 当前正在处理的bundle
     * @param bundles  存放所有待处理的 bundle   key是插件的名称 value是描述插件信息 以及jar包位置的对象
     * @param sortedBundles
     * @param dependencyStack
     */
    private static void addSortedBundle(Bundle bundle, Map<String, Bundle> bundles, LinkedHashSet<Bundle> sortedBundles,
                                        LinkedHashSet<String> dependencyStack) {

        String name = bundle.plugin.getName();
        // 循环依赖
        if (dependencyStack.contains(name)) {
            StringBuilder msg = new StringBuilder("Cycle found in plugin dependencies: ");
            dependencyStack.forEach(s -> {
                msg.append(s);
                msg.append(" -> ");
            });
            msg.append(name);
            throw new IllegalStateException(msg.toString());
        }
        // 代表当前插件已经完成排序了
        if (sortedBundles.contains(bundle)) {
            // already added this plugin, via a dependency
            return;
        }

        // 加入到用于检测循环依赖的容器
        dependencyStack.add(name);
        // 遍历所有依赖的插件    每个插件可能会依托于其他插件才能使用
        for (String dependency : bundle.plugin.getExtendedPlugins()) {
            // 确保这个插件确实存在
            Bundle depBundle = bundles.get(dependency);
            if (depBundle == null) {
                throw new IllegalArgumentException("Missing plugin [" + dependency + "], dependency of [" + name + "]");
            }
            // 尝试进行递归插入
            addSortedBundle(depBundle, bundles, sortedBundles, dependencyStack);
            assert sortedBundles.contains(depBundle);
        }
        dependencyStack.remove(name);

        sortedBundles.add(bundle);
    }

    /**
     * 加载所有插件对象
     * @param bundles   从 module/plugin 下抽取出来的插件描述信息
     * @return
     */
    private List<Tuple<PluginInfo,Plugin>> loadBundles(Set<Bundle> bundles) {
        List<Tuple<PluginInfo, Plugin>> plugins = new ArrayList<>();
        Map<String, Plugin> loaded = new HashMap<>();
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        // 因为某些插件可能依赖于其他插件 所以这里按照将被依赖的bundle放到 前面
        List<Bundle> sortedBundles = sortBundles(bundles);

        for (Bundle bundle : sortedBundles) {
            // TODO 检验性代码先忽略
            checkBundleJarHell(JarHell.parseClassPath(), bundle, transitiveUrls);

            // 找到插件对应的class 并进行实例化  因为插件类必须满足是Plugin的子类 所以可以以plugin的形式返回
            final Plugin plugin = loadBundle(bundle, loaded);
            // 将实例化后的插件对象 与插件的描述信息包装成一个对象后设置到列表中
            plugins.add(new Tuple<>(bundle.plugin, plugin));
        }

        return Collections.unmodifiableList(plugins);
    }

    // jar-hell check the bundle against the parent classloader and extended plugins
    // the plugin cli does it, but we do it again, in case lusers mess with jar files manually
    static void checkBundleJarHell(Set<URL> classpath, Bundle bundle, Map<String, Set<URL>> transitiveUrls) {
        // invariant: any plugins this plugin bundle extends have already been added to transitiveUrls
        List<String> exts = bundle.plugin.getExtendedPlugins();

        try {
            final Logger logger = LogManager.getLogger(JarHell.class);
            Set<URL> urls = new HashSet<>();
            for (String extendedPlugin : exts) {
                Set<URL> pluginUrls = transitiveUrls.get(extendedPlugin);
                assert pluginUrls != null : "transitive urls should have already been set for " + extendedPlugin;

                Set<URL> intersection = new HashSet<>(urls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException("jar hell! extended plugins " + exts +
                                                    " have duplicate codebases with each other: " + intersection);
                }

                intersection = new HashSet<>(bundle.urls);
                intersection.retainAll(pluginUrls);
                if (intersection.isEmpty() == false) {
                    throw new IllegalStateException("jar hell! duplicate codebases with extended plugin [" +
                                                    extendedPlugin + "]: " + intersection);
                }

                urls.addAll(pluginUrls);
                JarHell.checkJarHell(urls, logger::debug); // check jarhell as we add each extended plugin's urls
            }

            urls.addAll(bundle.urls);
            JarHell.checkJarHell(urls, logger::debug); // check jarhell of each extended plugin against this plugin
            transitiveUrls.put(bundle.plugin.getName(), urls);

            // check we don't have conflicting codebases with core
            Set<URL> intersection = new HashSet<>(classpath);
            intersection.retainAll(bundle.urls);
            if (intersection.isEmpty() == false) {
                throw new IllegalStateException("jar hell! duplicate codebases between plugin and core: " + intersection);
            }
            // check we don't have conflicting classes
            Set<URL> union = new HashSet<>(classpath);
            union.addAll(bundle.urls);
            JarHell.checkJarHell(union, logger::debug);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load plugin " + bundle.plugin.getName() + " due to jar hell", e);
        }
    }

    /**
     * 加载插件
     * @param bundle
     * @param loaded 存储已经加载完的插件
     * @return
     */
    private Plugin loadBundle(Bundle bundle, Map<String, Plugin> loaded) {
        String name = bundle.plugin.getName();

        // 校验兼容性 先忽略
        verifyCompatibility(bundle.plugin);

        // collect loaders of extended plugins
        // 存储当前待加载插件依赖的其他所有插件对应的类加载器
        List<ClassLoader> extendedLoaders = new ArrayList<>();
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            // 因为当前插件依赖于其他插件 所以先获取其他插件
            Plugin extendedPlugin = loaded.get(extendedPluginName);
            assert extendedPlugin != null;
            // 作为被依赖的插件 必须实现 ExtensiblePlugin 接口
            if (ExtensiblePlugin.class.isInstance(extendedPlugin) == false) {
                throw new IllegalStateException("Plugin [" + name + "] cannot extend non-extensible plugin [" + extendedPluginName + "]");
            }
            // 获取依赖插件的类加载器
            extendedLoaders.add(extendedPlugin.getClass().getClassLoader());
        }

        // 每个插件都定义了唯一的路径 在该路径下有自己的 SPI拓展类

        // create a child to load the plugin in this bundle
        // 将当前类加载器 以及依赖的所有插件类的 类加载器合并成一个 类加载器对象
        ClassLoader parentLoader = PluginLoaderIndirection.createLoader(getClass().getClassLoader(), extendedLoaders);
        // URLClassLoader 支持从一个额外的范围查找class
        // 这里是尝试加载插件目录下的jar包
        ClassLoader loader = URLClassLoader.newInstance(bundle.urls.toArray(new URL[0]), parentLoader);

        // reload SPI with any new services from the plugin
        // 加载插件下自定义的分词器  token过滤器 等等
        reloadLuceneSPI(loader);

        // TODO 这里是做什么
        for (String extendedPluginName : bundle.plugin.getExtendedPlugins()) {
            // note: already asserted above that extended plugins are loaded and extensible
            ExtensiblePlugin.class.cast(loaded.get(extendedPluginName)).reloadSPI(loader);
        }

        // 获取 Plugin的class对象  要求插件类都必须是 Plugin的子类
        Class<? extends Plugin> pluginClass = loadPluginClass(bundle.plugin.getClassname(), loader);
        // 使用相关参数对 plugin进行实例化
        Plugin plugin = loadPlugin(pluginClass, settings, configPath);
        // 将结果存储到容器中
        loaded.put(name, plugin);
        return plugin;
    }

    /**
     * Reloads all Lucene SPI implementations using the new classloader.
     * This method must be called after the new classloader has been created to
     * register the services for use.
     */
    static void reloadLuceneSPI(ClassLoader loader) {
        // do NOT change the order of these method calls!

        // 在lucene中 以下2种可以是以field为单位写入的  同时用户可以自行实现存储格式

        // Codecs:
        // 使用传入的类加载器加载所有 PostingsFormat 的实现类  (基于SPI机制)
        PostingsFormat.reloadPostingsFormats(loader);
        // 同上
        DocValuesFormat.reloadDocValuesFormats(loader);

        // 下面这些跟语言规范有关 就不细看了

        // Codec 类定义了存储所有数据使用的格式 通过修改该类就可以将写入工作转交给用户自定义的format对象了
        Codec.reloadCodecs(loader);
        // Analysis:  这个应该是针对char级别进行过滤
        CharFilterFactory.reloadCharFilters(loader);
        // 对解析出来的token进行过滤 过滤的单位更细 比如 stopFilter 就是一种tokenFilter  它的作用是对term起到过滤的作用
        TokenFilterFactory.reloadTokenFilters(loader);
        // 该对象加载自定义分词器
        TokenizerFactory.reloadTokenizers(loader);
    }

    private Class<? extends Plugin> loadPluginClass(String className, ClassLoader loader) {
        try {
            return loader.loadClass(className).asSubclass(Plugin.class);
        } catch (ClassNotFoundException e) {
            throw new ElasticsearchException("Could not find plugin class [" + className + "]", e);
        }
    }

    /**
     * 使用配置信息实例化 plugin 对象
     * @param pluginClass
     * @param settings
     * @param configPath
     * @return
     */
    private Plugin loadPlugin(Class<? extends Plugin> pluginClass, Settings settings, Path configPath) {
        final Constructor<?>[] constructors = pluginClass.getConstructors();
        if (constructors.length == 0) {
            throw new IllegalStateException("no public constructor for [" + pluginClass.getName() + "]");
        }

        if (constructors.length > 1) {
            throw new IllegalStateException("no unique public constructor for [" + pluginClass.getName() + "]");
        }

        final Constructor<?> constructor = constructors[0];
        if (constructor.getParameterCount() > 2) {
            throw new IllegalStateException(signatureMessage(pluginClass));
        }

        final Class<?>[] parameterTypes = constructor.getParameterTypes();
        try {
            if (constructor.getParameterCount() == 2 && parameterTypes[0] == Settings.class && parameterTypes[1] == Path.class) {
                return (Plugin)constructor.newInstance(settings, configPath);
            } else if (constructor.getParameterCount() == 1 && parameterTypes[0] == Settings.class) {
                return (Plugin)constructor.newInstance(settings);
            } else if (constructor.getParameterCount() == 0) {
                return (Plugin)constructor.newInstance();
            } else {
                throw new IllegalStateException(signatureMessage(pluginClass));
            }
        } catch (final ReflectiveOperationException e) {
            throw new IllegalStateException("failed to load plugin class [" + pluginClass.getName() + "]", e);
        }
    }

    private String signatureMessage(final Class<? extends Plugin> clazz) {
        return String.format(
                Locale.ROOT,
                "no public constructor of correct signature for [%s]; must be [%s], [%s], or [%s]",
                clazz.getName(),
                "(org.elasticsearch.common.settings.Settings,java.nio.file.Path)",
                "(org.elasticsearch.common.settings.Settings)",
                "()");
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> filterPlugins(Class<T> type) {
        return plugins.stream().filter(x -> type.isAssignableFrom(x.v2().getClass()))
            .map(p -> ((T)p.v2())).collect(Collectors.toList());
    }
}
