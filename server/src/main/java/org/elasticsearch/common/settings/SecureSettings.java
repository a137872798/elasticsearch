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

package org.elasticsearch.common.settings;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Set;

/**
 * An accessor for settings which are securely stored. See {@link SecureSetting}.
 * 某些配置是加密过的
 */
public interface SecureSettings extends Closeable {

    /** Returns true iff the settings are loaded and retrievable. */  // 当前配置是否已经加载
    boolean isLoaded();

    /** Returns the names of all secure settings available. */  // 获取所有配置项名
    Set<String> getSettingNames();

    /** Return a string setting. The {@link SecureString} should be closed once it is used. */  // 通过传入的配置项名 找到配置
    SecureString getString(String setting) throws GeneralSecurityException;

    /** Return a file setting. The {@link InputStream} should be closed once it is used. */
    InputStream getFile(String setting) throws GeneralSecurityException;

    /**
     * 将配置项以密文形式返回
     * @param setting
     * @return
     * @throws GeneralSecurityException
     */
    byte[] getSHA256Digest(String setting) throws GeneralSecurityException;

    @Override
    void close() throws IOException;
}
