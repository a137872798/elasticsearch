/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.spi.Message;

/**
 * Handles {@link Binder#addError} commands.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 * 消息处理器  处理 addError命令
 */
class MessageProcessor extends AbstractProcessor {

    //private static final Logger logger = Logger.getLogger(Guice.class.getName());

    MessageProcessor(Errors errors) {
        super(errors);
    }

    /**
     * 每个处理器对象继承自 AbstractProcessor 并根据需要实现相关方法
     * 而该处理器仅仅是收集产生的各种异常信息
     * @param message
     * @return
     */
    @Override
    public Boolean visit(Message message) {
        // ES_GUICE: don't log failures using jdk logging
//        if (message.getCause() != null) {
//            String rootMessage = getRootMessage(message.getCause());
//            logger.log(Level.INFO,
//                    "An exception was caught and reported. Message: " + rootMessage,
//                    message.getCause());
//        }

        errors.addMessage(message);
        return true;
    }

    public static String getRootMessage(Throwable t) {
        Throwable cause = t.getCause();
        return cause == null ? t.toString() : getRootMessage(cause);
    }
}
