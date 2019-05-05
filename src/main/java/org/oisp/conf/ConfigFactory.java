package org.oisp.conf;

import com.google.gson.Gson;
import java.util.Map;

/**
 * Copyright (c) 2016 Intel Corporation
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
public class ConfigFactory {

    public ConfigFactory() {

    }

    public Config getConfigFromArgs(String args) {
        Gson g = new Gson();
        try {
            Map<String, Object> hash = getConfig().getHash();
            hash = g.fromJson(args, hash.getClass());

            return getConfig().put(hash);
        } catch (IllegalStateException exception) {
            return null;
        }
    }

    public Config getConfig() {
        return new Config();
    }
}
