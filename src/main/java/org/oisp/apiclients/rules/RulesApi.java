/*
 * Copyright (c) 2016 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.oisp.apiclients.rules;

import org.oisp.apiclients.InvalidDashboardResponseException;
import org.oisp.apiclients.rules.model.ComponentRulesResponse;

import java.util.List;
import java.util.Set;


public interface RulesApi {

    List<ComponentRulesResponse> getActiveComponentsRules(Boolean synced) throws InvalidDashboardResponseException;
    void markRulesSynchronized(Set<String> rulesIds) throws InvalidDashboardResponseException;
}
