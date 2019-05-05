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

package org.oisp.collection;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;


public class RulesWithObservation implements Serializable {

    private final List<Rule> rules;
    private final Observation observation;

    public RulesWithObservation(Observation observation, List<Rule> rules) {
        this.observation = observation;
        this.rules = rules;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public Observation getObservation() {
        return observation;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final RulesWithObservation other = (RulesWithObservation) o;

        return Objects.equal(this.observation, other.observation) && Objects
                .equal(this.rules, other.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rules, observation);
    }
}
