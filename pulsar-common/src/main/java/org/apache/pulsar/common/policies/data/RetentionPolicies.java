/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.policies.data;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 */
@ApiModel(
        value = "RetentionPolicies",
        description = "Set the retention policy of backlog for a namespace"
)
public class RetentionPolicies {

    @ApiModelProperty(
            name = "retentionTimeInMinutes",
            value = "Retention time in minutes (or minutes, hours,days,weeks eg: 100m,"
                    + "3h, 2d, 5w). 0 means no retention and -1 means infinite time retention"
    )
    private int retentionTimeInMinutes;

    @ApiModelProperty(
            name = "retentionSizeInMB",
            value = "Retention size limit (eg: 10M, 16G, 3T). 0 or less than 1MB means "
                    + "no retention and -1 means infinite size retention"
    )
    private long retentionSizeInMB;

    public RetentionPolicies() {
        this(0, 0);
    }

    public RetentionPolicies(int retentionTimeInMinutes, int retentionSizeInMB) {
        this.retentionSizeInMB = retentionSizeInMB;
        this.retentionTimeInMinutes = retentionTimeInMinutes;
    }

    public int getRetentionTimeInMinutes() {
        return retentionTimeInMinutes;
    }

    public long getRetentionSizeInMB() {
        return retentionSizeInMB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RetentionPolicies that = (RetentionPolicies) o;

        if (retentionTimeInMinutes != that.retentionTimeInMinutes) {
            return false;
        }

        return retentionSizeInMB == that.retentionSizeInMB;
    }

    @Override
    public int hashCode() {
        long result = retentionTimeInMinutes;
        result = 31 * result + retentionSizeInMB;
        return Long.hashCode(result);
    }

    @Override
    public String toString() {
        return "RetentionPolicies{" + "retentionTimeInMinutes=" + retentionTimeInMinutes + ", retentionSizeInMB="
                + retentionSizeInMB + '}';
    }
}
