#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ${package};

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class Order {
    @JsonProperty("rowNumber")
    private String rowNumber;
    @JsonProperty("orderKey")
    private long orderKey;
    @JsonProperty("customerKey")
    private long customerKey;
    @JsonProperty("orderStatus")
    private String orderStatus;
    @JsonProperty("totalPrice")
    private float totalPrice;
    @JsonProperty("orderDate")
    private String orderDate;
    @JsonProperty("orderPriority")
    private String orderPriority;
    @JsonProperty("clerk")
    private String clerk;
    @JsonProperty("shipPriority")
    private long shipPriority;
    @JsonProperty("comment")
    private String comment;
}

