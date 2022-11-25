/**
 * Copyright © 2018 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.rule.engine.node.enrichment;

import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

import java.util.concurrent.TimeUnit;

@Data
public class TbCountGatewayLoadConfiguration implements NodeConfiguration<TbCountGatewayLoadConfiguration> {

    private int timeDays;
    private String deviceType;
    @Override
    public TbCountGatewayLoadConfiguration defaultConfiguration() {
        TbCountGatewayLoadConfiguration configuration = new TbCountGatewayLoadConfiguration();
        configuration.setTimeDays(10);
        configuration.setDeviceType("");
        return configuration;
    }
}
