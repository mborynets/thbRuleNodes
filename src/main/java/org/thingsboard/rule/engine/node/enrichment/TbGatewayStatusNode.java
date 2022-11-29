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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.dao.attributes.AttributesService;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j

@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "Process gateway status values",
        configClazz = TbGatewayStatusNodeConfiguration.class,
        nodeDescription = "Finds last active power off events",
        nodeDetails = "v1.0.0",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "TbGatewayStatusNodeConfiguration")

public class TbGatewayStatusNode implements TbNode {

    private TbGatewayStatusNodeConfiguration config;
    private DeviceService deviceService;
    private TimeseriesService timeseriesService;
    private AttributesService attributesService;

    private Long time;
    private Long daysThreshold;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbGatewayStatusNodeConfiguration.class);
        time = TimeUnit.DAYS.toMillis(config.getTimeDays());
        daysThreshold = TimeUnit.DAYS.toMillis(config.getDaysThreshold());

        deviceService = ctx.getDeviceService();
        timeseriesService = ctx.getTimeseriesService();
        attributesService = ctx.getAttributesService();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg tbMsg) throws ExecutionException, InterruptedException, TbNodeException {
        long startTs = Instant.ofEpochMilli(System.currentTimeMillis() - time).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
        List<Device> gateways = getGateways(ctx);
        log.info("{} gateways found", gateways.size());
        log.info("Starting processing gateways status events");
        for (Device gateway : gateways) {
            ListenableFuture<List<TsKvEntry>> telemetry = getPowerStatusTelemetry(gateway, ctx.getTenantId(), startTs, System.currentTimeMillis());
            Futures.transform(telemetry, data -> {
                if (!data.isEmpty()) {
                    findLastPowerOff(ctx, gateway, data);
                }
                return true;
            }, ctx.getDbCallbackExecutor());
        }
    }

    @Override
    public void destroy() {

    }

    private List<Device> getGateways(TbContext ctx) {
        PageLink pageLink = new PageLink(1000);
        PageData<Device> gateways;
        gateways = deviceService.findDevicesByTenantIdAndType(ctx.getTenantId(), "Gateway", pageLink);
        return gateways.getData();
    }

    private ListenableFuture<List<TsKvEntry>> getPowerStatusTelemetry(Device device, TenantId tenantId, Long startTs, Long endTs) {
        BaseReadTsKvQuery baseReadTsKvQuery = new BaseReadTsKvQuery("powerStatus", startTs, endTs, 0, Integer.MAX_VALUE, Aggregation.NONE);
        return timeseriesService.findAll(tenantId, device.getId(), Collections.singletonList(baseReadTsKvQuery));
    }

    private void findLastPowerOff(TbContext ctx, Device device, List<TsKvEntry> telemetry) {
        telemetry.sort(Comparator.comparingLong(TsKvEntry::getTs));
        long lastOff = 0;

        for (int i = 0; i < telemetry.size()-1; i++) {
            if (telemetry.get(i).getLongValue().orElse(Long.MIN_VALUE) == 0) {
                if (lastOff == 0) {
                    lastOff = telemetry.get(i).getTs();
                }
            }
            else {
                lastOff = 0;
            }
        }

        AttributeKvEntry lastOffEntry;
        if (lastOff != 0 && System.currentTimeMillis() - lastOff > daysThreshold) {
            log.info("{} last active power off event found since {}", device.getName(), new Date(lastOff));
            lastOffEntry = new BaseAttributeKvEntry(System.currentTimeMillis(), new LongDataEntry("activePowerOff", lastOff));
        }
        else {
            lastOffEntry = new BaseAttributeKvEntry(System.currentTimeMillis(), new LongDataEntry("activePowerOff", 0L));
        }

        attributesService.save(ctx.getTenantId(), device.getId(), DataConstants.SERVER_SCOPE, Collections.singletonList(lastOffEntry));
    }
}
