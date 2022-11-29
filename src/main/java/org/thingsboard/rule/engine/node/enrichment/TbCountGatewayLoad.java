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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.HashMultimap;
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
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j

@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "Count gateways load",
        configClazz = TbCountGatewayLoadConfiguration.class,
        nodeDescription = "Counts load for each gateway",
        nodeDetails = "v1.0.3",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "TbCountGatewayLoadConfiguration")

public class TbCountGatewayLoad implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private TbCountGatewayLoadConfiguration config;

    private DeviceService deviceService;
    private TimeseriesService timeseriesService;

    private Long time;
    private String deviceType;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbCountGatewayLoadConfiguration.class);
        time = TimeUnit.DAYS.toMillis(config.getTimeDays());
        deviceType = config.getDeviceType();

        deviceService = ctx.getDeviceService();
        timeseriesService = ctx.getTimeseriesService();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        List<Device> devices = getDevices(ctx, deviceType);
        log.info("Start calculating load for gateways");
        countGatewayLoad(ctx, devices);
        ctx.tellSuccess(msg);
    }

    @Override
    public void destroy() {

    }

    private List<Device> getDevices(TbContext ctx, String deviceType) {
        PageLink pageLink = new PageLink(1000);
        PageData<Device> devices;
        if (deviceType.equalsIgnoreCase("any")) {
            devices = deviceService.findDevicesByTenantId(ctx.getTenantId(), pageLink);
        }
        else {
            devices = deviceService.findDevicesByTenantIdAndType(ctx.getTenantId(), deviceType, pageLink);
        }
        return devices.getData();
    }

    private ListenableFuture<List<TsKvEntry>> getTelemetry(Device device, TenantId tenantId, Long startTs, Long endTs) {
        BaseReadTsKvQuery baseReadTsKvQuery = new BaseReadTsKvQuery("count", startTs, endTs, 0, Integer.MAX_VALUE, Aggregation.NONE);
        ListenableFuture<List<TsKvEntry>> all = timeseriesService.findAll(tenantId, device.getId(), Collections.singletonList(baseReadTsKvQuery));
        return all;
    }


    private ListenableFuture<Map<String, Device>> fetchGateways(TbContext ctx) {
        Map<String, Device> map = new ConcurrentHashMap<>();
        List<ListenableFuture<Boolean>> allFutures = new ArrayList<>();
        PageLink pageLink = new PageLink(1000);
        PageData<Device> gateways = deviceService.findDevicesByTenantIdAndType(ctx.getTenantId(), "Gateway", pageLink);
        log.info("{} gateways found", gateways.getData().size());
        for (Device gateway : gateways.getData()) {
            ListenableFuture<Optional<AttributeKvEntry>> future = ctx.getAttributesService().find(ctx.getTenantId(), gateway.getId(), DataConstants.SERVER_SCOPE, "lrrID");
            allFutures.add(Futures.transform(future, data -> {
                data.ifPresent(attributeKvEntry -> {
                    map.put(attributeKvEntry.getValueAsString(), gateway);
                    log.info("{} llrID is {}", gateway.getName(), attributeKvEntry.getValueAsString());
                });
                return true;
            }, ctx.getDbCallbackExecutor()));
        }
        log.info("LrrId found for {} gateways", map.size());
        return Futures.transform(Futures.allAsList(allFutures), r -> map, ctx.getDbCallbackExecutor());
    }

    private Collection<Collection<MyMessage>> partitionByDay(List<MyMessage> telemetry) {
        HashMultimap<String, MyMessage> multimap = HashMultimap.create();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        for (MyMessage myMessage : telemetry) {
            String date = sdf.format(myMessage.ts);
            multimap.put(date, myMessage);
        }
        Collection<Collection<MyMessage>> values = multimap.asMap().values();
        return values;
    }

    private ListenableFuture<List<MyMessage>> getRawTelemetry(TbContext ctx, Device device, TenantId tenantId, Long startTs, Long endTs) {
        BaseReadTsKvQuery baseReadTsKvQuery = new BaseReadTsKvQuery("raw", startTs, endTs, 0, Integer.MAX_VALUE, Aggregation.NONE);
        ListenableFuture<List<TsKvEntry>> all = timeseriesService.findAll(tenantId, device.getId(), Collections.singletonList(baseReadTsKvQuery));
        ListenableFuture<List<MyMessage>> future = Futures.transform(all, r -> {
//            return r.stream().map(e -> {
//                try {
//                    JsonNode jsonNode = mapper.readTree(e.getValueAsString());
//                    ArrayNode arrayNode = (ArrayNode) jsonNode.get("Lrrs").get("Lrr");
//                    for (JsonNode node : arrayNode) {
//                        return (new MyMessage(jsonNode.get("FCntUp").intValue(), node.get("LrrId").textValue(), e.getTs()));
//                    }
//                } catch (Exception ex) {
//                    log.error("Could not parse {}", e.getValueAsString(), ex);
//                    return null;
//                }
//                return null;
//            }).filter(Objects::nonNull).collect(Collectors.toList());
            ArrayList<MyMessage> messages = new ArrayList<>();
            r.forEach(a -> {
                try {
                    JsonNode jsonNode = mapper.readTree(a.getValueAsString());
                    if (jsonNode.has("Lrrs")) {
                        ArrayNode arrayNode = (ArrayNode) jsonNode.get("Lrrs").get("Lrr");
                        for (JsonNode node : arrayNode) {
                            messages.add(new MyMessage(jsonNode.get("FCntUp").intValue(), node.get("Lrrid").textValue(), a.getTs()));
                        }
                    }
                    else {
                        messages.add(new MyMessage(jsonNode.get("FCntUp").intValue(), jsonNode.get("Lrrid").textValue(), a.getTs()));
                    }
                } catch (Exception ex) {
                    log.error("Could not parse", ex);
                }
            });
            log.info("{} messages found", messages.size());
            return messages;
        }, ctx.getDbCallbackExecutor());

        return future;
    }

    private void countGatewayLoad(TbContext ctx, List<Device> devices) {
        long startTs = Instant.ofEpochMilli(System.currentTimeMillis() - time).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
        ListenableFuture<Map<String, Device>> gatewaysMap = fetchGateways(ctx);
        HashMultimap<Long, LoadEntry> hashMultimap = HashMultimap.create();
        for (Device device : devices) {
            ListenableFuture<List<MyMessage>> rawTelemetry = getRawTelemetry(ctx, device, ctx.getTenantId(), startTs, System.currentTimeMillis());
            Futures.transform(rawTelemetry, rawData -> {
                partitionByDay(rawData).forEach(partition -> {
                    List<String> uniqueLrrs = partition.stream().map(raw -> raw.lrrID).distinct().collect(Collectors.toList());
                    for (String uniqueLrr : uniqueLrrs) {
                        long load = partition.stream().filter(raw -> Objects.equals(raw.lrrID, uniqueLrr)).count();
                        long maxTs = partition.stream().mapToLong(m -> m.ts).max().orElse(Long.MIN_VALUE);
                        hashMultimap.put(maxTs, new LoadEntry(uniqueLrr, load, maxTs));
                    }
                });
                return true;
            }, ctx.getDbCallbackExecutor());
        }
        Collection<Collection<LoadEntry>> gatewayLoads = hashMultimap.asMap().values();
        Futures.transform(gatewaysMap, data -> {
            if (!data.isEmpty()) {
                for (Map.Entry<String, Device> stringDeviceEntry : data.entrySet()) {
                    gatewayLoads.forEach(gatewayLoad -> {
                        long loadSum = gatewayLoad.stream().filter(l -> Objects.equals(l.lrrId, stringDeviceEntry.getKey())).collect(Collectors.toList()).stream().mapToLong(m -> m.load).sum();
                        long maxTs = gatewayLoad.stream().mapToLong(m -> m.ts).max().orElse(Long.MIN_VALUE);
                        maxTs = Instant.ofEpochMilli(maxTs).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
                        BasicTsKvEntry result = new BasicTsKvEntry(maxTs, new LongDataEntry("load", loadSum));
                        log.info("Gateway {} load on {} is {} messages", stringDeviceEntry.getValue().getName(), maxTs, loadSum);
                        timeseriesService.save(ctx.getTenantId(), stringDeviceEntry.getValue().getId(), Collections.singletonList(result), TimeUnit.DAYS.toSeconds(3));
                    });

                }
            }
            return true;
        }, ctx.getDbCallbackExecutor());
    }

    class MyMessage {
        private int count;
        private String lrrID;
        private Long ts;

        public MyMessage(int count, String lrrID, Long ts) {
            this.count = count;
            this.lrrID = lrrID;
            this.ts = ts;
        }
    }

    class LoadEntry {
        private String lrrId;
        private long load;
        private long ts;

        public LoadEntry(String lrrId, long load, long ts) {
            this.lrrId = lrrId;
            this.load = load;
            this.ts = ts;
        }
    }
}
