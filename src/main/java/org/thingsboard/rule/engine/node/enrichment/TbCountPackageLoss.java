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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.DataConstants;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.Tenant;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.kv.*;
import org.thingsboard.server.common.data.page.PageData;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.dao.device.DeviceService;
import org.thingsboard.server.dao.timeseries.TimeseriesService;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j

@RuleNode(
        type = ComponentType.ENRICHMENT,
        name = "Count package loss",
        configClazz = TbCountPackageLossConfiguration.class,
        nodeDescription = "Counts package loss of all the LoRa devices",
        nodeDetails = "v4.1",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "TbCountPackageLossConfiguration")

public class TbCountPackageLoss implements TbNode {

    private static final ObjectMapper mapper = new ObjectMapper();

    private TbCountPackageLossConfiguration config;

    private DeviceService deviceService;
    private TimeseriesService timeseriesService;

    private Long time;
    private String deviceType;

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        this.config = TbNodeUtils.convert(configuration, TbCountPackageLossConfiguration.class);
        time = TimeUnit.DAYS.toMillis(config.getTimeDays());
        deviceType = config.getDeviceType();

        deviceService = ctx.getDeviceService();
        timeseriesService = ctx.getTimeseriesService();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        List<Device> devices = getDevices(ctx, deviceType);
        log.info("Start calculating loss for {} devices", devices.size());
        for (Device device : devices) {
            log.info("Processing device {}", device.getName());
            processDevice(ctx, device);
        }
        ctx.tellSuccess(msg);
    }

    @Override
    public void destroy() {

    }

    private List<Device> getDevices(TbContext ctx, String deviceType) {
        PageLink pageLink = new PageLink(1000);
        PageData<Device> pulse_sensor;
        if (deviceType.equalsIgnoreCase("any")) {
            pulse_sensor = deviceService.findDevicesByTenantId(ctx.getTenantId(), pageLink);
        }
        else {
            pulse_sensor = deviceService.findDevicesByTenantIdAndType(ctx.getTenantId(), deviceType, pageLink);
        }
        return pulse_sensor.getData();
    }

    private ListenableFuture<List<TsKvEntry>> getCounterTelemetry(Device device, TenantId tenantId, Long startTs, Long endTs) {
        BaseReadTsKvQuery baseReadTsKvQuery = new BaseReadTsKvQuery("count", startTs, endTs, 0, Integer.MAX_VALUE, Aggregation.NONE);
        return timeseriesService.findAll(tenantId, device.getId(), Collections.singletonList(baseReadTsKvQuery));
    }

    private ListenableFuture<List<TsKvEntry>> getPayloadTelemetry(Device device, TenantId tenantId, Long startTs, Long endTs) {
        BaseReadTsKvQuery baseReadTsKvQuery = new BaseReadTsKvQuery("payload_hex", startTs, endTs, 0, Integer.MAX_VALUE, Aggregation.NONE);
        return timeseriesService.findAll(tenantId, device.getId(), Collections.singletonList(baseReadTsKvQuery));
    }

    private List<TsKvEntry> calculateLossPercent(Collection<TsKvEntry> telemetry) {
//        long max = telemetry.stream().mapToLong(t -> t.getLongValue().orElse(Long.MIN_VALUE)).max().orElse(Long.MIN_VALUE);
//        long min = telemetry.stream().mapToLong(t -> t.getLongValue().orElse(Long.MIN_VALUE)).min().orElse(Long.MIN_VALUE);
//        long uniq = telemetry.stream().mapToLong(t -> t.getLongValue().orElse(Long.MIN_VALUE)).distinct().count();
        long maxTs = telemetry.stream().mapToLong(TsKvEntry::getTs).max().orElse(Long.MIN_VALUE);
        maxTs = Instant.ofEpochMilli(maxTs).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
        List<TsKvEntry> result = new ArrayList<>();
        Iterator<TsKvEntry> iterator = telemetry.iterator();
//        TsKvEntry prev = null;
        long lossSum = 0;
        telemetry = telemetry.stream().sorted(Comparator.comparingLong(TsKvEntry::getTs)).collect(Collectors.toList());
        List<TsKvEntry> telemetryList = new ArrayList<>(telemetry);
        for (int i = 1; i < telemetryList.size(); i++) {
            long curr = telemetryList.get(i).getLongValue().orElse(Long.MIN_VALUE);
            long prev = telemetryList.get(i-1).getLongValue().orElse(Long.MIN_VALUE);
            long diff = curr - prev;
            if (diff > 1) {
                if (diff < 20) {
                    lossSum += diff-1;
                }
                else {
                    if (sequenceCheck(telemetryList, i)) {
                        lossSum += diff-1;
                    }
                }
            }
        }
//        for (TsKvEntry curr : telemetry) {
//            if (prev != null) {
//                long delta = curr.getLongValue().orElse(Long.MIN_VALUE) - prev.getLongValue().orElse(Long.MIN_VALUE);
//                if (delta > 1 && delta < 20) {
//                    lossSum += delta-1;
//                    log.info("Loss detected: {}", delta-1);
//                }
//            }
//            prev = curr;
//        }

        double lossPercent = (double)lossSum / (lossSum + telemetry.size()) * 100;

        BasicTsKvEntry loss = new BasicTsKvEntry(maxTs, new LongDataEntry("packageLoss", lossSum));
        BasicTsKvEntry percent = new BasicTsKvEntry(maxTs, new DoubleDataEntry("lossPercent", lossPercent));
        result.add(loss);
        result.add(percent);
//        long requiredCount = max - min + 1;
//        double lossPercent = uniq*100/requiredCount;
//        BasicTsKvEntry result = new BasicTsKvEntry(maxTs, new DoubleDataEntry("lossPercent", lossPercent));
        return result;
    }

    private Collection<Collection<TsKvEntry>> partitionByDay(List<TsKvEntry> telemetry) {
        HashMultimap<String, TsKvEntry> multimap = HashMultimap.create();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        for (TsKvEntry tsKvEntry : telemetry) {
            String date = sdf.format(tsKvEntry.getTs());
            multimap.put(date, tsKvEntry);
        }
        return multimap.asMap().values();
    }

    private List<TsKvEntry> clearDuplicates(List<TsKvEntry> countersTelemetry, List<TsKvEntry> payloadsTelemetry, TbContext ctx, Device device) {
        ArrayList<PayloadCounterPair> pairs = new ArrayList<>();
        ArrayList<TsKvEntry> duplicates = new ArrayList<>();
        ArrayList<TsKvEntry> withoutDuplicates = new ArrayList<>();

        for (TsKvEntry counter : countersTelemetry) {
            for (TsKvEntry payload : payloadsTelemetry) {
                long counterTs = counter.getTs();
                long payloadTs = payload.getTs();
                String payloadValueAsString = payload.getValueAsString();
                if (counterTs == payloadTs) {
                    pairs.add(new PayloadCounterPair(counter, payloadValueAsString));
                    break;
                }
            }
        }
        pairs.sort(Comparator.comparingLong(PayloadCounterPair::getTs));

        for (int i = 0; i < pairs.size() - 1; i++) {
            boolean unique = true;
            for (int j = i+1; j < pairs.size(); j++) {
                long counter1 = pairs.get(i).counterTsKvEntry.getLongValue().orElse(Long.MIN_VALUE);
                long counter2 = pairs.get(j).counterTsKvEntry.getLongValue().orElse(Long.MIN_VALUE);
                String payload1 = pairs.get(i).payload;
                String payload2 = pairs.get(j).payload;
                if (counter1 == counter2 && payload1.equals(payload2)) {
                    BasicTsKvEntry duplicate = new BasicTsKvEntry(pairs.get(i).counterTsKvEntry.getTs(), new BooleanDataEntry("hasDuplicates", true));
                    duplicates.add(duplicate);
                    timeseriesService.save(ctx.getTenantId(), device.getId(), Collections.singletonList(duplicate), TimeUnit.DAYS.toSeconds(30));
//                    pairs.remove(i);
                    unique = false;
                    break;
                }
            }
            if (unique) {
                withoutDuplicates.add(pairs.get(i).counterTsKvEntry);
            }
        }

        withoutDuplicates.sort(Comparator.comparingLong(TsKvEntry::getTs));

//        for (PayloadCounterPair pair : pairs) {
//            withoutDuplicates.add(pair.counterTsKvEntry);
//        }
//        countersTelemetry.sort(Comparator.comparingLong(TsKvEntry::getTs));
//        for (TsKvEntry tsKvEntry : countersTelemetry) {
//            Optional<Long> value = tsKvEntry.getLongValue();
//            if (value.isPresent()) {
//                if (uniqCounters.contains(value.get())) {
//                    BasicTsKvEntry duplicate = new BasicTsKvEntry(tsKvEntry.getTs(), new BooleanDataEntry("packageDuplicate", true));
//                    duplicates.add(duplicate);
//                    timeseriesService.save(ctx.getTenantId(), device.getId(), Collections.singletonList(duplicate), TimeUnit.DAYS.toSeconds(90));
//                }
//                else {
//                    uniqCounters.add(value.get());
//                    withoutDuplicates.add(tsKvEntry);
//                }
//            }
//        }
        log.info("Duplicates found: {}", duplicates.size());
        return  withoutDuplicates;
    }

    private void processDevice(TbContext ctx, Device device) {
        long startTs = Instant.ofEpochMilli(System.currentTimeMillis() - time).truncatedTo(ChronoUnit.DAYS).toEpochMilli();
        ListenableFuture<List<TsKvEntry>> countersFuture = getCounterTelemetry(device, ctx.getTenantId(), startTs, System.currentTimeMillis());
        ListenableFuture<List<TsKvEntry>> payloadsFuture = getPayloadTelemetry(device, ctx.getTenantId(), startTs, System.currentTimeMillis());

        Futures.transform(countersFuture, countersData -> {
            if (!countersData.isEmpty()) {
                Futures.transform(payloadsFuture, payloadsData -> {
                    if (!payloadsData.isEmpty()) {
                        List<TsKvEntry> clearData = clearDuplicates(countersData, payloadsData, ctx, device);
                        partitionByDay(clearData).forEach(partition -> {
                            List<TsKvEntry> calculatedData = calculateLossPercent(partition);
                            timeseriesService.save(ctx.getTenantId(), device.getId(), calculatedData, 0);
                            log.info("Data saved {}", calculatedData);
                        });
                    }
                    else {
                        log.info("No data to compare");
                    }
                    return true;
                }, ctx.getDbCallbackExecutor());
            }
            else {
                log.info("No data saved");
            }
            return true;
        }, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<Map<String, Device>> fetchGateways(TbContext ctx) {
        Map<String, Device> map = new ConcurrentHashMap<>();
        List<ListenableFuture<Boolean>> allFutures = new ArrayList<>();
        PageLink pageLink = new PageLink(1000);
        PageData<Device> gateways = deviceService.findDevicesByTenantIdAndType(ctx.getTenantId(), "Gateway", pageLink);
        for (Device gateway : gateways.getData()) {
            ListenableFuture<Optional<AttributeKvEntry>> future = ctx.getAttributesService().find(ctx.getTenantId(), gateway.getId(), "lrrID", DataConstants.SERVER_SCOPE);
            allFutures.add(Futures.transform(future, data -> {
                map.put(data.get().getValueAsString(), gateway);
                return true;
            }, ctx.getDbCallbackExecutor()));
        }
        return Futures.transform(Futures.allAsList(allFutures), r -> map, ctx.getDbCallbackExecutor());
    }

    private ListenableFuture<List<TsKvEntry>> getRawTelemetry(TbContext ctx, Device device, TenantId tenantId, Long startTs, Long endTs) {
        BaseReadTsKvQuery baseReadTsKvQuery = new BaseReadTsKvQuery("raw", startTs, endTs, 0, Integer.MAX_VALUE, Aggregation.NONE);
        ListenableFuture<List<TsKvEntry>> all = timeseriesService.findAll(tenantId, device.getId(), Collections.singletonList(baseReadTsKvQuery));
        ListenableFuture<List<MyMessage>> future = Futures.transform(all, r -> {
            return r.stream().map(e -> {
                try {
                    JsonNode jsonNode = mapper.readTree(e.getValueAsString());
                    return (new MyMessage(jsonNode.get("FCntUp").intValue(), jsonNode.get("LrrId").textValue(), e.getTs()));
                } catch (Exception ex) {
                    log.error("Could not parse {}", e.getValueAsString(), ex);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }, ctx.getDbCallbackExecutor());
        return all;
    }

    private boolean sequenceCheck(List<TsKvEntry> list, int index) {
        boolean isSeq = true;
        if (index > 2 && index < list.size() - 3) {
            for (int e = index-1; e < index+3; e++) {
                if (list.get(e).getLongValue().orElse(Long.MIN_VALUE) < list.get(e-1).getLongValue().orElse(Long.MIN_VALUE)) {
                    isSeq = false;
                }
            }
        }
        if (index <= 2) {
            for (var e2 = 1; e2 < 5 && e2 < list.size(); e2++) {
                if (list.get(e2).getLongValue().orElse(Long.MIN_VALUE) < list.get(e2-1).getLongValue().orElse(Long.MIN_VALUE)) {
                    isSeq = false;
                }
            }
        }
        if (index >= list.size() - 3) {
            for (var e3 = list.size()-2; e3 > 0 && e3 > list.size() - 5; e3--) {
                if (list.get(e3).getLongValue().orElse(Long.MIN_VALUE) > list.get(e3+1).getLongValue().orElse(Long.MIN_VALUE)) {
                    isSeq = false;
                }
            }
        }
        return isSeq;
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

    class PayloadCounterPair {

        private TsKvEntry counterTsKvEntry;
        private String payload;

        public PayloadCounterPair(TsKvEntry counterTsKvEntry, String payload) {
            this.counterTsKvEntry = counterTsKvEntry;
            this.payload = payload;
        }

        private Long getTs() {
            return counterTsKvEntry.getTs();
        }
    }
}
