package org.oisp.pipeline;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.common.serialization.StringSerializer;

import org.joda.time.Duration;
import org.oisp.collection.RuleWithRuleConditions;
import org.oisp.collection.RulesWithObservation;
import org.oisp.conf.Config;
import org.apache.beam.sdk.Pipeline;
import org.oisp.collection.Observation;
import org.oisp.transformation.CheckBasicRule;
import org.oisp.transformation.CheckRuleFulfillment;
import org.oisp.transformation.CheckStatisticsRule;
import org.oisp.transformation.CheckTimeBasedRule;
import org.oisp.transformation.DownloadRulesTask;
import org.oisp.transformation.GetComponentRulesTask;
import org.oisp.transformation.KafkaSourceObservationsProcessor;
import org.oisp.transformation.KafkaSourceProcessor;
import org.oisp.transformation.KafkaSourceRulesUpdateProcessor;
import org.oisp.transformation.PersistRuleState;
import org.oisp.transformation.PersistRulesTask;
import org.oisp.transformation.PersistStatisticsRuleState;
import org.oisp.transformation.PersistTimeBasedRuleState;
import org.oisp.transformation.SendAlertFromRule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FullPipelineBuilder {

    private FullPipelineBuilder() {
    }

    public static Pipeline build(PipelineOptions options, Config conf) {
        Pipeline p = Pipeline.create(options);


        //Rules-Update Pipeline
        //Detects changes of Ruleset
        //Updates local rule storage
        //Signals rule updates to Observation Pipeline
        DownloadRulesTask downloadRulesTask = new DownloadRulesTask(conf);
        PersistRulesTask persistRulesTask = new PersistRulesTask(conf);
        KafkaSourceProcessor rulesKafka = new KafkaSourceRulesUpdateProcessor(conf);
        PCollection<Long> persistRuleUpdate = p.apply(rulesKafka.getTransform())
                .apply(ParDo.of(new CombineKVFromByteArrayFn()))
                .apply(ParDo.of(downloadRulesTask))
                .apply(ParDo.of(persistRulesTask));

        //Side input to signal change of ruleset
        //PCollectionView<Map<String, Long>> kafkaSideInput =
        PCollectionView<List<Long>> kafkaSideInput =
               persistRuleUpdate
                        .apply(Window.<Long>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane())).discardingFiredPanes())
                        .apply(Max.longsGlobally())
                        //.apply(ParDo.of(new LongToMapFn()))
                        .apply(View.asList());

        //Observation Pipeline
        //Map observations to rules
        //Process rules for Basic, Timebased and Statistics
        KafkaSourceObservationsProcessor observationsKafka = new KafkaSourceObservationsProcessor(conf);
        PCollection<List<RulesWithObservation>> rwo = p.apply(observationsKafka.getTransform())
                .apply(ParDo.of(new KafkaToObservationFn()))
                .apply(ParDo.of(new GetComponentRulesTask(conf, kafkaSideInput))
                .withSideInputs(kafkaSideInput));
        PCollection<KV<String, RuleWithRuleConditions>> basicRulePipeline =
                rwo
                        .apply(ParDo.of(new CheckBasicRule()));

        PCollection<KV<String, RuleWithRuleConditions>> timeBasedRulePipeline =
                rwo
                        .apply(ParDo.of(new CheckTimeBasedRule()))
                        .apply(ParDo.of(new PersistTimeBasedRuleState()));

        PCollection<KV<String, RuleWithRuleConditions>> statisticsRulePipeline =
                rwo
                        .apply(ParDo.of(new CheckStatisticsRule()))
                        .apply(ParDo.of(new PersistStatisticsRuleState()));
        PCollectionList<KV<String, RuleWithRuleConditions>> ruleColl = PCollectionList.of(basicRulePipeline).and(timeBasedRulePipeline).and(statisticsRulePipeline);
        ruleColl
                .apply(Flatten.<KV<String, RuleWithRuleConditions>>pCollections())
                .apply(ParDo.of(new PersistRuleState()))
                .apply(ParDo.of(new CheckRuleFulfillment()))
                .apply(ParDo.of(new SendAlertFromRule(conf)));

        //Heartbeat Pipeline
        //Send regular Heartbeat to Kafka topic
        String serverUri = conf.get(Config.KAFKA_URI_PROPERTY).toString();
        System.out.println("serverUri:" + serverUri);
        p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
                .apply(ParDo.of(new StringToKVFn()))
                .apply(KafkaIO.<String, String>write()
                        .withBootstrapServers(serverUri)
                        .withTopic("heartbeat")
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(StringSerializer.class));



        return p;
    }

    static class KafkaToObservationFn extends DoFn<KafkaRecord<String, byte[]>, List<Observation>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KafkaRecord<String, byte[]> record = c.element();
            Gson g = new Gson();
            List<Observation> obserations = g.fromJson(new String(record.getKV().getValue()), new TypeToken<List<Observation>>() {
            }.getType());
            c.output(obserations);
        }
    }

    static class StringToKVFn extends DoFn<Long, KV<String, String>> {
        @DoFn.ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, String> outputKv = KV.<String, String>of("", "rules-engine");
            c.output(outputKv);
        }
    }

    static class CombineKVFromByteArrayFn extends DoFn<KafkaRecord<String, byte[]>, KV<String, String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            KafkaRecord<String, byte[]> record = c.element();
            KV<String, String> outputKv = KV.<String, String>of("key", record.getKV().getKey() + " " + new String(record.getKV().getValue()));
            c.output(outputKv);
        }
    }
    static class LongToMapFn extends DoFn<Long, Map<String, Long>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Map<String, Long> map = new HashMap<String, Long>();
            map.put("ver", c.element());
            System.out.println("Marcel423: " + c.element());
            c.output(map);
        }
    }
}
