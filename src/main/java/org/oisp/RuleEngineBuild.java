package org.oisp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.oisp.conf.CmdlineOptions;
import org.oisp.conf.Config;
import org.oisp.conf.ExternalConfig;
import org.oisp.pipeline.FullPipelineBuilder;

import java.util.Base64;


/**
 * RuleEngineBuild - creates different pipelines for Rule-engine Example
 */



public abstract class RuleEngineBuild {
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CmdlineOptions.class);

        PipelineOptionsFactory.register(CmdlineOptions.class);
        Pipeline fullPipeline;

        //read json config from ENVIRONMENT - needed because stupid mvn cannot read JSON from cmdline. Unbelievable, but true.
        String inputConfig = ((CmdlineOptions) options).getJSONConfig().replaceAll(" ", "\n");
        String config = new String(Base64.getMimeDecoder().decode(inputConfig));

        System.out.println("JSON config retrieved: " + config);
        ExternalConfig extConf = ExternalConfig.getConfigFromString(config);
        Config conf;
        conf = extConf.getConfig();
        fullPipeline = FullPipelineBuilder.build(options, conf);
        fullPipeline.run().waitUntilFinish();
    }
}
