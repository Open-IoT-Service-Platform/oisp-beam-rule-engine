package org.oisp.conf;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface CmdlineOptions extends PipelineOptions {
    @Description("JSON config for RuleEngine in Base64")
    @Default.String("")
    String getJSONConfig();
    void setJSONConfig(String value);

}
