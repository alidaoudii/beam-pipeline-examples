package com.harland.example.batch;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface LocalOptions extends PipelineOptions {
    @Description("Path of the input CSV file")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Prefix for the output files")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
}
