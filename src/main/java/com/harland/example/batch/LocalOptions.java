// File: src/main/java/com/harland/example/batch/LocalOptions.java
package com.harland.example.batch;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface LocalOptions extends PipelineOptions {

    @Description("Path of the input CSV file, e.g. ali/accidents_2005_2020.csv")
    @Validation.Required
    String getInput();
    void setInput(String value);

    @Description("Prefix for the output file, e.g. output/accidents_result")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
}
