package com.harland.example.batch;

import com.harland.example.common.model.TransferRecord;
import com.harland.example.common.options.BigQueryImportOptions;
import com.harland.example.common.transform.ConvertToTransferRecordFn;
import com.harland.example.common.utils.MathUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BigQueryImportPipeline {

  public static void main(String[] args) {
    BigQueryImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryImportOptions.class);

    Pipeline p = Pipeline.create(options);

    p.apply("ReadFromLocal", TextIO.read().from("ali/accidents_2005_2020.csv"))
        .apply("ConvertToTransferRecord", ParDo.of(new ConvertToTransferRecordFn()))
        .apply(
            "CreateKVPairs",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                .via((TransferRecord record) -> KV.of(record.getUser(), record.getAmount())))
        .apply("SumAmountsPerUser", Sum.doublesPerKey())
        .apply(
            "FormatResults",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Double> record) ->
                        record.getKey() + "," + MathUtils.roundToTwoDecimals(record.getValue())))
        .apply("WriteToLocal", TextIO.write().to("output/results").withSuffix(".csv").withoutSharding());

    p.run().waitUntilFinish();
  }
}
