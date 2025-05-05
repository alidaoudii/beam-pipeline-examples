package com.harland.example.batch;

import com.harland.example.common.model.TransferRecord;
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
import org.apache.beam.sdk.coders.AvroCoder;

import org.apache.beam.sdk.transforms.Filter;

public class BigQueryImportPipeline {

    public static void main(String[] args) {
        LocalOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(LocalOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        // tout de suite après Pipeline.create(options);
        pipeline.getCoderRegistry()
            .registerCoderForClass(
            TransferRecord.class,
            AvroCoder.of(TransferRecord.class));


        pipeline
            .apply("ReadCSV", TextIO.read().from(options.getInput()))
            .apply("FilterHeader", Filter.by((String line) ->
                     line != null
                     && !line.trim().isEmpty()
                     && !line.startsWith("user,")))  // même condition que ci-dessus
            .apply("ToRecord", ParDo.of(new ConvertToTransferRecordFn()))
            .apply("ToKV", MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                .via(record -> KV.of(record.getUser(), record.getAmount())))
            .apply("SumAmounts", Sum.doublesPerKey())
            .apply("FormatCSV", MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "," + MathUtils.roundToTwoDecimals(kv.getValue())))
            .apply("WriteCSV", TextIO.write()
                .to(options.getOutput())
                .withSuffix(".csv")
                .withoutSharding());
        

        pipeline.run().waitUntilFinish();
    }
}
