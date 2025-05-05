// File: src/main/java/com/harland/example/batch/BigQueryImportPipeline.java
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

/**
 * Pipeline Apache Beam pour lire un CSV local, agréger les montants
 * et écrire le résultat dans un fichier local unique.
 */
public class BigQueryImportPipeline {

    public static void main(String[] args) {
        LocalOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(LocalOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply("ReadCSV", TextIO.read().from(options.getInput()))
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
