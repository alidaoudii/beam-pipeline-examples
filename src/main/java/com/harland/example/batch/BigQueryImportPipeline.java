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

public class BigQueryImportPipeline {

    public static void main(String[] args) {
        // Utilisation de notre interface d'options locales
        LocalOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(LocalOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            // Lire le fichier CSV local
            .apply("ReadCSV", TextIO.read().from(options.getInput()))
            // Convertir en TransferRecord
            .apply("ToRecord", ParDo.of(new ConvertToTransferRecordFn()))
            // Créer KV<user, amount>
            .apply("ToKV",
                MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                .via(record -> KV.of(record.getUser(), record.getAmount())))
            // Somme des montants par utilisateur
            .apply("SumAmounts", Sum.doublesPerKey())
            // Formatter en lignes CSV
            .apply("FormatCSV",
                MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey() + "," + MathUtils.roundToTwoDecimals(kv.getValue())))
            // Écriture dans un unique fichier local
            .apply("WriteCSV",
                TextIO.write()
                      .to(options.getOutput())
                      .withSuffix(".csv")
                      .withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
