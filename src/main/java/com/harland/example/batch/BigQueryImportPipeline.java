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
        // Parse custom options if any (e.g., for runner configuration)
        BigQueryImportOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(BigQueryImportOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            // Lire le fichier CSV local
            .apply("ReadFromLocal", TextIO.read().from("ali/accidents_2005_2020.csv"))
            // Convertir chaque ligne CSV en TransferRecord
            .apply("ConvertToTransferRecord", ParDo.of(new ConvertToTransferRecordFn()))
            // Créer des paires clef/valeur (user, amount)
            .apply(
                "CreateKVPairs",
                MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles())
                ).via((TransferRecord record) -> KV.of(record.getUser(), record.getAmount()))
            )
            // Agréger les montants par utilisateur
            .apply("SumAmountsPerUser", Sum.doublesPerKey())
            // Formater les résultats en CSV (user,amount)
            .apply(
                "FormatResults",
                MapElements.into(TypeDescriptors.strings())
                    .via(record -> record.getKey() + "," + MathUtils.roundToTwoDecimals(record.getValue()))
            )
            // Écrire le résultat dans un fichier local sans sharding (un seul fichier)
            .apply(
                "WriteToLocal",
                TextIO.write()
                    .to("output/accidents_result")
                    .withSuffix(".csv")
                    .withoutSharding()
            );

        pipeline.run().waitUntilFinish();
    }
}
