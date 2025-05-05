package com.harland.example.common.transform;

import com.harland.example.common.model.TransferRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class ConvertToTransferRecordFn extends DoFn<String, TransferRecord> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    String line = c.element();
    // 1) ignorer lignes vides
    if (line == null || line.trim().isEmpty()) {
      return;
    }
    // 2) ignorer l’en-tête (adapter la condition selon le contenu exact du header)
    //    par exemple si votre CSV commence par "user,amount"
    if (line.startsWith("user,") || line.startsWith("User,")) {
      return;
    }
    // 3) découper et checker le nombre de champs
    String[] parts = line.split(",");
    if (parts.length < 2) {
      // ligne malformée
      return;
    }
    String user = parts[0].trim();
    double amount;
    try {
      amount = Double.parseDouble(parts[1].trim());
    } catch (NumberFormatException e) {
      // montant invalide => on skip
      return;
    }
    c.output(new TransferRecord(user, amount));
  }
}
