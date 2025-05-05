package com.harland.example.common.model;

import java.io.Serializable;
import java.util.Objects;

public class TransferRecord implements Serializable {

    private String user;
    private String transferTo;
    private Double amount;

    public TransferRecord(String user, String transferTo, Double amount) {
        this.user = user;
        this.transferTo = transferTo;
        this.amount = amount;
    }

    public String getUser() {
        return user;
    }

    public String getTransferTo() {
        return transferTo;
    }

    public Double getAmount() {
        return amount;
    }
    @Override
public boolean equals(Object o) {
  if (this == o) return true;
  if (!(o instanceof TransferRecord)) return false;
  TransferRecord that = (TransferRecord) o;
  return Double.compare(that.getAmount(), getAmount()) == 0
      && Objects.equals(getUser(), that.getUser());
}

@Override
public int hashCode() {
  return Objects.hash(getUser(), getAmount());
}

}
