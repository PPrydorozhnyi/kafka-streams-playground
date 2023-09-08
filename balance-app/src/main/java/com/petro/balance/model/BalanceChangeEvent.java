package com.petro.balance.model;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public record BalanceChangeEvent(String name, int balance, Instant publishedAt) {

  public BalanceChangeEvent(String name) {
    this(name, ThreadLocalRandom.current().nextInt(0,1500), Instant.now());
  }

}
