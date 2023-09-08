package com.petro.balance.model;

import java.time.Instant;

public record BalanceStateEvent(String name, long balance, Instant latestChangeTime, long count) {

}
