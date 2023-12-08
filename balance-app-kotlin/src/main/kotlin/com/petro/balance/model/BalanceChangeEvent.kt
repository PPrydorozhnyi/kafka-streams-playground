package com.petro.balance.model

import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

data class BalanceChangeEvent(
    val name: String,
    val balance: Int = ThreadLocalRandom.current().nextInt(0, 1500),
    val publishedAt: Instant = Instant.now(),
)
