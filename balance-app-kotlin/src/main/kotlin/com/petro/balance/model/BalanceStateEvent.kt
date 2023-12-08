package com.petro.balance.model

import java.time.Instant

data class BalanceStateEvent(val name: String, val balance: Long, val latestChangeTime: Instant, val count: Long)
