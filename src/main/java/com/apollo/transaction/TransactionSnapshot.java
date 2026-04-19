package com.apollo.transaction;

import java.util.Set;

public record TransactionSnapshot(long transactionId, Set<Long> activeTransactions) {
}
