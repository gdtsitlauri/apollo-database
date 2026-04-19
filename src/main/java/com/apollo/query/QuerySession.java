package com.apollo.query;

import com.apollo.transaction.Transaction;
import com.apollo.transaction.TransactionSnapshot;

import java.util.LinkedHashSet;
import java.util.Set;

public record QuerySession(
        Transaction transaction,
        TransactionSnapshot snapshot,
        Set<Long> committedTransactions,
        Set<Long> abortedTransactions
) {
    public static QuerySession autocommit(Set<Long> committedTransactions, Set<Long> abortedTransactions) {
        Set<Long> committed = new LinkedHashSet<>(committedTransactions);
        committed.add(0L);
        return new QuerySession(null, new TransactionSnapshot(Long.MAX_VALUE, Set.of()), Set.copyOf(committed), Set.copyOf(abortedTransactions));
    }
}
