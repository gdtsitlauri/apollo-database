package com.apollo.showcase;

import com.apollo.storage.Record;
import com.apollo.transaction.MVCCManager;
import com.apollo.transaction.TransactionSnapshot;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Demonstrates MVCC version chains and snapshot isolation.
 */
public class MVCCVersionChainShowcase {

    public record VersionChainReport(List<String> lines) {
        @Override
        public String toString() {
            return String.join("\n", lines);
        }
    }

    public VersionChainReport run() {
        List<String> lines = new ArrayList<>();
        MVCCManager mvcc = new MVCCManager();

        // Simulate a record "salary=1000" created by txn 1, then updated by txn 3
        Record v1 = Record.fromString("salary=1000");
        v1.setXmin(1);
        v1.setXmax(3L);

        Record v2 = Record.fromString("salary=1500");
        v2.setXmin(3);

        // Txn 2 snapshot: only txn 1 committed; txn 3 is still active → txn 2 cannot see v2
        Set<Long> committedAtSnap2 = Set.of(1L);
        Set<Long> aborted = Set.of();
        TransactionSnapshot snap2 = new TransactionSnapshot(2L, Set.of(2L, 3L));
        boolean txn2seesV1 = mvcc.isVisible(v1, snap2, committedAtSnap2, aborted);
        boolean txn2seesV2 = mvcc.isVisible(v2, snap2, committedAtSnap2, aborted);

        // Txn 4 snapshot: both txn 1 and txn 3 committed → txn 4 sees v2 only
        Set<Long> committedAtSnap4 = Set.of(1L, 3L);
        TransactionSnapshot snap4 = new TransactionSnapshot(4L, Set.of(4L));
        boolean txn4seesV1 = mvcc.isVisible(v1, snap4, committedAtSnap4, aborted);
        boolean txn4seesV2 = mvcc.isVisible(v2, snap4, committedAtSnap4, aborted);

        lines.add("=== MVCC Version Chain Demo ===");
        lines.add("Record: employees.salary");
        lines.add("  v1 (xmin=1, xmax=3): salary=1000");
        lines.add("  v2 (xmin=3, xmax=null): salary=1500");
        lines.add("");
        lines.add("Txn 2 snapshot (started before txn 3 committed):");
        lines.add("  sees v1 (salary=1000): " + txn2seesV1 + "  [expected: true]");
        lines.add("  sees v2 (salary=1500): " + txn2seesV2 + "  [expected: false]");
        lines.add("");
        lines.add("Txn 4 snapshot (started after txn 3 committed):");
        lines.add("  sees v1 (salary=1000): " + txn4seesV1 + "  [expected: false]");
        lines.add("  sees v2 (salary=1500): " + txn4seesV2 + "  [expected: true]");
        lines.add("");

        // Demonstrate aborted transaction: v3 created by aborted txn 5
        Record v3 = Record.fromString("salary=9999");
        v3.setXmin(5);
        Set<Long> withAborted = committedAtSnap4;
        Set<Long> abortedSet = Set.of(5L);
        TransactionSnapshot snap6 = new TransactionSnapshot(6L, Set.of(6L));
        boolean txn6seesV3 = mvcc.isVisible(v3, snap6, withAborted, abortedSet);
        lines.add("Aborted txn 5 created v3 (salary=9999):");
        lines.add("  txn 6 sees v3: " + txn6seesV3 + "  [expected: false — aborted version invisible]");
        lines.add("");
        lines.add("Snapshot isolation guarantees each transaction sees a consistent");
        lines.add("point-in-time snapshot. Readers never block writers.");

        return new VersionChainReport(lines);
    }
}
