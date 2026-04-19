package com.apollo.transaction;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DeadlockDetector {
    public boolean hasDeadlock(Map<Long, Set<Long>> waitForGraph) {
        return findCycle(waitForGraph).isPresent();
    }

    public Optional<Set<Long>> findCycle(Map<Long, Set<Long>> waitForGraph) {
        Set<Long> visited = new HashSet<>();
        for (Long node : waitForGraph.keySet()) {
            Optional<Set<Long>> cycle = dfs(node, waitForGraph, visited, new HashSet<>(), new ArrayDeque<>());
            if (cycle.isPresent()) {
                return cycle;
            }
        }
        return Optional.empty();
    }

    public Optional<Long> youngestVictim(Map<Long, Set<Long>> waitForGraph) {
        return findCycle(waitForGraph).flatMap(cycle -> cycle.stream().max(Long::compareTo));
    }

    private Optional<Set<Long>> dfs(
            long current,
            Map<Long, Set<Long>> graph,
            Set<Long> visited,
            Set<Long> active,
            Deque<Long> path
    ) {
        if (active.contains(current)) {
            Set<Long> cycle = new HashSet<>();
            boolean inCycle = false;
            for (Long node : path) {
                if (node == current) {
                    inCycle = true;
                }
                if (inCycle) {
                    cycle.add(node);
                }
            }
            cycle.add(current);
            return Optional.of(cycle);
        }
        if (!visited.add(current)) {
            return Optional.empty();
        }

        active.add(current);
        path.addLast(current);
        for (Long next : graph.getOrDefault(current, Set.of())) {
            Optional<Set<Long>> cycle = dfs(next, graph, visited, active, path);
            if (cycle.isPresent()) {
                return cycle;
            }
        }
        path.removeLast();
        active.remove(current);
        return Optional.empty();
    }
}
