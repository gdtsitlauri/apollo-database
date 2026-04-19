package com.apollo.lens;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class LensModel {
    public double predictSplitPoint(Map<String, Double> features) {
        double fillFactor = features.getOrDefault("fillFactor", 0.5);
        double skew = features.getOrDefault("skew", 0.0);
        double locality = features.getOrDefault("locality", 0.5);
        double prediction = 0.5 + 0.2 * skew + 0.15 * locality - 0.1 * (fillFactor - 0.5);
        return Math.min(0.9, Math.max(0.1, prediction));
    }

    public String choosePlan(List<PlanCandidate> candidates) {
        return candidates.stream()
                .min(Comparator.comparingDouble(candidate -> candidate.estimatedCost() - candidate.learnedBias()))
                .map(PlanCandidate::name)
                .orElse("SEQ_SCAN");
    }

    public record PlanCandidate(String name, double estimatedCost, double learnedBias) {
    }
}
