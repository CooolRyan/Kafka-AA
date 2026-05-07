package com.kafka.active.metrics;

public record TopicTailRecord(int partition, long offset, long timestampMs, String key, String value) {}
