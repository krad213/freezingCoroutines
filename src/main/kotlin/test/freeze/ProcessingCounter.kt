package test.freeze

import java.util.concurrent.atomic.AtomicLong

class ProcessingCounter {

    val counters = mutableMapOf<String, AtomicLong>()
    val nameIndexes = mutableMapOf<String, Int>()

    fun increment(key: String): Long {
        return counters.computeIfAbsent(normalizeKey(key)) { AtomicLong(0) }.incrementAndGet()
    }

    fun increment(key: String, value: Long): Long {
        return counters.computeIfAbsent(normalizeKey(key)) { AtomicLong(0) }.addAndGet(value)
    }

    private fun normalizeKey(key: String): String {
        val index =  nameIndexes.computeIfAbsent(key) { nameIndexes.size }
        return "${index.toString().padStart(3,'0')} - $key"
    }

    fun log() = counters.entries
        .sortedBy { it.key }
        .map { "${it.key}: ${it.value}" }
        .joinToString("\n")
        .apply { println("Counters:\n$this") }
}