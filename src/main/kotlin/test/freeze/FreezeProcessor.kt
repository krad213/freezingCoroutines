package test.freeze

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.time.LocalDate
import java.util.Random
import java.util.UUID

class FreezeProcessor {
    open fun process() {
        val counter = ProcessingCounter()

        runBlocking {
            (1..100)
                .onEach { counter.increment("point 1") }
                .map { "number:$it" }
                .onEach { counter.increment("point 2") }
                .asFlow()
                .flowOn(Dispatchers.IO)
                .transform {
                    val rnd = Random()
                    val nums = (1..10).map { rnd.nextInt(100) }
                    emit(it to nums)
                }
                .onEach { counter.increment("point 3") }
                .filter { (key, records) -> !key.contains("2") }
                .onEach { counter.increment("point 4") }
                .filter { (key, records) ->
                    (!records.any { r -> r == 45 }).also {
                        if (!it) {
                            println("Group $key contains 45")
                        }
                    }
                }
                .onEach { counter.increment("point 5") }
                .transform { (key, records) ->
                    emitAll(records.map { Record(key, it) }.asFlow())
                }
                .onEach { counter.increment("point 5") }
                .groupToList { it.key }
                .onEach { counter.increment("point 6") }
                .map { (key, records) ->
                    records.map {
                        DataContainer(
                            key,
                            it.value,
                            randomOf("Open", "Closed", "Other", "Blah"),
                            LocalDate.now().minusDays(Random().nextInt(1000).toLong()),
                            Random().nextDouble(),
                            UUID.randomUUID().toString()
                        )
                    }.sortedWith(
                        Comparator
                            .comparing<DataContainer, Boolean> { it.stage !in setOf("Blah", "Other") }
                            .thenComparing<Boolean> { it.closeDate >= LocalDate.now() }
                            .then(Comparator.comparing<DataContainer?, Duration?> {
                                if (it.closeDate.isBefore(LocalDate.now())) {
                                    Duration.between(it.closeDate.atStartOfDay(), LocalDate.now().atStartOfDay())
                                } else {
                                    Duration.between(LocalDate.now().atStartOfDay(), it.closeDate.atStartOfDay())
                                }
                            }.reversed())
                            .thenComparing<Double> { it.amount ?: 0.0 }
                            .thenComparing<String> { it.id }
                            .reversed()
                    )
                }
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .chunked(100)
                .transform {
                    emitAll(
                        it
                            .map {
                                DataContainer(it.key, it.record, it.stage, it.closeDate, it.amount, it.id)
                            }
                            .asFlow()
                    )
                }
                .collect { println(it) }

        }
    }

    private fun randomOf(vararg strs: String) = strs[Random().nextInt(strs.size)]


    private fun <T, K> Flow<T>.groupToList(getKey: (T) -> K): Flow<Pair<K, List<T>>> = flow {
        val storage = mutableMapOf<K, MutableList<T>>()
        collect { t -> storage.getOrPut(getKey(t)) { mutableListOf() } += t }
        val iterator = storage.iterator()
        while (iterator.hasNext()) {
            val (k, ts) = iterator.next()
            iterator.remove()
            emit(k to ts as List<T>)
        }
    }

    private fun <T> Flow<T>.chunked(size: Int): Flow<List<T>> = flow {
        val chunkedList = mutableListOf<T>()
        this@chunked.collect {
            chunkedList.add(it)
            if (chunkedList.size == size) {
                emit(chunkedList.toList())
                chunkedList.clear()
            }
        }
        if (chunkedList.isNotEmpty()) {
            emit(chunkedList.toList())
        }
    }
}

data class Record(val key: String, val value: Int)

data class DataContainer(
    val key: String,
    val record: Int,
    val stage: String,
    val closeDate: LocalDate,
    val amount: Double,
    val id: String
)