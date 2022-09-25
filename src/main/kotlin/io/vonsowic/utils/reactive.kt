package io.vonsowic.utils

import reactor.core.publisher.Flux
import java.time.Duration
import java.time.Instant

private val TICK_DURATION = Duration.ofMillis(100)

fun <E> Flux<E>.completeOnIdleStream(maxIdleTime: Duration): Flux<E> {
    var lastEvent = Instant.now()
    return Flux.create { sink ->
        val subscription =
            this.doOnNext { lastEvent = Instant.now() }
                .subscribe { sink.next(it) }

        Flux.interval(Duration.ZERO, TICK_DURATION)
            .filter { lastEvent + maxIdleTime < Instant.now() }
            .take(1)
            .subscribe {
                subscription.dispose()
                sink.complete()
            }
    }
}