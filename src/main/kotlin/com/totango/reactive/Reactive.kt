package com.totango.reactive

import io.github.resilience4j.bulkhead.Bulkhead
import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.kotlin.core.publisher.toMono
import reactor.util.retry.Retry
import reactor.util.retry.RetryBackoffSpec
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture


@Component
class Reactive : ApplicationListener<ApplicationReadyEvent> {

    override fun onApplicationEvent(event: ApplicationReadyEvent) {

        /*
        Future vs Mono
        1. Both can return zero or one value.
        2. Future start running as soon as I create it (eager) mono is lazy, it is just a data structure.
        3. Future represent *A value* may not computed yet,
           Mono represent a *recipe* to get a value in a non blocking way
           It means that I can use the same nono to trigger multiple computation (activate it more then once)
           In essence it is a code that I can manipulate like a data,
           compose it to create complex abstraction where
           In the case of Future the composition applied only for the current value, not to the recipe,
           For example it makes no sense to compose retries to a future that already send request and maybe return value.
           With Mono the composition give me another more complex
           recipe that I can use at will
        4. Mono has 2 kinds of final events (complete  (possibly empty), and error) thus I can know when Mono is empty,
           and I don't need null to represent absence of value
           while future just leave me hanging in case of empty value
        5. Mono has rich set of combinator compared to future,
           for example try to map the error a future return to another type of error or recover with value.
        6. A mono can be turned to a future at any time.
        7. Mono is not depends on spring, just on project-reactor.
        reference https://projectreactor.io/docs/core/release/reference/
         */

        // Mono is activated using the subscribe method (non-blocking), or the block method (blocking)

//        reactive protocol publisher/subscriber and subscription
//        The doOnX calls for side effect
//        map and flatmap (functor and monad)
//        many more combinators
//        mono and null are not good friends
//        exceptions are unchecked


//        val m = Mono.error<String>(IllegalAccessError("foo"))
//        m.log().toFuture().get()

        // well that is not very interesting

        // let's try to create a mono that simulate a non-blocking http call
        val timer = Timer(true)
        fun asyncGetUrl(latency: Duration, url: String): CompletableFuture<String> {
            val res = CompletableFuture<String>()
            logger.info("getting url: $url")
            val task: TimerTask = object : TimerTask() {
                override fun run() {
                    if (url.contains("error")) {
                        res.completeExceptionally(IOException("url not not found $url"))
                    } else {
                        res.complete("content of url: $url")
                    }
                }
            }
            timer.schedule(task, latency.toMillis())
            return res
        }

//        val body = asyncGetUrl(Duration.ofSeconds(3), "http://foo/bar").get()
//        logger.info("got body: $body")

        // now lets convert this to Mono

        fun getUrl(latency: Duration, url: String): Mono<String> {
            // by deferring the mono I supress the eagerness of the future, and delay the computation to the subscription time
            // instead of the creation time + if I subscribe twice the computation will run twice.
            return Mono.defer { asyncGetUrl(latency, url).toMono() }
        }


        val primary = getUrl(Duration.ofSeconds(3), "http://primary")

//        logger.info("This is the time")
//        logger.info("got body: ${primary.block()}")
//        logger.info("foo")
        // ok what is the big deal, it is the same.

        // let's limit the time of the request to 2 seconds without blocking the thread.
//        primary.timeout(Duration.ofSeconds(2))
//            .doOnNext { logger.info(it) }
//            .doOnError { error -> logger.error("oops $error") }
//            .subscribe()
//        logger.info("foo")

        // nice, this is not trivial with futures.
        // How can I recover from error

//        primary.timeout(Duration.ofSeconds(2))
//            .onErrorReturn("this is the default body")
//            .doOnNext { logger.info(it) }
//            .doOnError { logger.error("oops $it") }
//            .subscribe()

        // what if I want to execute sequentially 2 requests ?
        val primaryWithAll = primary
            .onErrorReturn("this is the default body")
            .doOnNext { logger.info(it) }
            .doOnError { logger.error("oops $it") }
//
        primaryWithAll.then(primaryWithAll)
            .subscribe()

//         cool but what If the second request depends on the result of the first one ?
        primaryWithAll.flatMap { url ->
            getUrl(Duration.ofSeconds(3), "http://primarry+($url)")
                .onErrorReturn("this is the default body")
                .doOnNext { logger.info(it) }
                .doOnError { logger.error("oops $it") }
        }.subscribe()

        // but what is the second function does not return a Mono ?
        // Well you can always use Mono.Just() or you can use map

//        primary.flatMap { Mono.just("$it is a nice day") }.doOnNext{logger.info(it) }.subscribe()
//        primary.map {"$it is a nice day" }.doOnNext{logger.info(it) }.subscribe()


        // that's cool, but say I wish to try my backup url when the primary fail ?
        val backup = getUrl(Duration.ofSeconds(1), "http://backup")
        primary.timeout(Duration.ofSeconds(2))
            .onErrorResume { backup }
            .doOnNext { logger.info(it) }
            .doOnError { logger.error("oops $it") }
            .subscribe()

        // this is cool, but if I wish to run them in parallel and return the first one
        Mono.firstWithValue(primary, backup).timeout(Duration.ofSeconds(5))
            .doOnNext { logger.info(it) }
            .doOnError { logger.error("oops $it") }
            .subscribe()

//        wait, but if I wish to run the first and only after some time start the backup ?
        Mono.firstWithValue(primary, backup.delaySubscription(Duration.ofSeconds(1))).timeout(Duration.ofSeconds(5))
            .doOnNext { logger.info(it) }
            .doOnError { logger.error("oops $it") }
            .subscribe()

        // or that you can stay in the mono land by combine them to a pair
        Mono.zip(primary, backup)
            .doOnNext { pair -> logger.info(pair.toString()) }
            .doOnError { logger.error("oops $it") }
            .subscribe()


        // if I wish to run them in parallel and get both of them, but get each as soon as it ready?
        // well that technically return a Flux

        val flux: Flux<String> = Flux.merge(primary, backup)
            .doOnNext { logger.info(it) }
            .doOnError { logger.error("oops $it") }
        flux.subscribe()


        // but if I am getting them as a list ?
        val requests1 = List(10) {
            getUrl(Duration.ofSeconds(it.toLong()), "http://item${it}")
        }
//         Flux.merge(requests1)
//            .doOnNext { logger.info(it) }
//            .doOnError { logger.error("oops $it") }
//            .subscribe()


        // But if one of them fails ?
        val requests = List(10) {
            if (it == 0) {
                getUrl(Duration.ofSeconds(0), "http://error${it}")
            } else {
                getUrl(Duration.ofSeconds(it.toLong()), "http://item${it}")
            }
        }
//        Flux.merge(requests)
//            .doOnNext { logger.info(it) }
//            .doOnError { logger.error("oops $it") }
//            .subscribe()

        // if this is not the behaviour you need, you can use onErrorResume
        // but what if I just wish to ignore the failed ones ?
        // this is easy, just resume with the empty mono

//        Flux.merge(requests.map { it.onErrorResume { Mono.empty() } })
//            .doOnNext { logger.info(it) }
//            .doOnError { logger.error("oops $it") }
//            .subscribe()

        // and if I wish to wait for all the result to be processed together ?
        // you can collect the results to list, map or other data structure that you have.

//        Flux.merge(requests.map { it.onErrorResume { Mono.empty() } })
//            .collectList()
//            .doOnNext { logger.info("the only event is a list: $it") }
//            .doOnError { logger.error("oops $it") }
//            .subscribe()


        // and if some of them are old code that block the thread?

        fun getUrlBlocking(url: String): String {
            logger.info("starting block get of $url")
            Thread.sleep(1000);
            logger.info("finish block get of $url")
            return url
        }

        fun getUrlBlockingNb(url: String): Mono<String> {
            return Mono.fromCallable { getUrlBlocking(url) }
                .subscribeOn(Schedulers.boundedElastic())
        }
//
//        Flux.range(0, 10)
//            .flatMap {i ->  getUrlBlockingNb("http://$i")  }
//            .doOnNext { logger.info(it) }
//            .subscribe()


        // and if I need to add a proper retry ?

        val fail = getUrl(Duration.ofSeconds(0), "http://error")

        val retry: RetryBackoffSpec = RetryBackoffSpec.backoff(5, Duration.ofSeconds(20))
            .doAfterRetry { signal: Retry.RetrySignal -> logger.info("retires #${signal.totalRetries()} got error ${signal.failure()}") }
//
        fail.retryWhen(retry)
            .doOnEach { logger.info("$it") }
            .doOnError { logger.error("oops $it") }
            .subscribe()

        // but if mono.subscribe trigger new computation how can I cache values I get in non-blocking way ?
//        val cached = primaryWithAll.cache()
//        val first = cached.block()
//        val second = cached.block()
//        logger.info("got first = $first, second = $second")
        // you can even add ttl


        // what if I have a parallel aggregator that doing multiple queries to ES from my API
        // and I wish to limit the number in-flight es queries executing by this call to 10 for all users combined.
        // for not slowing other parts of the system.

        val config = BulkheadConfig.custom()
            .maxConcurrentCalls(10)
            .maxWaitDuration(Duration.ofMillis(500))
            .build()
        val bulkhead = Bulkhead.of("parallel-aggregator", config)
//
        val parallel = primary.transformDeferred(BulkheadOperator.of(bulkhead))
//
//        val req = List(100) {
//            parallel
//        }
//        Flux.merge(req)
//            .doOnNext { logger.info(it) }
//            .doOnError { logger.error("oops $it") }
//            .subscribe()

        Thread.sleep(Long.MAX_VALUE)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(Reactive::class.java)
    }

}
