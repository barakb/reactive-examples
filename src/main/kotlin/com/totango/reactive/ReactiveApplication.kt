package com.totango.reactive

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import reactor.tools.agent.ReactorDebugAgent

@SpringBootApplication
class ReactiveApplication


fun main(args: Array<String>) {
    ReactorDebugAgent.init()
    runApplication<ReactiveApplication>(*args)
}
