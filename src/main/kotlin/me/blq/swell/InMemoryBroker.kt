package me.blq.swell

import me.blq.swell.workflow.Task

class InMemoryBroker(
    val server: Server
) : Broker {
    val worker = Worker()

    override fun startConsuming() {}

    override fun stopConsuming() {}

    override fun publish(task: Task) {
    }
}
