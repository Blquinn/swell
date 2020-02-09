package me.blq.swell

import java.util.concurrent.Executors
import kotlin.reflect.KCallable
import kotlin.reflect.KFunction

class Server(
    val config: ServerConfig,
    val broker: Broker,
    val backend: Backend
) {
    val tasks: MutableMap<String, KFunction<Unit>> = mutableMapOf()
    private val jobExecutor = Executors.newFixedThreadPool(config.workerConcurrency)

    /* Task Registration */

    fun registerNamedTask(name: String, newTask: KFunction<Unit>) {
        tasks[name] = newTask
    }

    fun registerNamedTasks(newTasks: Map<String, KFunction<Unit>>) {
        tasks.putAll(newTasks)
    }

    fun registerTask(newTask: KFunction<Unit>) {
        tasks[newTask::class.java.canonicalName] = newTask
    }

    fun registerTasks(vararg newTasks: KFunction<Unit>) {
        newTasks
            .map { Pair(it::class.java.canonicalName, it) }
            .forEach { tasks[it.first] = it.second }
    }
}

class UnregisteredTaskException(taskName: String) : Exception("Task $taskName is not registered with the server.")
