package me.blq.swell.workflow

import java.util.*

/**
 * task is a helper to create a single task, it is the core building block of
 * all the other workflows.
 */
fun task(
    name: String,
    vararg args: Any,
    callbacks: List<Signature> = listOf(),
    errCallbacks: List<Signature> = listOf(),
    chain: List<Signature> = listOf()
): Signature {
    val body = SignatureBody(args.toMutableList(), callbacks, errCallbacks, chain, null)
    val id = UUID.randomUUID().toString()
    val headers = SignatureHeaders(id = id, task = name, rootId = id)
    val properties = SignatureProperties(correlationId = id)
    return Signature(body, headers, properties)
}

/**
 * chain creates a chain of tasks that run sequentially.
 *
 * @param tasks A list of tasks to run in sequence.
 * @param callbacks A list of callbacks to run after the chain is complete.
 * @param errCallbacks A list of callbacks to call if any of the tasks in the chain fail.
 * @return A signature which represents the chain.
 */
fun chain(
    vararg tasks: Signature,
    callbacks: List<Signature> = listOf(),
    errCallbacks: List<Signature> = listOf()
): Signature {
    assert(tasks.isNotEmpty())

    val root = tasks.first()
    val rest = tasks.takeLast(tasks.size-2).map { it.copy(headers = it.headers.copy(rootId = root.headers.id)) }
    return root.copy(body = root.body.copy(
        chain = rest,
        callbacks = callbacks.map { it.copy(headers = it.headers.copy(rootId = root.headers.id)) },
        errCallbacks = errCallbacks.map { it.copy(headers = it.headers.copy(rootId = root.headers.id)) }
    ))
}

/**
 * group creates a group of tasks which run in parallel
 */
//fun group(
//    vararg tasks: Signature
//): Signature {
//    assert(tasks.isNotEmpty())
//
//    return task("group", )
//}
