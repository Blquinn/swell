package me.blq.swell.workflow

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.time.ZonedDateTime
import java.util.*
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.jvm.reflect


class SignatureProperties(
    val correlationId: String = UUID.randomUUID().toString(),
    val contentType: String = "application/json",
    val contentEncoding: String = "application/json",
    val replyTo: String? = null // Queue or url
) {
    fun copy(
        correlationId: String = this.correlationId,
        contentType: String = this.contentType,
        contentEncoding: String = this.contentEncoding,
        replyTo: String? = this.replyTo
    ): SignatureProperties {
        return SignatureProperties(correlationId, contentType, contentEncoding, replyTo)
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false

        if (other !is SignatureProperties) return false

        return when {
            correlationId != other.correlationId -> false
            contentType != other.contentType -> false
            contentEncoding != other.contentEncoding -> false
            replyTo != other.replyTo -> false
            else -> true
        }
    }

    override fun toString(): String {
        return "Properties(correlationId = $correlationId, contentType = $contentType, " +
                "contentEncoding = $contentEncoding, replyTo = $replyTo)"
    }
}

class SignatureHeaders(
    val lang: String = "java", // eg. java
    val task: String, // The task name
    val id: String,
    val rootId: String, // Root task
    val parentId: String? = null,
    val group: String? = null,

    // Optional
    val meth: String? = null,
    val shadow: String? = null, // Alias name
    val eta: ZonedDateTime? = null,
    val expires: ZonedDateTime? = null,
    val retries: Int? = null,
    val origin: String? = null // The node name
//    val timeLimit: Pair<Int, Int>?
//    val argsRepr: String?,
//    val kwargsRepr: String?,
) {
    fun copy(
        lang: String = this.lang,
        task: String = this.task,
        id: String = this.id,
        rootId: String = this.rootId,
        parentId: String? = this.parentId,
        group: String? = this.group,
        meth: String? = this.meth,
        shadow: String? = this.shadow,
        eta: ZonedDateTime? = this.eta,
        expires: ZonedDateTime? = this.expires,
        retries: Int? = this.retries,
        origin: String? = this.origin
    ): SignatureHeaders {
        return SignatureHeaders(lang, task, id, rootId, parentId, group, meth, shadow, eta, expires, retries, origin)
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false

        if (other !is SignatureHeaders) return false

        return when {
            lang != other.lang -> false
            task != other.task -> false
            id != other.id -> false
            rootId != other.rootId -> false
            parentId != other.parentId -> false
            group != other.group -> false
            meth != other.meth -> false
            shadow != other.shadow -> false
            eta != other.eta -> false
            expires != other.expires -> false
            retries != other.retries -> false
            origin != other.origin -> false
            else -> true
        }
    }

    override fun toString(): String {
        return "SignatureHeaders(lang = $lang, task = $task, id = $id, rootId = $rootId, parentId = $parentId, " +
                "group = $group, meth = $meth, shadow = $shadow, eta = $eta, expires = $expires, retries = $retries, " +
                "origin = $origin)"
    }
}

class SignatureBody(
    val args: List<Any>,
//    val kwargs: ObjectNode,
    val callbacks: List<Signature> = listOf(),
    val errCallbacks: List<Signature> = listOf(),
    val chain: List<Signature> = listOf(),
    val chord: Signature? = null
) {
    fun copy(
        args: List<Any> = this.args,
        callbacks: List<Signature> = this.callbacks,
        errCallbacks: List<Signature> = this.errCallbacks,
        chain: List<Signature> = this.chain,
        chord: Signature? = this.chord
    ): SignatureBody {
        return SignatureBody(args, callbacks, errCallbacks, chain, chord)
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false

        if (other !is SignatureBody) return false

        return when {
            args != other.args -> false
            callbacks != other.callbacks -> false
            errCallbacks != other.errCallbacks -> false
            chain != other.chain -> false
            else -> true
        }
    }

    override fun toString(): String {
        return "Body(args = [${args.joinToString(", ") }], " +
                "callbacks = [${callbacks.joinToString(", ") }], " +
                "errCallbacks = [${errCallbacks.joinToString(", ")}], " +
                "chord = $chord)"
    }
}

class Signature (
    val body: SignatureBody,
    val headers: SignatureHeaders,
    val properties: SignatureProperties
) {
    fun toTask(objectMapper: ObjectMapper, taskFunc: KFunction<Unit>): Task {
        val rfunc = taskFunc.reflect() ?: throw Exception("Task func $taskFunc could not call reflect.")

        val jsonArgs = objectMapper.writeValueAsBytes(body.args)
        val jsonArray: List<JsonNode> = objectMapper.readValue(jsonArgs)

        val args = rfunc.parameters.toList().mapIndexed { idx, param ->
            if (idx > body.args.size) {
                  Any()
            } else {
                objectMapper.treeToValue(jsonArray[idx], param.type.jvmErasure.java)
            }
        }

        return Task(taskFunc, *args.toTypedArray())
    }

    fun copy(
        body: SignatureBody = this.body,
        headers: SignatureHeaders = this.headers,
        properties: SignatureProperties = this.properties
    ): Signature {
        return Signature(body, headers, properties)
    }

    override fun equals(other: Any?): Boolean {
        if (other == null) return false

        if (other !is Signature) return false

        return when {
            body != other.body -> false
            headers != other.headers -> false
            properties != other.properties -> false
            else -> true
        }
    }

    override fun toString(): String {
        return "Signature(body = $body, headers = $headers, properties = $properties)"
    }
}

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

fun chain(
    vararg tasks: Signature,
    callbacks: List<Signature> = listOf(),
    errCallbacks: List<Signature> = listOf()
): Signature {
    assert(tasks.isNotEmpty())

    val rootId = tasks[0].headers.id

    val tasksCbs = tasks.mapIndexed { idx, t ->
        val cb = if (idx < tasks.size-1) {
            listOf(tasks[idx+1]) // Each task will get the next task as its callback
        } else {
            callbacks // The last task will get the callbacks supplied to the chain
        }

        t.copy(
            headers = t.headers.copy(rootId = rootId),
            // All tasks must execute the error callbacks if they fail
            body = t.body.copy(callbacks = cb, errCallbacks = errCallbacks)
        )
    }

    return tasksCbs.first()


    // Any task in the chain may fail, so they all need the error callbacks
//    val tasksCbs = tasks.map {
//        it.copy(
//            headers = it.headers.copy(rootId = it.headers.rootId),
//            body = it.body.copy(errCallbacks = errCallbacks)
//        )
//    }

    // Last task gets the success callbacks
//    val last = tasksCbs.last().let { it.copy(body = it.body.copy(callbacks = callbacks)) }
//    return root.copy(body = root.body.copy(chain = tasksCbs.take(tasksCbs.size-1) + last))
}

/**
 * Signatures are the wire format for tasks.
 */
//@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
//@JsonSubTypes(value = [
//    JsonSubTypes.Type(value = DecodeTaskSignature::class, name = "task"),
//    JsonSubTypes.Type(value = TaskSignature::class, name = "task")
////    JsonSubTypes.Type(value = BarData.class, name = "group"),
////    JsonSubTypes.Type(value = BarData.class, name = "chain")
//])
//abstract class BaseSignature(
//    val type: String,
//    val groupId: String?
//)
//
//
//abstract class BaseTaskSignature(
//    val id: String,
//    val taskName: String,
//    groupId: String?
//) : BaseSignature(type = "task", groupId = groupId)
//
//class TaskSignature(
//    id: String,
//    taskName: String,
//    val taskArguments: List<Any>,
//    groupId: String?
//) : BaseTaskSignature(id = id, taskName = taskName, groupId = groupId)
//
//class DecodeTaskSignature(
//    id: String,
//    taskName: String,
//    val taskArguments: List<JsonNode>,
//    groupId: String?
//) : BaseTaskSignature(id = id, taskName = taskName, groupId = groupId) {
//
//    fun toTask(objectMapper: ObjectMapper, taskFunc: Any): Task {
//        val meth = taskFunc::class.java.methods[0] // Better way to get the method?
//
//        val args = meth.parameters.mapIndexed { idx, param ->
//            if (idx > taskArguments.size) {
//                param.type.newInstance() // Add zero value of the instance
//            } else {
//                objectMapper.treeToValue(taskArguments[idx], param.type)
//            }
//        }
//
//        return Task(taskFunc, *args.toTypedArray())
//    }
//
//}
//
//
