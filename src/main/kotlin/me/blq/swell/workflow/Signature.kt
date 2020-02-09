package me.blq.swell.workflow

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.treeToValue
import java.time.ZonedDateTime
import java.util.*
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.jvm.reflect


@JsonInclude(JsonInclude.Include.NON_NULL)
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

@JsonInclude(JsonInclude.Include.NON_NULL)
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

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonSerialize(using = SignatureBodySerializer::class)
class SignatureBody(
    val args: List<Any>,
//    val kwargs: ObjectNode,
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    val callbacks: List<Signature> = listOf(),
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    val errCallbacks: List<Signature> = listOf(),
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
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

class SignatureBodySerializer(t: Class<SignatureBody>) : StdSerializer<SignatureBody>(t) {
    constructor() : this(SignatureBody::class.java)

    override fun serialize(body: SignatureBody, gen: JsonGenerator, provider: SerializerProvider) {
        gen.writeStartArray()
        gen.writeObject(body.args)
        gen.writeObject(mapOf<String, Any>())
        gen.writeObject(mapOf(
            "callbacks" to body.callbacks,
            "errbacks" to body.errCallbacks,
            "chain" to body.chain
        ))
        gen.writeEndArray()
    }
}

class SignatureBodyDeserializer(private val objectMapper: ObjectMapper, t: Class<SignatureBody>) : StdDeserializer<SignatureBody>(t) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): SignatureBody {
        val arrNode = p.readValueAsTree<ArrayNode>()
        if (arrNode.size() != 3) throw JsonParseException(p, "Signature body expected to have 3 elements.")

        val args = objectMapper.treeToValue<List<Any>>(arrNode.get(0))
        val callbacks = objectMapper.treeToValue<List<Signature>>(arrNode.get(1))
        return SignatureBody(args, )
    }
}

@JsonInclude(JsonInclude.Include.NON_NULL)
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
