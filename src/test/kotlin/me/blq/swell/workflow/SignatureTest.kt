package me.blq.swell.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.Assert
import org.junit.Test

@Suppress("UNUSED_PARAMETER")
class SignatureTest {
    @Test
    fun `test decode signature decodes arguments`() {
        val objectMapper = ObjectMapper()
        objectMapper.findAndRegisterModules()

        var called = false
        fun testFunc(arg1: String, arg2: Int) {
            called = true
        }

        val sig = task("foo", "foo", 2)
        val task = sig.toTask(objectMapper, ::testFunc)

        Assert.assertEquals(::testFunc, task.function)
        task.invoke()

        Assert.assertEquals(true, called)
    }

    @Test
    fun `test signature decoded from json works`() {
        val objectMapper = ObjectMapper()
        objectMapper.findAndRegisterModules()

        var called = false
        fun testFunc(arg1: String, arg2: Int) {
            called = true
        }

        val jsonSig = objectMapper.writeValueAsBytes(task("foo", "foo", 2))
        val sig: Signature = objectMapper.readValue(jsonSig)
        val task = sig.toTask(objectMapper, ::testFunc)
        Assert.assertEquals(::testFunc, task.function)
        task.invoke()

        Assert.assertEquals(true, called)
    }

    data class TestObject(
        val foo: String
    )

    @Test
    fun `test signature with complex type works`() {
        val objectMapper = ObjectMapper()
        objectMapper.findAndRegisterModules()

        var called = false
        fun testFunc(stringArg: String, testObjectArg: TestObject) {
            called = true
        }

        val jsonSig = objectMapper.writeValueAsBytes(task("foo", "foo", TestObject(foo = "bar")))
        val sig: Signature = objectMapper.readValue(jsonSig)
        val task = sig.toTask(objectMapper, ::testFunc)

        Assert.assertEquals(::testFunc, task.function)
        task.invoke()

        Assert.assertEquals(true, called)
    }

    @Test
    fun `test task method creates expected signature`() {
        var sig = task("foo", 1, "bar")

        var expected = Signature(
            body = SignatureBody(args = listOf(1, "bar")),
            headers = SignatureHeaders(task = "foo", id = sig.headers.id, rootId = sig.headers.id),
            properties = SignatureProperties(sig.properties.correlationId)
        )
        Assert.assertEquals(expected, sig)

        // Signature with callbacks

        sig = task("foo", 1, "bar", callbacks = listOf(
            task("foo2", 1)
        ))

        expected = Signature(
            body = SignatureBody(args = listOf(1, "bar"), callbacks = listOf(
                Signature(
                    body = SignatureBody(args = listOf(1)),
                    headers = SignatureHeaders(task = "foo2", id = sig.body.callbacks[0].headers.id, rootId = sig.body.callbacks[0].headers.id),
                    properties = SignatureProperties(sig.body.callbacks[0].properties.correlationId)
                )
            )),
            headers = SignatureHeaders(task = "foo", id = sig.headers.id, rootId = sig.headers.id),
            properties = SignatureProperties(sig.properties.correlationId)
        )

        Assert.assertEquals(expected, sig)
    }

    @Test
    fun `test chain`() {
        val sig = chain(
            task("foo1", 1, "b"),
            task("foo2", 2),
            task("foo3", 3),
            callbacks = listOf(task("callback", 1)),
            errCallbacks = listOf(task("errCallback", 1))
        )
        println(sig)
        val objectMapper = ObjectMapper()
        objectMapper.findAndRegisterModules()
        println(objectMapper.writeValueAsString(sig))
    }
}
