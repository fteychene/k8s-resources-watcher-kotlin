package io.saagie.k8sresourceswatcher

import arrow.core.None
import arrow.core.Option
import arrow.core.Try
import arrow.core.getOrElse
import arrow.data.run
import com.google.gson.JsonParser
import com.google.gson.JsonSyntaxException
import com.google.gson.reflect.TypeToken
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import com.squareup.okhttp.*
import com.squareup.okhttp.mockwebserver.MockResponse
import com.squareup.okhttp.mockwebserver.MockWebServer
import io.kotlintest.assertions.arrow.`try`.shouldBeFailure
import io.kotlintest.assertions.arrow.`try`.shouldBeSuccess
import io.kotlintest.assertions.arrow.option.shouldBeNone
import io.kotlintest.assertions.arrow.option.shouldBeSome
import io.kotlintest.fail
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.matchers.haveSize
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kubernetes.client.ApiClient
import io.kubernetes.client.ApiException
import io.kubernetes.client.models.*
import io.saagie.k8sresourceswatcher.WatchItem.Error
import io.saagie.k8sresourceswatcher.WatchItem.WatchResponse
import okio.BufferedSource
import org.joda.time.DateTime
import org.junit.Rule
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.SocketTimeoutException
import java.util.*

class ResourceWatcherTest {

    @Rule
    @JvmField
    var server = MockWebServer()

    val apiClient = ApiClient().setBasePath(server.url("").toString().dropLast(1))

    @Test
    fun `checkResponse should be ok`() {
        val expected = Response.Builder()
            .request(Request.Builder().url("http://fakekube/apis/mock").build())
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .body(ResponseBody.create(MediaType.parse("application/json"), "TestBody"))
            .build()
        val actual = checkResponse(expected)
        actual.shouldBeSuccess(expected)
    }

    @Test
    fun `checkResponse should fail without body in response`() {
        val expected = Response.Builder()
            .request(Request.Builder().url("http://fakekube/apis/mock").build())
            .protocol(Protocol.HTTP_1_1)
            .code(401)
            .message("Unauthorized")
            .build()
        val actual = checkResponse(expected)
        actual.shouldBeFailure()
        actual.failed().map {
            when (it) {
                is ApiException -> {
                    it.code shouldBe 401
                    it.message shouldBe "Unauthorized"
                }
                else -> fail("Wrong type of exception : ${it::javaClass.name}")
            }
        }
    }

    @Test
    fun `checkResponse should fail on invalid code and string body`() {
        val expected = Response.Builder()
            .request(Request.Builder().url("http://fakekube/apis/mock").build())
            .protocol(Protocol.HTTP_1_1)
            .code(401)
            .message("Unauthorized")
            .body(ResponseBody.create(MediaType.parse("application/json"), "TestBody"))
            .build()
        val actual = checkResponse(expected)
        actual.shouldBeFailure()
        actual.failed().map {
            when (it) {
                is ApiException -> {
                    it.code shouldBe 401
                    it.message shouldBe "Unauthorized"
                    it.responseBody shouldBe "TestBody"
                }
                else -> fail("Wrong type of exception : ${it::javaClass.name}")
            }
        }
    }

    @Test
    fun `parseData should parse content`() {
        val expected = "Test content"
        val json = """{"object": "${expected}"}"""
        val actual = parseData<String>(
            JsonParser().parse(json).asJsonObject,
            apiClient.json.gson,
            object : TypeToken<String>() {}.type
        )
        actual.shouldBeSuccess(expected)
    }

    @Test
    fun `parseData should fail parse content`() {
        val json = """{"object": "String content should be int"}"""
        val actual = parseData<Int>(
            JsonParser().parse(json).asJsonObject,
            apiClient.json.gson,
            object : TypeToken<Int>() {}.type
        )
        actual.shouldBeFailure()
        actual.failed().map { it should beInstanceOf<JsonSyntaxException>() }
    }

    @Test
    fun `parseData should fail parse missing object content`() {
        val json = """{"type":"ADD"}"""
        val actual = parseData<Int>(
            JsonParser().parse(json).asJsonObject,
            apiClient.json.gson,
            object : TypeToken<Int>() {}.type
        )
        actual.shouldBeFailure()
        actual.failed().map { it should beInstanceOf<IllegalArgumentException>() }
    }

    @Test
    fun `parseType should parse content`() {
        val expected = "ADD"
        val json = """{"type":"${expected}", "object": "coucou"}"""
        val actual = parseResponseType(JsonParser().parse(json).asJsonObject)
        actual.shouldBeSuccess(expected)
    }

    @Test
    fun `parseType should fail parse content`() {
        val json = """{"type": null}"""
        val actual = parseResponseType(JsonParser().parse(json).asJsonObject)
        actual.shouldBeFailure()
        actual.failed().map { it should beInstanceOf<UnsupportedOperationException>() }

        val jsonNull = """{"obect": "coucou"}"""
        val actualNull = parseResponseType(JsonParser().parse(jsonNull).asJsonObject)
        actualNull.shouldBeFailure()
        actualNull.failed().map { it should beInstanceOf<IllegalStateException>() }
    }

    @Test
    fun `readBody should return error if responseBody misses data`() {
        val response = getResponseForBody<V1Pod>("""{"type": "ADD"}""")
        response should beInstanceOf<WatchItem.Error<V1Pod>>()
        (response as WatchItem.Error<V1Namespace>).exception should beInstanceOf<IllegalArgumentException>()
    }

    @Test
    fun `readBody should return error if responseBody misses type`() {
        val response = getResponseForBody<V1Namespace>("""{"object": "Hello world"}""")
        response should beInstanceOf<WatchItem.Error<V1Namespace>>()
        (response as WatchItem.Error<V1Namespace>).exception should beInstanceOf<IllegalStateException>()
    }


    @Test
    fun `readBody should return next response from body and register resource version`() {
        val namespace = V1Namespace()
            .spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("0"))
        val namespace1 = V1Namespace().spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("1").name("pwet-${UUID.randomUUID()}"))

        val body = ResponseBody.create(
            MediaType.parse("application/json"),
            """${watchResponseJson("ADD", namespace)}
                        ${watchResponseJson("MODIFIED", namespace1)}
                    """.trimIndent()
        )

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )

        val readBody = readBody<V1Namespace>()
        val (state1, response1) = readBody.run(state0)
        state1.resourceVersion.shouldBeSome("0")
        response1 shouldBe WatchResponse("ADD", namespace)

        val (state2, response2) = readBody.run(state1)
        state2.resourceVersion.shouldBeSome("1")
        response2 shouldBe WatchResponse("MODIFIED", namespace1)
    }

    @Test
    fun `readBody should return next response from body and can't register resource version`() {

        val namespace = V1Namespace()
            .spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-"))
        val namespace1 = V1Namespace().spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").name("pwet-${UUID.randomUUID()}"))

        val body = ResponseBody.create(
            MediaType.parse("application/json"),
            """
                    ${watchResponseJson("ADD", namespace)}
                    ${watchResponseJson("MODIFIED", namespace1)}
                """.trimIndent()
        )

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )

        val readBody = readBody<V1Namespace>()
        val (state1, response1) = readBody.run(state0)
        state1.resourceVersion.shouldBeNone()
        response1 shouldBe WatchResponse("ADD", namespace)

        val (state2, response2) = readBody.run(state1)
        state2.resourceVersion.shouldBeNone()
        response2 shouldBe WatchResponse("MODIFIED", namespace1)
    }

    @Test
    fun `readBody should NoData on timeout in body`() {
        val mock: BufferedSource = mock()
        whenever(mock.readUtf8Line()).thenThrow(SocketTimeoutException("Server timeout"))
        val readBody = readBody<V1Namespace>()
        val body = ResponseBody.create(MediaType.parse("application/json"), 0, mock)


        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (state1, response1) = readBody.run(state0)
        response1 should beInstanceOf<WatchItem.NoData<V1Namespace>>()
        state1.body shouldBe state0.body
    }

    @Test
    fun `readBody should Error on terminate body`() {
        val readBody = readBody<V1Namespace>()
        val body = ResponseBody.create(MediaType.parse("application/json"), "")

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (state1, response1) = readBody.run(state0)
        response1 should beInstanceOf<WatchItem.Error<V1Namespace>>()
        (response1 as WatchItem.Error<V1Namespace>).exception should beInstanceOf<IOException>()
        state1.body shouldBe state0.body
    }

    @Test
    fun `readBody should Error on invalid json object body`() {
        val readBody = readBody<V1Namespace>()
        val body = ResponseBody.create(MediaType.parse("application/json"), "INVALID CONTENT")

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (state1, response1) = readBody.run(state0)
        response1 should beInstanceOf<WatchItem.Error<V1Namespace>>()
        (response1 as WatchItem.Error<V1Namespace>).exception should beInstanceOf<JsonSyntaxException>()
        state1.body shouldBe state0.body
    }

    @Test
    fun `readBody should Error on invalid WatchResponse json body`() {
        val readBody = readBody<V1Namespace>()
        val body = ResponseBody.create(
            MediaType.parse("application/json"),
            """
                    {"object": "Hello world"}
                    {"type": "ADD"}
                """.trimIndent()
        )

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (state1, response1) = readBody.run(state0)
        response1 should beInstanceOf<WatchItem.Error<V1Namespace>>()
        (response1 as WatchItem.Error<V1Namespace>).exception should beInstanceOf<IllegalStateException>()
        state1.body shouldBe state0.body

        val (state2, response2) = readBody.run(state1)
        response2 should beInstanceOf<WatchItem.Error<V1Namespace>>()
        (response2 as WatchItem.Error<V1Namespace>).exception should beInstanceOf<IllegalArgumentException>()
        state2.body shouldBe state0.body
    }


    @Test
    fun `readBody should recover on invalid resourceVersion`() {
        val readBody = readBody<V1Namespace>()
        val body = ResponseBody.create(
            MediaType.parse("application/json"),
            """{"type":"ERROR","object":{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"too old resource version: 3981707 (3987044)","reason":"Gone","code":410}}"""
        )
        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (actualState, response1) = readBody.run(state0)

        response1 should beInstanceOf<WatchItem.InvalidResourceVersion<V1Namespace>>()
        (response1 as WatchItem.InvalidResourceVersion<V1Namespace>).newVersion.shouldBeSome("3987044")

        actualState.body.shouldBeFailure()
        actualState.resourceVersion.shouldBeSome("3987044")

    }

    @Test
    fun `responseBody should return body on valid response`() {
        val responseBody = responseBody()
        val body =
            ResponseBody.create(MediaType.parse("application/json"), """{"type": "ADD", "object": "Stringevent"}""")
        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (actualState, resultBody) = responseBody.run(state0)
        actualState.body.shouldBeSuccess(body)
        resultBody.shouldBeSuccess(body)
    }

    @Test
    fun `responseBody should fail body on exhausted body`() {
        val responseBody = responseBody()
        val body = ResponseBody.create(MediaType.parse("application/json"), "")
        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        val (actualState, resultBody) = responseBody.run(state0)
        actualState.body.shouldBeFailure()
        resultBody.shouldBeFailure()
    }

    @Test
    fun `responseBody should recall Kubernetes on failure body`() {
        val responseBody = responseBody()
        val namespace = V1Namespace()
            .spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("0"))


        server.willReturnResponse(
            """
            ${watchResponseJson("ADD", namespace)}
        """.trimIndent()
        )

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.raise(SocketTimeoutException("Timeout"))
        )
        val (actualState, resultBody) = responseBody.run(state0)
        actualState.body.shouldBeSuccess()
        resultBody.shouldBeSuccess()
    }

    @Test
    fun `watchResource should fail on wrong Kubernetes call`() {
        server.enqueue(MockResponse().setResponseCode(403).setBody("Unauthorized"))
        val actual = watchResource<V1Namespace>("/apis/mock", apiClient)
        actual.shouldBeFailure()
    }

    @Test
    fun `watchResource should sucess on corrent Kubernetes call`() {
        server.willReturnResponse("")
        val actual = watchResource<V1Namespace>("/apis/mock", apiClient)
        actual.shouldBeSuccess()
    }

    @Test
    fun `watchResource should create a sequence`() {
        val namespace = V1Namespace()
            .spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("0"))
        val namespace1 = V1Namespace().spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("1").name("pwet-${UUID.randomUUID()}"))
        val namespace2 = V1Namespace().spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("2").name("pwet-${UUID.randomUUID()}"))
        val namespace3 = V1Namespace().spec(V1NamespaceSpec())
            .metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("3").name("pwet-${UUID.randomUUID()}"))


        server.willReturnResponse(
            """
            ${watchResponseJson("ADD", namespace)}
            ${watchResponseJson("MODIFIED", namespace1)}
            ${watchResponseJson("MODIFIED", namespace2)}
            ${watchResponseJson("MODIFIED", namespace3)}
        """.trimMargin()
        )

        val actual = watchResource<V1Namespace>("/apis/mock", apiClient)
        actual.shouldBeSuccess()

        actual.map {
            it.take(4).toList() shouldBe listOf(
                WatchResponse("ADD", namespace),
                WatchResponse("MODIFIED", namespace1),
                WatchResponse("MODIFIED", namespace2),
                WatchResponse("MODIFIED", namespace3)
            )
        }
    }

    @Test
    fun `watchResource should not break sequence on invalid JSON`() {
        val namespace = V1Namespace().spec(V1NamespaceSpec()).metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("0"))
        val namespace1 = V1Namespace().spec(V1NamespaceSpec()).metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("1").name("pwet-${UUID.randomUUID()}"))
        val namespace2 = V1Namespace().spec(V1NamespaceSpec()).metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("2").name("pwet-${UUID.randomUUID()}"))
        val namespace3 = V1Namespace().spec(V1NamespaceSpec()).metadata(V1ObjectMeta().namespace("namespacepwet").generateName("pwet-").resourceVersion("3").name("pwet-${UUID.randomUUID()}"))

        server.willReturnResponse(
            """
            ${watchResponseJson("ADD", namespace)}
            {"status":"ADD, object:"INVALID"}
            ${watchResponseJson("MODIFIED", namespace1)}
            "{"
            ${watchResponseJson("MODIFIED", namespace2)}
            "}"
            ${watchResponseJson("MODIFIED", namespace3)}
        """.trimMargin()
        )

        val actual = watchResource<V1Namespace>("/apis/mock", apiClient)
        actual.shouldBeSuccess()

        val actualValue = actual.map { it.take(7).toList() }.getOrElse { listOf() }

        actualValue.filter { it is WatchResponse } shouldBe listOf(
            WatchResponse("ADD", namespace),
            WatchResponse("MODIFIED", namespace1),
            WatchResponse("MODIFIED", namespace2),
            WatchResponse("MODIFIED", namespace3)
        )

        actualValue.filter { it is Error } should haveSize(3)
    }

    fun <T> watchResponseJson(type: String, data: T, status: Option<V1Status> = None): String =
        """{"type":"${type}","object":${apiClient.json.serialize(data)}${status.map(apiClient.json::serialize).map { ""","status":$it""" }.getOrElse { "" }}}"""

    fun MockWebServer.willReturnResponse(response: String) = this.enqueue(MockResponse().setBody(response))

    fun V1ObjectMeta.copy(
        annotations: Map<String, String>? = null,
        clusterName: String? = null,
        creationTimestamp: DateTime? = null,
        deletionGracePeriodSeconds: Long? = null,
        deletionTimestamp: DateTime? = null,
        finalizers: List<String>? = null,
        generateName: String? = null,
        generation: Long? = null,
        initializers: V1Initializers? = null,
        labels: Map<String, String>? = null,
        name: String? = null,
        namespace: String? = null,
        ownerReferences: List<V1OwnerReference>? = null,
        resourceVersion: String? = null,
        selfLink: String? = null,
        uid: String? = null
    ) =
        V1ObjectMeta()
            .annotations(annotations ?: this.annotations)
            .clusterName(clusterName ?: this.clusterName)
            .creationTimestamp(creationTimestamp ?: this.creationTimestamp)
            .deletionGracePeriodSeconds(deletionGracePeriodSeconds ?: this.deletionGracePeriodSeconds)
            .deletionTimestamp(deletionTimestamp ?: this.deletionTimestamp)
            .finalizers(finalizers ?: this.finalizers)
            .generateName(generateName ?: this.generateName)
            .generation(generation ?: this.generation)
            .initializers(initializers ?: this.initializers)
            .labels(labels ?: this.labels)
            .name(name ?: this.name)
            .namespace(namespace ?: this.namespace)
            .ownerReferences(ownerReferences ?: this.ownerReferences)
            .resourceVersion(resourceVersion ?: this.resourceVersion)
            .selfLink(selfLink ?: this.selfLink)
            .uid(uid ?: this.uid)

    private fun <T> getResponseForBody(bodyContent: String): WatchItem<T> {
        val body = ResponseBody.create(MediaType.parse("application/json"), bodyContent)

        val state0 = Watcher(
            "",
            apiClient,
            object : TypeToken<V1Namespace>() {}.type,
            apiClient.json.gson,
            None,
            None,
            None,
            Try.just(body)
        )
        return readBody<T>().run(state0).b
    }
}