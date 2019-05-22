package io.saagie.k8sresourceswatcher

import arrow.core.*
import arrow.data.State
import arrow.data.fix
import arrow.data.run
import arrow.instances.ForState
import arrow.instances.extensions
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.reflect.TypeToken
import com.squareup.okhttp.Call
import com.squareup.okhttp.Response
import com.squareup.okhttp.ResponseBody
import io.kubernetes.client.ApiClient
import io.kubernetes.client.ApiException
import io.kubernetes.client.models.V1Status
import io.saagie.k8sresourceswatcher.WatchItem.*
import java.io.IOException
import java.lang.reflect.Type
import java.net.SocketTimeoutException

sealed class WatchItem<T> {

    /** Sent when an unexpected error occurs during watch */
    data class Error<T>(val exception: Throwable) : WatchItem<T>()

    /** Response sent when a timeout occurs because no data has been sent for a long time */
    class NoData<T> : WatchItem<T>()

    class InvalidResourceVersion<T>(val newVersion: Option<String>) : WatchItem<T>()

    data class WatchResponse<T>(val kind: String, val data: T) : WatchItem<T>()
}

data class Watcher(
    val baseUrl: String,
    val apiClient: ApiClient,
    val type: Type,
    val serializer: Gson = Gson(),
    val resourceVersion: Option<String> = None,
    val fieldSelector: Option<String> = None,
    val labelSelector: Option<String> = None,
    val body: Try<ResponseBody> = Try.raise(IllegalStateException("Watcher have not been executed, no call done"))
)

/**
 * Verify if the Call Response is valid or not and resposne a corresponding Try
 */
fun checkResponse(response: Response): Try<Response> =
    if (response.isSuccessful) Try.just(response)
    else {
        val body = response.body().toOption()
        Try.raise(
            when (body) {
                is Some -> Try { response.body().string() }.fold(
                    { ApiException(response.message(), it, response.code(), response.headers().toMultimap()) },
                    { ApiException(response.message(), response.code(), response.headers().toMultimap(), it) }
                )
                is None -> ApiException(response.message(), response.code(), response.headers().toMultimap(), null)
            })
    }

/**
 * Execute a Call, check it's a valid execution and return the Response body
 */
fun executeCall(call: Call): Try<ResponseBody> =
    Try { call.execute() }
        .flatMap(::checkResponse)
        .map { it.body() }


// Json parsing

/**
 * Parse "data" from JsonObject to specific Type
 */
fun <T> parseData(json: JsonObject, serializer: Gson, type: Type): Try<T> = Try {
    serializer.fromJson<T>(json.get("object"), type)
        ?: throw IllegalArgumentException("Json object should have an object field")
}

/**
 * Read "type" from JsonObject
 */
fun parseResponseType(json: JsonObject): Try<String> = Try { json.get("type").asString }


private fun V1Status.extractResourceVersion() =
    Regex("""too old resource version: \d* \((\d*)\)""")
        .find(this.message).toOption()
        .map { it.groupValues[1] }

/**
 * Read next response in body and return the new state and Try from parsed result
 *  - Check if the Watcher state have a current body, Failure with IOException if not
 *  - Read the next line from ResponseBody, Failure with IOException if not (body should be test if exhausted before, this function target is to read and parse body safely only)
 *  - Parse the next line in json
 *  - Try to read the resourceVersion of the Data if possible and set it ot the state
 */
fun <T> readBody() = State<Watcher, WatchItem<T>> { state ->
    state.body
        .flatMap { Try { it.source().readUtf8Line() ?: throw IOException("Null response from the server.") } }
        .flatMap { Try { JsonParser().parse(it).asJsonObject } }
        .flatMap { body ->
            parseResponseType(body).flatMap { responseType ->
                when (responseType) {
                    "ERROR" -> parseData<V1Status>(body, state.serializer, V1Status::class.java)
                        .map { InvalidResourceVersion<T>(it.extractResourceVersion()) }
                    else -> parseData<T>(body, state.serializer, state.type)
                        .map { WatchResponse(responseType, it) }
                }
            }
        }.fold(
            {
                state toT when (it) {
                    is SocketTimeoutException -> NoData()
                    else -> Error(it)
                }
            },
            { response ->
                when (response) {
                    is WatchResponse -> state.copy(resourceVersion = resourceVersion(response.data).or(state.resourceVersion)) toT response
                    is WatchItem.InvalidResourceVersion -> state.copy(
                        resourceVersion = response.newVersion,
                        body = Try.raise(IllegalStateException("Outdated body (invalid resourceversion)"))
                    ) toT response
                    else -> state toT response
                }
            }
        )
}


/**
 * Read the state to check if the body exists and is still valid and run the Api call if needed
 *  - Check if state contains a ResponseBody and make the Kubernetes Api call if not
 *  - Check if the body is exhausted
 */
fun responseBody() = State<Watcher, Try<ResponseBody>> { state ->
    val computedBody = ForTry extensions {
        // Make call if the body is invalid
        val body = state.body.recoverWith { executeCall(buildWatchCall(state)) }
        // Test the source body is not exhausted
        body.product(body.flatMap { Try { it.source().exhausted() } }).fix()
            .filter { (_, exausthed) -> exausthed == false }
            .map { it.a }.fix()
    }

    state.copy(body = computedBody) toT computedBody
}

/**
 * Read the reponse body from state, reopenthe connection if needed and try to read the next event
 */
fun <T> readNextResponse() =
    ForState<Watcher>() extensions {
        binding {
            responseBody().bind()
            readBody<T>().bind()
        }.fix()
    }

/**
 * Build the Call for the Watcher context
 */
fun buildWatchCall(watcher: Watcher): Call {
    val url = buildApiPath(
        watcher.baseUrl,
        toParams(("watch" to "true").some(),
            watcher.resourceVersion.map { "resourceVersion" to it },
            watcher.fieldSelector.map { "fieldSelector" to it },
            watcher.labelSelector.map { "labelSelector" to it })
    )
    return watcher.apiClient.buildGetCall(url, headers = mapOf("connection" to "keep-alive"))
}

/**
 * Watch a Kubernetes ressources. Generate a {@code Sequence } of {@code Try<WatchResponse<T>> } where {@code T } is the resource type watched.
 *
 * Optional paraméters :
 *  * fieldSelector: {@code Option<String> } = Field selector for watch query, default {@code None }
 *  * resourceVersion: {@code Option<String> } = Ressource version to begin watch, default {@code None }
 *  * onApiError: {@code (Throwable, Watcher) -> Unit } = Callback on APi call errors
 *
 *  Usage for all events for specific type (warn infinite sequence):
 *  <pre>
 *  {@code
 *  watchResource<V1Pod>(apiClient).forEach {
 *      when (it) {
 *          is Failure -> it.exception.printStackTrace()
 *          is Success -> println(it.value)
 *      }
 *  }
 *  }
 *  </pre>
 *  To stop {@code forEach } you can use a {@code return } statement
 *
 *  Search a specific element :
 *  <pre>
 *  {@code
 *  val podWithLabel = watchResource<V1Pod>(apiClient)
 *    .filter { it.fold( {false}, { it.data.metadata.labels.getOrDefault("completed", false) == true }) }
 *    .take(1)
 *    .elementAt(0)
 *  }
 *  </pre>
 *
 *  Integration with Reactor:
 *  <pre>
 *  {@code
 *  Flux.from(watchResource<V1Pod>("/api/v1/pods", apiClient, resourceVersion = "473".some()).asPublisher())
 *    .subscribe(
 *      { println(it) },
 *      { println("Error on subscriber"); it.printStackTrace() }
 *    )
 *  }
 *  </pre>
 *
 *
 *  Returns a lazy evaluated {@code Sequence } it's on the client side to manage the rhythm for http consumption.
 *
 *  Example to wait 2min before calling again the api on socket timeout
 *  <pre>
 *  {@code
 *  watchResource<V1Pod>("/api/v1/pods", apiClient, resourceVersion = "473".some())
 *  .forEach {
 *      it.fold(
 *          {
 *              it.printStackTrace()
 *              if (it is IOException && it.cause is SocketTimeoutException) {
 *                  Thread.sleep(2000L)
 *              }
 *          },
 *          ::println
 *      )
 *  }
 *  }
 *  </pre>
 *
 *
 */
inline fun <reified T> watchResource(
    baseUrl: String,
    apiClient: ApiClient,
    fieldSelector: Option<String> = None,
    resourceVersion: Option<String> = None,
    labelSelector: Option<String> = None
): Try<SequenceOfWatchItem<T>> {
    val token = object : TypeToken<T>() {}.type
    val readNextResponse = readNextResponse<T>()
    val context = Watcher(baseUrl, apiClient, token, apiClient.json.gson, resourceVersion, fieldSelector, labelSelector)
    return executeCall(buildWatchCall(context)).map { context.copy(body = Try.just(it)) }
        .map { initialContext ->
            generateSequence({ readNextResponse.run(initialContext) }) { (context, _) ->
                readNextResponse.run(context)
            }.map { it.b }
        }
}

typealias SequenceOfWatchItem<T> = Sequence<WatchItem<T>>
