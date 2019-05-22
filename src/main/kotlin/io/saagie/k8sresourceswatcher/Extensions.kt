package io.saagie.k8sresourceswatcher

import arrow.core.None
import arrow.core.Option
import arrow.core.toOption
import com.squareup.okhttp.Call
import com.squareup.okhttp.MediaType
import com.squareup.okhttp.Request
import com.squareup.okhttp.RequestBody
import com.squareup.okhttp.internal.http.HttpMethod
import io.kubernetes.client.ApiClient
import io.kubernetes.client.Pair
import io.kubernetes.client.models.*

const val APPLICATION_JSON = "application/json; charset=utf-8"


fun <T> resourceVersion(resource: T): Option<String> =
    when (resource) {
        is V1Pod -> resource.metadata.resourceVersion.toOption()
        is V1Event -> resource.metadata.resourceVersion.toOption()
        is V1ConfigMap -> resource.metadata.resourceVersion.toOption()
        is V1beta1CronJob -> resource.metadata.resourceVersion.toOption()
        is V1Namespace -> resource.metadata.resourceVersion.toOption()
        else -> None
    }

fun toParams(vararg params: Option<kotlin.Pair<String, String>>): List<kotlin.Pair<String, String>> =
    params.fold(listOf(), { acc, option -> option.fold({ acc }, { acc + it }) })

fun buildApiPath(basePath: String, params: List<kotlin.Pair<String, String>>): String =
    params.fold(basePath to "?", { (url, sep), (key, value) -> "$url$sep$key=$value" to "&" }).first


fun <T> ApiClient.buildCall(
    path: String,
    method: String,
    body: T,
    queryParams: List<Pair> = listOf(),
    collectionQueryParams: List<Pair> = listOf(),
    headerParams: Map<String, String> = mapOf(),
    formParams: Map<String, String> = mapOf(),
    authNames: List<String> = listOf()
): Call = httpClient.newCall(
    buildRequest(path, method, body, queryParams, collectionQueryParams, headerParams, formParams, authNames)
)

fun <T> ApiClient.buildRequest(
    path: String,
    method: String,
    body: T,
    queryParams: List<Pair> = listOf(),
    collectionQueryParams: List<Pair> = listOf(),
    headerParams: Map<String, String> = mapOf(),
    formParams: Map<String, String> = mapOf(),
    authNames: List<String> = listOf()
): Request {
    updateParamsForAuth(authNames.toTypedArray(), queryParams, headerParams)

    val url = buildUrl(path, queryParams, collectionQueryParams)
    val reqBuilder = Request.Builder().url(url)
    processHeaderParams(headerParams, reqBuilder)

    val contentType: String = headerParams["Content-Type"] ?: APPLICATION_JSON

    val reqBody: RequestBody?
    if (!HttpMethod.permitsRequestBody(method)) {
        reqBody = null
    } else if ("application/x-www-form-urlencoded" == contentType) {
        reqBody = buildRequestBodyFormEncoding(formParams)
    } else if ("multipart/form-data" == contentType) {
        reqBody = buildRequestBodyMultipart(formParams)
    } else if (body == null) {
        if ("DELETE" == method) {
            reqBody = null
        } else {
            reqBody = RequestBody.create(MediaType.parse(contentType), "")
        }
    } else {
        reqBody = serialize(body, contentType)
    }

    return reqBuilder.method(method, reqBody).build()
}

fun ApiClient.buildGetCall(url: String, headers: Map<String, String> = mapOf()): Call =
    buildCall(
        path = url,
        method = "GET",
        body = Object(),
        headerParams = mapOf(
            "Accept" to APPLICATION_JSON,
            "Content-Type" to APPLICATION_JSON
        ) + headers,
        authNames = listOf("BearerToken")
    )
