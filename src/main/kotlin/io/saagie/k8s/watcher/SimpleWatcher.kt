package io.saagie.k8s.watcher

import arrow.core.None
import arrow.core.Option
import arrow.data.State
import com.squareup.okhttp.ResponseBody
import io.kubernetes.client.ApiClient
import java.lang.reflect.Type

data class SimpleWatcherData(
    val apiClient: ApiClient,
    val url: String,
    val type: Type,
    val response: Option<ResponseBody> = None
)


fun <T> callApi(): State<SimpleWatcherData, WatchEvent<T>> = State {

}