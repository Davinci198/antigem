package com.example.gemini.data.agent.hermes

import android.app.Application
import android.util.Log
import com.example.gemini.data.agent.AgentChatBackend
import com.example.gemini.data.agent.ChatSessionStore
import com.example.gemini.data.preferences.AuthPreferences
import com.example.gemini.domain.model.AgentKind
import com.example.gemini.domain.model.ChatMessage
import com.example.gemini.domain.model.Conversation
import com.example.gemini.domain.model.MessageRole
import com.example.gemini.domain.model.TokenUsage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.util.UUID
import java.util.concurrent.TimeUnit

private const val TAG = "HermesChatBackend"

/**
 * Chat backend for Hermes Agent.
 *
 * Talks to the bridge at `/api/hermes/sessions` (REST) and
 * `/api/hermes/sessions/{id}/stream` (WebSocket NDJSON), mirroring the
 * Claude Code pattern already in the bridge.
 */
class HermesChatBackend(
    private val application: Application,
    private val backendScope: CoroutineScope,
    private val store: ChatSessionStore,
    private val authPrefs: AuthPreferences
) : AgentChatBackend {

    override val kind: AgentKind get() = AgentKind.HERMES

    // ── conversation list (we own HERMES conversations only) ──────────────────
    private val _conversations = MutableStateFlow<List<Conversation>>(emptyList())
    override val conversations: StateFlow<List<Conversation>> = _conversations.asStateFlow()
    override val currentConversation: StateFlow<Conversation?> = store.currentConversation.asStateFlow()

    // ── messages for the current chat ─────────────────────────────────────────
    private val _messages = MutableStateFlow<List<ChatMessage>>(emptyList())
    override val messages: StateFlow<List<ChatMessage>> = _messages.asStateFlow()

    private val _isStreaming = MutableStateFlow(false)
    override val isStreaming: StateFlow<Boolean> = _isStreaming.asStateFlow()

    private val _isLoadingConversation = MutableStateFlow(false)
    override val isLoadingConversation: StateFlow<Boolean> = _isLoadingConversation.asStateFlow()

    private val _conversationError = MutableStateFlow<String?>(null)
    override val conversationError: StateFlow<String?> = _conversationError.asStateFlow()

    // ── live streaming status shown under the chat input ─────────────────────
    private val _startupStatus = MutableStateFlow<String?>(null)
    /** "Starting Hermes…" while the bridge is booting the process. */
    val startupStatus: StateFlow<String?> = _startupStatus.asStateFlow()

    // ── HTTP / WS clients ─────────────────────────────────────────────────────
    private val http = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.SECONDS)   // streaming
        .writeTimeout(30, TimeUnit.SECONDS)
        .build()

    private var activeSession: String? = null
    private var activeWs: WebSocket? = null
    private var streamingJob: Job? = null

    // ── AgentChatBackend ──────────────────────────────────────────────────────

    override fun startNewChat() {
        backendScope.launch {
            disconnectWs()
            val conv = Conversation(
                id = UUID.randomUUID().toString(),
                title = "New Chat",
                agent = AgentKind.HERMES
            )
            activeSession = null
            _messages.value = emptyList()
            _isStreaming.value = false
            _conversationError.value = null
            _conversations.value = _conversations.value + conv
            store.setCurrentConversation(conv)
        }
    }

    /** Detach from the shared store without creating a new conversation (called when another agent takes over). */
    fun detach() {
        backendScope.launch { disconnectWs() }
    }

    override fun selectConversation(id: String) {
        backendScope.launch {
            disconnectWs()
            val conv = _conversations.value.find { it.id == id } ?: return@launch
            _isLoadingConversation.value = true
            store.setCurrentConversation(conv)
            // reload history from bridge
            runCatching { loadHistory(conv) }
                .onFailure { _conversationError.value = it.message }
            _isLoadingConversation.value = false
        }
    }

    override fun sendMessage(content: String) {
        val conv = store.currentConversation.value ?: run { startNewChat(); return }
        if (conv.agent != AgentKind.HERMES) return
        backendScope.launch { doSend(conv, content) }
    }

    override fun stopStreaming() {
        backendScope.launch {
            activeSession?.let { sid ->
                runCatching {
                    postBridge("/api/hermes/sessions/$sid/abort", "{}")
                }
            }
            _isStreaming.value = false
            _startupStatus.value = null
            // mark last assistant message done
            _messages.value = _messages.value.map {
                if (it.isStreaming) it.copy(isStreaming = false) else it
            }
        }
    }

    override fun deleteConversation(id: String) {
        backendScope.launch {
            if (activeSession != null && store.currentConversation.value?.id == id) disconnectWs()
            runCatching { postBridge("/api/hermes/sessions/$id/delete", "{}") }
            _conversations.value = _conversations.value.filter { it.id != id }
            if (store.currentConversation.value?.id == id) {
                val next = _conversations.value.lastOrNull()
                store.setCurrentConversation(next)
                if (next != null) selectConversation(next.id) else startNewChat()
            }
        }
    }

    override fun forkConversation(id: String) {
        // Hermes sessions are independent — fork = new chat seeded with a note
        startNewChat()
    }

    override fun retryConnections() {
        activeSession?.let { sid ->
            backendScope.launch { reconnectWs(sid) }
        }
    }

    override fun onAppForegrounded() = retryConnections()

    // ── private helpers ───────────────────────────────────────────────────────

    private suspend fun doSend(conv: Conversation, text: String) {
        _conversationError.value = null

        // append the user bubble immediately
        appendMessage(
            ChatMessage(
                conversationId = conv.id,
                role = MessageRole.USER,
                content = text
            )
        )

        val sid = activeSession ?: run {
            // create a new bridge session
            _startupStatus.value = "Starting Hermes…"
            val newSid = runCatching { createSession(conv) }.getOrElse {
                _startupStatus.value = null
                _conversationError.value = "Could not start Hermes: ${it.message}"
                return
            }
            activeSession = newSid
            updateConversation(conv.copy(title = conv.title))
            reconnectWs(newSid)
            newSid
        }
        _startupStatus.value = null

        // send the message payload to the running session
        runCatching {
            postBridge(
                "/api/hermes/sessions/$sid/send",
                JSONObject().put("content", text).toString()
            )
        }.onFailure {
            _conversationError.value = "Send failed: ${it.message}"
        }
    }

    private suspend fun createSession(conv: Conversation): String = withContext(Dispatchers.IO) {
        val body = JSONObject()
            .put("conversation_id", conv.id)
            .put("title", conv.title)
            .toString()
        val resp = postBridge("/api/hermes/sessions", body)
        JSONObject(resp).getString("session_id")
    }

    private suspend fun loadHistory(conv: Conversation) = withContext(Dispatchers.IO) {
        val sid = activeSession ?: return@withContext
        val resp = getBridge("/api/hermes/sessions/$sid/history")
        val arr = JSONObject(resp).optJSONArray("messages") ?: return@withContext
        val msgs = mutableListOf<ChatMessage>()
        for (i in 0 until arr.length()) {
            val o = arr.getJSONObject(i)
            msgs += ChatMessage(
                conversationId = conv.id,
                role = if (o.optString("role") == "assistant") MessageRole.ASSISTANT else MessageRole.USER,
                content = o.optString("content"),
                id = o.optString("id", UUID.randomUUID().toString())
            )
        }
        _messages.value = msgs
    }

    private fun reconnectWs(sid: String) {
        streamingJob?.cancel()
        activeWs?.cancel()
        val wsUrl = authPrefs.currentBridgeHttpUrl
            .trimEnd('/')
            .replace("http://", "ws://")
            .replace("https://", "wss://") +
                "/api/hermes/sessions/$sid/stream"
        val req = Request.Builder().url(wsUrl)
            .addHeader(authPrefs.bridgeAuthHeader, authPrefs.bridgeAuthToken)
            .build()
        activeWs = http.newWebSocket(req, HermesWsListener(sid))
        Log.d(TAG, "WS connecting to $wsUrl")
    }

    private fun disconnectWs() {
        streamingJob?.cancel()
        activeWs?.cancel()
        activeWs = null
        activeSession = null
        _isStreaming.value = false
        _startupStatus.value = null
    }

    private suspend fun postBridge(path: String, json: String): String = withContext(Dispatchers.IO) {
        val url = authPrefs.currentBridgeHttpUrl.trimEnd('/') + path
        val body = json.toRequestBody("application/json".toMediaType())
        val req = Request.Builder().url(url)
            .addHeader(authPrefs.bridgeAuthHeader, authPrefs.bridgeAuthToken)
            .post(body).build()
        http.newCall(req).execute().use { it.body()?.string() ?: "" }
    }

    private suspend fun getBridge(path: String): String = withContext(Dispatchers.IO) {
        val url = authPrefs.currentBridgeHttpUrl.trimEnd('/') + path
        val req = Request.Builder().url(url)
            .addHeader(authPrefs.bridgeAuthHeader, authPrefs.bridgeAuthToken)
            .get().build()
        http.newCall(req).execute().use { it.body()?.string() ?: "" }
    }

    private fun appendMessage(msg: ChatMessage) {
        _messages.value = _messages.value + msg
    }

    private fun updateConversation(conv: Conversation) {
        _conversations.value = _conversations.value.map { if (it.id == conv.id) conv else it }
        if (store.currentConversation.value?.id == conv.id) store.setCurrentConversation(conv)
    }

    // ── WebSocket listener ────────────────────────────────────────────────────

    private inner class HermesWsListener(private val sid: String) : WebSocketListener() {

        /** Accumulate streamed text for the current assistant bubble. */
        private var assistantMsgId: String? = null

        override fun onOpen(webSocket: WebSocket, response: Response) {
            Log.d(TAG, "WS open sid=$sid")
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            runCatching { handleLine(text) }.onFailure {
                Log.w(TAG, "Parse error: $text", it)
            }
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            Log.d(TAG, "WS closed $code $reason")
            finaliseStreaming()
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            Log.w(TAG, "WS failure", t)
            finaliseStreaming()
            _conversationError.value = "Connection lost: ${t.message}"
        }

        private fun handleLine(line: String) {
            if (line.isBlank()) return
            val obj = JSONObject(line)
            val conv = store.currentConversation.value ?: return

            when (obj.optString("type")) {
                "assistant_start" -> {
                    _isStreaming.value = true
                    val id = UUID.randomUUID().toString()
                    assistantMsgId = id
                    appendMessage(
                        ChatMessage(
                            id = id,
                            conversationId = conv.id,
                            role = MessageRole.ASSISTANT,
                            content = "",
                            isStreaming = true
                        )
                    )
                }

                "assistant_delta" -> {
                    val delta = obj.optString("delta")
                    val id = assistantMsgId ?: return
                    _messages.value = _messages.value.map { m ->
                        if (m.id == id) m.copy(content = m.content + delta) else m
                    }
                }

                "assistant_done" -> {
                    val id = assistantMsgId
                    val usage = obj.optJSONObject("usage")
                    _messages.value = _messages.value.map { m ->
                        if (m.id == id) m.copy(
                            isStreaming = false,
                            tokenUsage = usage?.let {
                                TokenUsage(
                                    promptTokens = it.optInt("input_tokens"),
                                    outputTokens = it.optInt("output_tokens"),
                                    totalTokens = it.optInt("input_tokens") + it.optInt("output_tokens")
                                )
                            }
                        ) else m
                    }
                    assistantMsgId = null
                    finaliseStreaming()
                    // refresh conversation title if the bridge sends one
                    obj.optString("title").takeIf { it.isNotBlank() }?.let { title ->
                        updateConversation(
                            (_conversations.value.find { it.id == conv.id } ?: conv).copy(title = title)
                        )
                    }
                }

                "error" -> {
                    val msg = obj.optString("message", "Unknown error")
                    _conversationError.value = msg
                    finaliseStreaming()
                }

                "status" -> {
                    _startupStatus.value = obj.optString("message").takeIf { it.isNotBlank() }
                }
            }
        }

        private fun finaliseStreaming() {
            _isStreaming.value = false
            _startupStatus.value = null
            _messages.value = _messages.value.map {
                if (it.isStreaming) it.copy(isStreaming = false) else it
            }
        }
    }
}
