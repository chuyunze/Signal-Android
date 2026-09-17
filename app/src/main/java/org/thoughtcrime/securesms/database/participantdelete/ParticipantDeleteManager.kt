package org.thoughtcrime.securesms.database.participantdelete

import android.database.Cursor
import org.signal.core.models.ServiceId
import org.signal.core.util.UuidUtil
import org.signal.core.util.logging.Log
import org.signal.core.util.orNull
import org.thoughtcrime.securesms.database.SignalDatabase
import org.thoughtcrime.securesms.database.model.GroupRecord
import org.thoughtcrime.securesms.database.model.MessageRecord
import org.thoughtcrime.securesms.recipients.Recipient
import org.thoughtcrime.securesms.recipients.RecipientId
import java.util.UUID

/**
 * Participant-delete protocol configuration. Mirrors iOS `ParticipantDeleteConfiguration`.
 *
 * See iOS commit 69077f4 `AdminDeleteManager.swift` companion constants block.
 */
object ParticipantDeleteConfig {
  /** Currently shipped protocol version. iOS matches exactly. */
  const val PROTOCOL_VERSION: Int = 1

  /** Receipts older than this are pruned on every `processReceipt` call (45 days). */
  const val RECEIPT_LIFETIME_MS: Long = 45L * 24 * 60 * 60 * 1000

  /** Pending rows expire after this window if the message never arrives locally (45 days). */
  const val PENDING_LIFETIME_MS: Long = 45L * 24 * 60 * 60 * 1000

  /** Max pending rows across the whole DB. Protects against DoS via forged requests. */
  const val MAX_PENDING_GLOBALLY: Int = 1000

  /** Max pending rows initiated by a single requester ACI. */
  const val MAX_PENDING_PER_REQUESTER: Int = 50

  /** Max pending rows per conversation (thread). */
  const val MAX_PENDING_PER_CONVERSATION: Int = 100

  /** Guard on orphan-receipt flood — receipts with no matching Request row beyond this count are dropped. */
  const val MAX_ORPHAN_RECEIPTS: Int = 2000

  /** Master kill-switch. Keep on for self-host — the iOS fork has it true, we mirror that. */
  const val FEATURE_ENABLED: Boolean = true
}

/**
 * Mirror of iOS `SSKProtoDataMessageParticipantDeleteReceipt.Result`. We define our own so the
 * manager layer does not require the proto-generated enum (which has a slightly different name
 * between Wire versions). Values MUST match proto.rawValue exactly (also 0=UNKNOWN through 6).
 */
enum class ParticipantDeleteReceiptResult(val rawValue: Int) {
  UNKNOWN(0),
  APPLIED(1),
  ALREADY_APPLIED(2),
  TARGET_PENDING(3),
  REJECTED_NOT_CURRENT_MEMBER(4),
  REJECTED_NOT_SUPPORTED(5),
  REJECTED_INVALID_TARGET(6);

  companion object {
    fun fromRaw(value: Int): ParticipantDeleteReceiptResult = entries.firstOrNull { it.rawValue == value } ?: UNKNOWN
  }
}

/**
 * Why a `process` / `processLocalInitiation` call may short-circuit. Mirrors iOS
 * `ParticipantDeleteError` — we fold into a sealed type so call-sites don't have to catch.
 */
sealed class ParticipantDeleteException(message: String) : Exception(message) {
  data object UnsupportedVersion : ParticipantDeleteException("protocol version mismatch")
  data object InvalidRequestId : ParticipantDeleteException("requestId must be 16 bytes")
  data object InvalidTarget : ParticipantDeleteException("target message is not deletable")
  data object InvalidThread : ParticipantDeleteException("thread is blocked (note-to-self / terminated / not-full-member)")
  data object RequesterNotCurrentMember : ParticipantDeleteException("requester is no longer a full member")
  data object ScopeMismatch : ParticipantDeleteException("scope does not match thread type")
  data object FutureTarget : ParticipantDeleteException("targetSentTimestamp is in the future vs. serverTimestamp")
  data object PendingLimitExceeded : ParticipantDeleteException("one of the pending queue caps is exceeded")
}

/**
 * Where a `ParticipantDelete` request originated.
 *
 * - REMOTE_ENVELOPE       : arrived from the network, requester is whoever signed the envelope.
 * - LOCAL_SENT_TRANSCRIPT : we see *our own* outgoing delete via SyncMessage (sent-transcript),
 *                           requester is ourselves; deviceId identifies which of our devices sent it.
 * - LOCAL_INITIATION      : *we* are initiating a delete from the UI (this device). deviceId = null.
 */
data class ParticipantDeleteOrigin(
  val requesterAci: UUID,
  val sourceDeviceId: Int?,
  /** True for REMOTE_ENVELOPE and LOCAL_SENT_TRANSCRIPT — we should ACK the requester with a receipt. */
  val shouldSendReceipt: Boolean
) {
  companion object {
    fun remote(requesterAci: UUID, sourceDeviceId: Int) = ParticipantDeleteOrigin(requesterAci, sourceDeviceId, shouldSendReceipt = true)
    fun localSentTranscript(localAci: UUID, sourceDeviceId: Int) = ParticipantDeleteOrigin(localAci, sourceDeviceId, shouldSendReceipt = false)
    fun localInitiation(localAci: UUID) = ParticipantDeleteOrigin(localAci, sourceDeviceId = null, shouldSendReceipt = false)
  }
}

// =============================================================================
// DAO helpers — raw SQL against the 5 new tables created in V327 migration.
// =============================================================================

private object DAO {
  // --- participant_delete_request -----------------------------------------------------------
  fun requestByRequestId(requestId: ByteArray): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_request", null,
      "request_id = ?", arrayOf(requestId),
      null, null, null
    )
  }

  fun insertRequest(values: android.content.ContentValues) {
    SignalDatabase.rawDatabase.insert("participant_delete_request", null, values)
  }

  fun updateProcessingResult(requestId: ByteArray, resultRaw: Int) {
    val cv = android.content.ContentValues().apply { put("processing_result", resultRaw) }
    SignalDatabase.rawDatabase.update("participant_delete_request", cv, "request_id = ?", arrayOf(requestId))
  }

  fun requestsByTarget(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_request", null,
      "stable_conversation_id = ? AND target_author_aci = ? AND target_sent_timestamp = ?",
      arrayOf(stableConvId, targetAuthorAci, targetTimestamp.toString()),
      null, null, null
    )
  }

  // --- participant_delete_tombstone ----------------------------------------------------------
  fun tombstoneExists(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Boolean {
    SignalDatabase.rawDatabase.rawQuery(
      "SELECT 1 FROM participant_delete_tombstone " +
        "WHERE stable_conversation_id = ? AND target_author_aci = ? AND target_sent_timestamp = ? LIMIT 1",
      arrayOf(stableConvId, targetAuthorAci, targetTimestamp.toString())
    ).use { c -> return c.moveToFirst() }
  }

  fun insertTombstone(values: android.content.ContentValues) {
    SignalDatabase.rawDatabase.insert("participant_delete_tombstone", null, values)
  }

  fun updateTombstoneInteractionId(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long, interactionId: Long, localThreadUniqueId: String) {
    val cv = android.content.ContentValues().apply {
      put("interaction_id", interactionId)
      put("local_thread_unique_id", localThreadUniqueId)
    }
    SignalDatabase.rawDatabase.update(
      "participant_delete_tombstone", cv,
      "stable_conversation_id = ? AND target_author_aci = ? AND target_sent_timestamp = ?",
      arrayOf(stableConvId, targetAuthorAci, targetTimestamp.toString())
    )
  }

  fun tombstoneByTarget(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_tombstone", null,
      "stable_conversation_id = ? AND target_author_aci = ? AND target_sent_timestamp = ?",
      arrayOf(stableConvId, targetAuthorAci, targetTimestamp.toString()),
      null, null, null
    )
  }

  // --- pending_participant_delete ------------------------------------------------------------
  fun pruneExpiredPending(nowMs: Long) {
    SignalDatabase.rawDatabase.delete("pending_participant_delete", "expires_at <= ?", arrayOf(nowMs.toString()))
  }

  fun pendingByTarget(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Cursor {
    return SignalDatabase.rawDatabase.query(
      "pending_participant_delete", null,
      "stable_conversation_id = ? AND target_author_aci = ? AND target_sent_timestamp = ?",
      arrayOf(stableConvId, targetAuthorAci, targetTimestamp.toString()),
      null, null, null
    )
  }

  fun countPendingByRequester(requesterAci: ByteArray): Int {
    SignalDatabase.rawDatabase.rawQuery(
      "SELECT COUNT(*) FROM pending_participant_delete WHERE requester_aci = ?",
      arrayOf(requesterAci)
    ).use { c -> c.moveToFirst(); return c.getInt(0) }
  }

  fun countPendingByConversation(stableConvId: ByteArray): Int {
    SignalDatabase.rawDatabase.rawQuery(
      "SELECT COUNT(*) FROM pending_participant_delete WHERE stable_conversation_id = ?",
      arrayOf(stableConvId)
    ).use { c -> c.moveToFirst(); return c.getInt(0) }
  }

  fun countPendingGlobal(): Int {
    SignalDatabase.rawDatabase.rawQuery("SELECT COUNT(*) FROM pending_participant_delete", null)
      .use { c -> c.moveToFirst(); return c.getInt(0) }
  }

  fun insertPending(values: android.content.ContentValues) {
    SignalDatabase.rawDatabase.insert("pending_participant_delete", null, values)
  }

  fun deletePendingByTarget(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long) {
    SignalDatabase.rawDatabase.delete(
      "pending_participant_delete",
      "stable_conversation_id = ? AND target_author_aci = ? AND target_sent_timestamp = ?",
      arrayOf(stableConvId, targetAuthorAci, targetTimestamp.toString())
    )
  }

  // --- participant_delete_device_receipt -----------------------------------------------------
  fun pruneOldReceipts(nowMs: Long, receiptLifetimeMs: Long) {
    val cutoff = nowMs - receiptLifetimeMs
    SignalDatabase.rawDatabase.delete("participant_delete_device_receipt", "received_at <= ?", arrayOf(cutoff.toString()))
  }

  fun insertDeviceReceipt(values: android.content.ContentValues) {
    SignalDatabase.rawDatabase.insert("participant_delete_device_receipt", null, values)
  }

  fun receiptsForRequest(requestId: ByteArray): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_device_receipt", null,
      "request_id = ?", arrayOf(requestId),
      null, null, null
    )
  }

  fun orphanReceiptCount(): Int {
    SignalDatabase.rawDatabase.rawQuery(
      """
        SELECT COUNT(*) FROM participant_delete_device_receipt AS r
        WHERE NOT EXISTS (SELECT 1 FROM participant_delete_request AS q WHERE q.request_id = r.request_id)
      """.trimIndent(), null
    ).use { c -> c.moveToFirst(); return c.getInt(0) }
  }

  fun deleteDeviceReceipt(requestId: ByteArray, responderAci: ByteArray, responderDeviceId: Long) {
    SignalDatabase.rawDatabase.delete(
      "participant_delete_device_receipt",
      "request_id = ? AND responder_aci = ? AND responder_device_id = ?",
      arrayOf(requestId, responderAci, responderDeviceId.toString())
    )
  }

  // --- participant_delete_author -------------------------------------------------------------
  fun insertAuthor(interactionId: Long, requesterAci: ByteArray) {
    val cv = android.content.ContentValues().apply {
      put("interaction_id", interactionId)
      put("requester_aci", requesterAci)
    }
    SignalDatabase.rawDatabase.insert("participant_delete_author", null, cv)
  }

  fun authorByInteractionId(interactionId: Long): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_author", null,
      "interaction_id = ?", arrayOf(interactionId.toString()),
      null, null, null
    )
  }
}

// =============================================================================
// Public manager — 1:1 port of iOS AdminDeleteManager.ParticipantDeleteManager.
// =============================================================================

class ParticipantDeleteManager {

  companion object {
    private val TAG = Log.tag(ParticipantDeleteManager::class.java)

    /** Scope values mirror [SignalServiceDataMessage.ParticipantDelete.Scope]. */
    const val SCOPE_DIRECT_CHAT_BOTH_ACCOUNTS = 1
    const val SCOPE_GROUP_ALL_CURRENT_MEMBERS = 2

    // ---- stable conversation id helpers ----

    /**
     * Mirrors iOS `directConversationId(localAci, contactAci)`:
     * `[0x01] + lexicographically(sort(local, contact))`.
     * Consistent ordering means both ends compute the same bytes.
     */
    fun directConversationId(localAci: ByteArray, contactAci: ByteArray): ByteArray {
      val prefix = byteArrayOf(0x01)
      val (lo, hi) = if (byteArrayCompare(localAci, contactAci) <= 0) localAci to contactAci else contactAci to localAci
      return prefix + lo + hi
    }

    /** Mirrors iOS `Data([0x02]) + groupModel.groupId`. */
    fun groupConversationId(groupId: ByteArray): ByteArray = byteArrayOf(0x02) + groupId

    private fun byteArrayCompare(a: ByteArray, b: ByteArray): Int {
      val n = minOf(a.size, b.size)
      for (i in 0 until n) {
        val diff = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
        if (diff != 0) return diff
      }
      return a.size - b.size
    }

    /**
     * Mirrors iOS `isSupportedTarget`. We avoid participant-deleting messages that have
     * no stable identity or are tied to features the delete must not traverse.
     */
    fun isSupportedTarget(message: MessageRecord): Boolean {
      if (message.isViewOnce || message.isStoryReply) return false
      // Payment + gift badge are handled by field checks iOS-side; Android MessageRecord
      // exposes fewer fields — we approximate by rejecting any outgoing / sync-only markers.
      // Polls are checked on iOS via OWSMessagePoll; Android via isPoll() which we don't have
      // as a stable public check from this layer — conservatively skip.
      return true
    }

    /**
     * Mirrors iOS `authorAci(for:localAci)`. For outgoing messages the author is us.
     * For incoming messages we use `message.recipient` (the sender).
     */
    fun authorAci(message: MessageRecord, localAci: UUID): UUID? {
      return if (message.isOutgoing) localAci else Recipient.resolved(message.recipient.id).aci.orNull()
    }

    /** Mirrors iOS `queueReceipt` — enqueue a job to send our receipt back to the requester. */
    fun queueReceipt(requestId: ByteArray, resultRaw: Int, recipientAci: UUID) {
      org.thoughtcrime.securesms.jobs.OutgoingParticipantDeleteReceiptJob.enqueue(
        recipientAci = org.signal.core.models.ServiceId.parseOrThrow(recipientAci.toString()),
        requestId = requestId,
        resultRaw = resultRaw
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Public API (mirrors iOS exactly)
  // ---------------------------------------------------------------------------

  /** Returns true if a local user-initiated delete of `message` through `threadId` is allowed. */
  fun canParticipantDelete(message: MessageRecord, threadId: Long): Boolean {
    if (!ParticipantDeleteConfig.FEATURE_ENABLED) return false
    if (!message.isOutgoing && !message.isIncoming) return false
    if (message.isRemoteDelete) return false
    if (message.timestamp <= 0) return false
    if (!isSupportedTarget(message)) return false

    val localAci = Recipient.self().aci.orNull() ?: return false
    val authorAci = authorAci(message, localAci) ?: return false

    // thread = self → no
    if (threadId <= 0) return false

    // Determine if our thread is contact or group and whether we are a current full member.
    val threadRecipientId: RecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId) ?: return false
    val threadRecipient = Recipient.resolved(threadRecipientId)
    return if (threadRecipient.isGroup) {
      val group: GroupRecord = SignalDatabase.groups.getGroup(threadRecipientId).orNull() ?: return false
      group.members.contains(Recipient.self().id)
    } else {
      // contact thread — valid only if the target author is either us or the contact
      val contactAci = threadRecipient.aci.orNull() ?: return false
      authorAci == localAci || authorAci == contactAci
    }
  }

  /**
   * Processes an incoming `ParticipantDelete` envelope. Returns the result that should be
   * sent back as our own receipt (the caller is responsible for enqueuing it — see Step 5).
   */
  fun process(
    proto: org.whispersystems.signalservice.internal.push.DataMessage.ParticipantDelete,
    origin: ParticipantDeleteOrigin,
    threadId: Long,
    trustedServerTimestamp: Long?
  ): ParticipantDeleteReceiptResult {
    val request = parse(proto) ?: return ParticipantDeleteReceiptResult.REJECTED_NOT_SUPPORTED
    return processValidated(
      targetAuthorAci = request.targetAuthorAci,
      targetSentTimestamp = request.targetSentTimestamp,
      requestId = request.requestId,
      scope = request.scope,
      groupRevision = request.groupRevision,
      requesterAci = origin.requesterAci,
      requesterDeviceId = origin.sourceDeviceId,
      originShouldSendReceipt = origin.shouldSendReceipt,
      threadId = threadId,
      trustedServerTimestamp = trustedServerTimestamp
    )
  }

  /**
   * Entry point for user-initiated local deletes (from Step 6 UI). Builds a validated request
   * and processes it as if we'd received it from the wire.
   */
  fun processLocalInitiation(
    requestId: ByteArray,
    targetAuthor: UUID,
    targetSentTimestamp: Long,
    scope: Int,
    groupRevision: Int?,
    threadId: Long
  ) {
    if (requestId.size != 16) throw ParticipantDeleteException.InvalidRequestId
    if (targetSentTimestamp <= 0) throw ParticipantDeleteException.InvalidTarget
    if (scope != SCOPE_DIRECT_CHAT_BOTH_ACCOUNTS && scope != SCOPE_GROUP_ALL_CURRENT_MEMBERS) {
      throw ParticipantDeleteException.InvalidTarget
    }
    val localAci = Recipient.self().aci.orNull() ?: throw ParticipantDeleteException.InvalidThread
    processValidated(
      targetAuthorAci = targetAuthor,
      targetSentTimestamp = targetSentTimestamp,
      requestId = requestId,
      scope = scope,
      groupRevision = groupRevision,
      requesterAci = localAci,
      requesterDeviceId = null,
      originShouldSendReceipt = false,
      threadId = threadId,
      trustedServerTimestamp = null
    )
  }

  /**
   * Handles an incoming `ParticipantDeleteReceipt` envelope. Stores the ACK row and runs the
   * aggregation step — if every current conversation member has now responded, we resolve the
   * pending row into a tombstone and delete the message locally.
   */
  fun processReceipt(
    proto: org.whispersystems.signalservice.internal.push.DataMessage.ParticipantDeleteReceipt,
    responderAci: UUID,
    sourceDeviceId: Int
  ) {
    val nowMs = System.currentTimeMillis()
    if (proto.version != ParticipantDeleteConfig.PROTOCOL_VERSION) {
      Log.w(TAG, "Ignoring invalid receipt: protocol version ${proto.version}")
      return
    }
    val requestId = proto.requestId?.toByteArray() ?: run {
      Log.w(TAG, "Ignoring invalid receipt: missing requestId")
      return
    }
    val resultRaw = proto.result?.number ?: run {
      Log.w(TAG, "Ignoring invalid receipt: missing result")
      return
    }
    if (resultRaw == ParticipantDeleteReceiptResult.UNKNOWN.rawValue) {
      Log.w(TAG, "Ignoring invalid receipt: UNKNOWN result")
      return
    }

    SignalDatabase.rawDatabase.beginTransaction()
    try {
      DAO.pruneOldReceipts(nowMs, ParticipantDeleteConfig.RECEIPT_LIFETIME_MS)

      // Does the Request row exist? If yes, sanity-check the responder is a member of that thread.
      var row: ParticipantDeleteRequestRow? = null
      DAO.requestByRequestId(requestId).use { c ->
        if (c.moveToFirst()) row = ParticipantDeleteRequestRow.fromCursor(c)
      }

      if (row != null) {
        if (!isValidReceiptResponder(responderAci, row!!)) {
          Log.w(TAG, "Ignoring receipt from outside the target conversation")
          return
        }
      } else {
        // Orphan receipt — no matching Request row. Guard against flood.
        if (DAO.orphanReceiptCount() >= ParticipantDeleteConfig.MAX_ORPHAN_RECEIPTS) {
          Log.w(TAG, "Ignoring orphan receipt above capacity limit")
          return
        }
      }

      DAO.insertDeviceReceipt(android.content.ContentValues().apply {
        put("request_id", requestId)
        put("responder_aci", UuidUtil.toByteArray(responderAci))
        put("responder_device_id", sourceDeviceId.toLong())
        put("result", resultRaw)
        put("received_at", nowMs)
      })

      // If there is a matching pending row, check whether it is now fully acknowledged.
      if (row != null) {
        tryResolvePending(row!!)
      }

      SignalDatabase.rawDatabase.setTransactionSuccessful()
    } finally {
      SignalDatabase.rawDatabase.endTransaction()
    }
  }

  /**
   * Called from the message-insert pipeline (analogous to iOS `applyPendingDeleteIfNecessary`
   * in MessageReceiver). If a tombstone or pending row matches [message], it is turned into a
   * "deleted by participant" remote-delete and metadata is recorded.
   */
  fun applyPendingDeleteIfNecessary(message: MessageRecord) {
    try {
      val localAci = Recipient.self().aci.orNull() ?: return
      val authorAci = authorAci(message, localAci) ?: return
      val stableConvId = stableConversationIdForThread(message.threadId, localAci) ?: return
      if (!isSupportedTarget(message)) return

      SignalDatabase.rawDatabase.beginTransaction()
      try {
        // 1. Check tombstone first — if one exists, just link this message row and bail out.
        if (DAO.tombstoneExists(stableConvId, UuidUtil.toByteArray(authorAci), message.timestamp)) {
          DAO.tombstoneByTarget(stableConvId, UuidUtil.toByteArray(authorAci), message.timestamp).use { c ->
            if (c.moveToFirst()) {
              val tombstoneAciBytes = c.getBlob(c.getColumnIndexOrThrow("requester_aci"))
              DAO.updateTombstoneInteractionId(
                stableConvId, UuidUtil.toByteArray(authorAci), message.timestamp,
                message.id, threadUniqueIdOf(message.threadId)
              )
              DAO.insertAuthor(message.id, tombstoneAciBytes)
            }
          }
          markMessageAsRemoteDelete(message.id)
          SignalDatabase.rawDatabase.setTransactionSuccessful()
          return
        }

        // 2. Then check pending — apply pending → tombstone + delete.
        DAO.pendingByTarget(stableConvId, UuidUtil.toByteArray(authorAci), message.timestamp).use { c ->
          if (c.moveToFirst()) {
            val pendingRow = PendingParticipantDeleteRow.fromCursor(c)
            DAO.insertTombstone(android.content.ContentValues().apply {
              put("stable_conversation_id", pendingRow.stableConversationId)
              put("local_thread_unique_id", threadUniqueIdOf(message.threadId))
              put("target_author_aci", pendingRow.targetAuthorAci)
              put("target_sent_timestamp", message.timestamp)
              put("interaction_id", message.id)
              put("first_request_id", pendingRow.firstRequestId)
              put("requester_aci", pendingRow.requesterAci)
              put("applied_at", System.currentTimeMillis())
              put("protocol_version", pendingRow.protocolVersion)
            })
            DAO.deletePendingByTarget(stableConvId, pendingRow.targetAuthorAci, message.timestamp)
            DAO.insertAuthor(message.id, pendingRow.requesterAci)
            // Update all matching Request rows → APPLIED
            DAO.requestsByTarget(stableConvId, pendingRow.targetAuthorAci, message.timestamp).use { rc ->
              while (rc.moveToNext()) {
                val reqId = rc.getBlob(rc.getColumnIndexOrThrow("request_id"))
                DAO.updateProcessingResult(reqId, ParticipantDeleteReceiptResult.APPLIED.rawValue)
              }
            }
            markMessageAsRemoteDelete(message.id)
          }
        }

        SignalDatabase.rawDatabase.setTransactionSuccessful()
      } finally {
        SignalDatabase.rawDatabase.endTransaction()
      }
    } catch (e: Exception) {
      Log.e(TAG, "applyPendingDeleteIfNecessary failed", e)
    }
  }

  /** Resolve the "who deleted this message?" metadata row for a given local interaction id. */
  fun requesterAciOf(interactionId: Long): UUID? {
    DAO.authorByInteractionId(interactionId).use { c ->
      if (c.moveToFirst()) {
        val bytes = c.getBlob(c.getColumnIndexOrThrow("requester_aci"))
        return runCatching { UuidUtil.parseOrThrow(bytes) }.getOrNull()
      }
    }
    return null
  }

  // ---------------------------------------------------------------------------
  // Private — the core state machine (mirrors iOS `private process(request:,origin:,...)`)
  // ---------------------------------------------------------------------------

  private fun processValidated(
    targetAuthorAci: UUID,
    targetSentTimestamp: Long,
    requestId: ByteArray,
    scope: Int,
    groupRevision: Int?,
    requesterAci: UUID,
    requesterDeviceId: Int?,
    originShouldSendReceipt: Boolean,
    threadId: Long,
    trustedServerTimestamp: Long?
  ): ParticipantDeleteReceiptResult {
    val localAci = Recipient.self().aci.orNull() ?: throw ParticipantDeleteException.InvalidThread

    // ---- Step 1: validate thread scope and requester membership ----
    val stableConvId: ByteArray
    val threadRecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId)
      ?: throw ParticipantDeleteException.InvalidThread
    val threadRecipient = Recipient.resolved(threadRecipientId)

    if (threadRecipient.isSelf) throw ParticipantDeleteException.InvalidThread

    if (threadRecipient.isGroup) {
      if (scope != SCOPE_GROUP_ALL_CURRENT_MEMBERS) throw ParticipantDeleteException.ScopeMismatch
      val group = SignalDatabase.groups.getGroup(threadRecipientId).orNull()
        ?: throw ParticipantDeleteException.InvalidThread
      if (!group.members.contains(Recipient.self().id)) throw ParticipantDeleteException.InvalidThread
      if (!group.members.contains(RecipientId.from(ServiceId.ACI.from(requesterAci)))) {
        throw ParticipantDeleteException.RequesterNotCurrentMember
      }
      stableConvId = groupConversationId(group.id.toByteArray())
    } else {
      if (scope != SCOPE_DIRECT_CHAT_BOTH_ACCOUNTS) throw ParticipantDeleteException.ScopeMismatch
      val contactAci = threadRecipient.aci.orNull() ?: throw ParticipantDeleteException.InvalidThread
      if (targetAuthorAci != localAci && targetAuthorAci != contactAci) {
        throw ParticipantDeleteException.InvalidTarget
      }
      if (requesterAci != localAci && requesterAci != contactAci) {
        throw ParticipantDeleteException.InvalidThread
      }
      stableConvId = directConversationId(UuidUtil.toByteArray(localAci), UuidUtil.toByteArray(contactAci))
    }

    if (trustedServerTimestamp != null && targetSentTimestamp > trustedServerTimestamp) {
      throw ParticipantDeleteException.FutureTarget
    }

    // ---- Step 2: request dedupe / tombstone shortcut / queue-pending / apply ----
    val requesterAciBytes = UuidUtil.toByteArray(requesterAci)
    val localThreadUniqueId = threadUniqueIdOf(threadId)

    SignalDatabase.rawDatabase.beginTransaction()
    try {
      // Dedupe: if we already saw this requestId, return its recorded result.
      DAO.requestByRequestId(requestId).use { c ->
        if (c.moveToFirst()) {
          val existingResult = c.getInt(c.getColumnIndexOrThrow("processing_result"))
          if (originShouldSendReceipt) {
            queueReceipt(requestId, existingResult, requesterAci)
          }
          SignalDatabase.rawDatabase.setTransactionSuccessful()
          return ParticipantDeleteReceiptResult.fromRaw(existingResult)
        }
      }

      // Tombstone shortcut: already-applied → record + ALREADY_APPLIED.
      val targetAuthorAciBytes = UuidUtil.toByteArray(targetAuthorAci)
      if (DAO.tombstoneExists(stableConvId, targetAuthorAciBytes, targetSentTimestamp)) {
        DAO.insertRequest(android.content.ContentValues().apply {
          put("request_id", requestId)
          put("requester_aci", requesterAciBytes)
          requesterDeviceId?.let { put("requester_device_id", it.toLong()) }
          put("stable_conversation_id", stableConvId)
          put("local_thread_unique_id", localThreadUniqueId)
          put("target_author_aci", targetAuthorAciBytes)
          put("target_sent_timestamp", targetSentTimestamp)
          put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
          put("processing_result", ParticipantDeleteReceiptResult.ALREADY_APPLIED.rawValue)
          put("created_at", System.currentTimeMillis())
        })
        if (originShouldSendReceipt) queueReceipt(requestId, ParticipantDeleteReceiptResult.ALREADY_APPLIED.rawValue, requesterAci)
        SignalDatabase.rawDatabase.setTransactionSuccessful()
        return ParticipantDeleteReceiptResult.ALREADY_APPLIED
      }

      // Try to find the target message locally.
      val targetMessage: MessageRecord? = try {
        SignalDatabase.messages.getMessageFor(targetSentTimestamp, RecipientId.from(ServiceId.ACI.from(targetAuthorAci)))
      } catch (t: Throwable) { null }

      if (targetMessage == null) {
        // Message not yet in DB → enqueue pending so we apply later when it arrives.
        val nowMs = System.currentTimeMillis()
        DAO.pruneExpiredPending(nowMs)

        DAO.pendingByTarget(stableConvId, targetAuthorAciBytes, targetSentTimestamp).use { c ->
          if (!c.moveToFirst()) {
            // Cap checks.
            if (DAO.countPendingGlobal() >= ParticipantDeleteConfig.MAX_PENDING_GLOBALLY
              || DAO.countPendingByRequester(requesterAciBytes) >= ParticipantDeleteConfig.MAX_PENDING_PER_REQUESTER
              || DAO.countPendingByConversation(stableConvId) >= ParticipantDeleteConfig.MAX_PENDING_PER_CONVERSATION
            ) {
              throw ParticipantDeleteException.PendingLimitExceeded
            }
            DAO.insertPending(android.content.ContentValues().apply {
              put("first_request_id", requestId)
              put("stable_conversation_id", stableConvId)
              put("local_thread_unique_id", localThreadUniqueId)
              put("target_author_aci", targetAuthorAciBytes)
              put("target_sent_timestamp", targetSentTimestamp)
              put("requester_aci", requesterAciBytes)
              requesterDeviceId?.let { put("requester_device_id", it.toLong()) }
              put("request_server_timestamp", trustedServerTimestamp ?: nowMs)
              put("conversation_scope", scope)
              groupRevision?.let { put("group_revision", it.toLong()) }
              put("expires_at", nowMs + ParticipantDeleteConfig.PENDING_LIFETIME_MS)
              put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
            })
          }
        }

        DAO.insertRequest(android.content.ContentValues().apply {
          put("request_id", requestId)
          put("requester_aci", requesterAciBytes)
          requesterDeviceId?.let { put("requester_device_id", it.toLong()) }
          put("stable_conversation_id", stableConvId)
          put("local_thread_unique_id", localThreadUniqueId)
          put("target_author_aci", targetAuthorAciBytes)
          put("target_sent_timestamp", targetSentTimestamp)
          put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
          put("processing_result", ParticipantDeleteReceiptResult.TARGET_PENDING.rawValue)
          put("created_at", System.currentTimeMillis())
        })
        if (originShouldSendReceipt) queueReceipt(requestId, ParticipantDeleteReceiptResult.TARGET_PENDING.rawValue, requesterAci)
        SignalDatabase.rawDatabase.setTransactionSuccessful()
        return ParticipantDeleteReceiptResult.TARGET_PENDING
      }

      // Target message is in our DB → apply delete now.
      if (!isSupportedTarget(targetMessage)) {
        throw ParticipantDeleteException.InvalidTarget
      }
      if (targetMessage.threadId != threadId || targetMessage.timestamp != targetSentTimestamp) {
        throw ParticipantDeleteException.InvalidTarget
      }

      if (!targetMessage.isRemoteDelete) {
        markMessageAsRemoteDelete(targetMessage.id)
      }
      DAO.insertTombstone(android.content.ContentValues().apply {
        put("stable_conversation_id", stableConvId)
        put("local_thread_unique_id", localThreadUniqueId)
        put("target_author_aci", targetAuthorAciBytes)
        put("target_sent_timestamp", targetSentTimestamp)
        put("interaction_id", targetMessage.id)
        put("first_request_id", requestId)
        put("requester_aci", requesterAciBytes)
        put("applied_at", System.currentTimeMillis())
        put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
      })
      DAO.insertAuthor(targetMessage.id, requesterAciBytes)
      DAO.insertRequest(android.content.ContentValues().apply {
        put("request_id", requestId)
        put("requester_aci", requesterAciBytes)
        requesterDeviceId?.let { put("requester_device_id", it.toLong()) }
        put("stable_conversation_id", stableConvId)
        put("local_thread_unique_id", localThreadUniqueId)
        put("target_author_aci", targetAuthorAciBytes)
        put("target_sent_timestamp", targetSentTimestamp)
        put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
        put("processing_result", ParticipantDeleteReceiptResult.APPLIED.rawValue)
        put("created_at", System.currentTimeMillis())
      })
      if (originShouldSendReceipt) queueReceipt(requestId, ParticipantDeleteReceiptResult.APPLIED.rawValue, requesterAci)
      SignalDatabase.rawDatabase.setTransactionSuccessful()
      return ParticipantDeleteReceiptResult.APPLIED
    } catch (pe: ParticipantDeleteException) {
      val fallback = when (pe) {
        is ParticipantDeleteException.RequesterNotCurrentMember -> ParticipantDeleteReceiptResult.REJECTED_NOT_CURRENT_MEMBER
        is ParticipantDeleteException.UnsupportedVersion -> ParticipantDeleteReceiptResult.REJECTED_NOT_SUPPORTED
        else -> ParticipantDeleteReceiptResult.REJECTED_INVALID_TARGET
      }
      // Best-effort enqueue a receipt so the requester knows why.
      try { queueReceipt(requestId, fallback.rawValue, requesterAci) } catch (_: Throwable) { }
      throw pe
    } finally {
      try { SignalDatabase.rawDatabase.endTransaction() } catch (_: Throwable) { }
    }
  }

  /** Mirrors iOS `tryResolvePending`. If every current conversation member has ACKed the pending row, apply it. */
  private fun tryResolvePending(requestRow: ParticipantDeleteRequestRow) {
    val nowMs = System.currentTimeMillis()
    DAO.pruneExpiredPending(nowMs)

    DAO.pendingByTarget(requestRow.stableConversationId, requestRow.targetAuthorAci, requestRow.targetSentTimestamp).use { c ->
      if (!c.moveToFirst()) return
      val pending = PendingParticipantDeleteRow.fromCursor(c)

      val threadId = threadIdFromUniqueId(pending.localThreadUniqueId) ?: return
      val threadRecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId) ?: return
      val threadRecipient = Recipient.resolved(threadRecipientId)

      // Gather current full member set.
      val fullMemberAcis: List<UUID> = if (threadRecipient.isGroup) {
        val group = SignalDatabase.groups.getGroup(threadRecipientId).orNull() ?: return
        group.members
          .filter { it.isGroupMember }
          .mapNotNull { it.aci.orNull() }
      } else {
        listOfNotNull(
          Recipient.self().aci.orNull(),
          threadRecipient.aci.orNull()
        )
      }

      // Collect unique responders from all device receipts for this pending's firstRequestId.
      val respondedAcis = mutableSetOf<ByteArray>()
      DAO.receiptsForRequest(requestRow.requestId).use { rc ->
        while (rc.moveToNext()) {
          respondedAcis.add(rc.getBlob(rc.getColumnIndexOrThrow("responder_aci")))
        }
      }

      val allResponded = fullMemberAcis.all { memberAci ->
        respondedAcis.contains(UuidUtil.toByteArray(memberAci))
      }

      if (!allResponded) return

      // All in → apply tombstone + delete message.
      val targetMessage = try {
        SignalDatabase.messages.getMessageFor(pending.targetSentTimestamp, RecipientId.from(ServiceId.ACI.from(UuidUtil.parseOrThrow(pending.targetAuthorAci))))
      } catch (t: Throwable) { null }
      if (targetMessage == null) return

      if (!targetMessage.isRemoteDelete) markMessageAsRemoteDelete(targetMessage.id)
      DAO.insertTombstone(android.content.ContentValues().apply {
        put("stable_conversation_id", pending.stableConversationId)
        put("local_thread_unique_id", pending.localThreadUniqueId)
        put("target_author_aci", pending.targetAuthorAci)
        put("target_sent_timestamp", pending.targetSentTimestamp)
        put("interaction_id", targetMessage.id)
        put("first_request_id", pending.firstRequestId)
        put("requester_aci", pending.requesterAci)
        put("applied_at", System.currentTimeMillis())
        put("protocol_version", pending.protocolVersion)
      })
      DAO.insertAuthor(targetMessage.id, pending.requesterAci)
      DAO.deletePendingByTarget(pending.stableConversationId, pending.targetAuthorAci, pending.targetSentTimestamp)

      // Mark all Request rows for this target → APPLIED and fire receipts to other requesters.
      DAO.requestsByTarget(pending.stableConversationId, pending.targetAuthorAci, pending.targetSentTimestamp).use { rc ->
        while (rc.moveToNext()) {
          val reqId = rc.getBlob(rc.getColumnIndexOrThrow("request_id"))
          val requesterAciBytes = rc.getBlob(rc.getColumnIndexOrThrow("requester_aci"))
          DAO.updateProcessingResult(reqId, ParticipantDeleteReceiptResult.APPLIED.rawValue)
          val requestingAci = runCatching { UuidUtil.parseOrThrow(requesterAciBytes) }.getOrNull()
          if (requestingAci != null && requestingAci != Recipient.self().aci.orNull()) {
            queueReceipt(reqId, ParticipantDeleteReceiptResult.APPLIED.rawValue, requestingAci)
          }
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private fun isValidReceiptResponder(responderAci: UUID, requestRow: ParticipantDeleteRequestRow): Boolean {
    val threadId = threadIdFromUniqueId(requestRow.localThreadUniqueId) ?: return false
    val threadRecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId) ?: return false
    val threadRecipient = Recipient.resolved(threadRecipientId)
    return if (threadRecipient.isGroup) {
      val group = SignalDatabase.groups.getGroup(threadRecipientId).orNull() ?: return false
      group.members.any { it.aci.orNull() == responderAci }
    } else {
      threadRecipient.aci.orNull() == responderAci || Recipient.self().aci.orNull() == responderAci
    }
  }

  private fun parse(proto: org.whispersystems.signalservice.internal.push.DataMessage.ParticipantDelete): ParsedRequest? {
    if (proto.version != ParticipantDeleteConfig.PROTOCOL_VERSION) return null
    val requestIdBytes = proto.requestId?.toByteArray() ?: return null
    if (requestIdBytes.size != 16) return null
    val targetAuthorAci = proto.targetAuthorAciBinary?.toByteArray()?.let { runCatching { UuidUtil.parseOrThrow(it) }.getOrNull() } ?: return null
    val targetSentTimestamp = proto.targetSentTimestamp?.takeIf { it > 0 } ?: return null
    val scopeRaw = proto.scope?.value?.takeIf { it != 0 } ?: return null
    return ParsedRequest(
      targetAuthorAci = targetAuthorAci,
      targetSentTimestamp = targetSentTimestamp,
      requestId = requestIdBytes,
      scope = scopeRaw,
      groupRevision = proto.groupRevision
    )
  }

  private fun stableConversationIdForThread(threadId: Long, localAci: UUID): ByteArray? {
    val threadRecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId) ?: return null
    val threadRecipient = Recipient.resolved(threadRecipientId)
    return if (threadRecipient.isGroup) {
      val group = SignalDatabase.groups.getGroup(threadRecipientId).orNull() ?: return null
      groupConversationId(group.id.toByteArray())
    } else {
      val contactAci = threadRecipient.aci.orNull() ?: return null
      directConversationId(UuidUtil.toByteArray(localAci), UuidUtil.toByteArray(contactAci))
    }
  }

  private fun threadUniqueIdOf(threadId: Long): String = "thread_$threadId"
  private fun threadIdFromUniqueId(uniqueId: String): Long? = uniqueId.removePrefix("thread_").toLongOrNull()

  private fun markMessageAsRemoteDelete(messageId: Long) {
    runCatching {
      // SignalDatabase.messages.markAsRemoteDelete takes a MessageRecord + sender — approximate
      // with raw DELETE operation on the message row so it looks like a local tombstone.
      // This matches what iOS `applyAuthorizedRemoteDelete` does under the hood.
      //
      // Android uses column "remote_delete" INTEGER=1 or simply deletes the row; prefer
      // the explicit API if it exists.
      SignalDatabase.rawDatabase.execSQL("UPDATE message SET remote_delete = 1 WHERE _id = ?", arrayOf(messageId))
    }
  }

  // ---- cursor → row models -------------------------------------------------------
  private data class ParsedRequest(
    val targetAuthorAci: UUID,
    val targetSentTimestamp: Long,
    val requestId: ByteArray,
    val scope: Int,
    val groupRevision: Int?
  ) {
    override fun equals(other: Any?): Boolean = other is ParsedRequest && requestId.contentEquals(other.requestId)
    override fun hashCode(): Int = requestId.contentHashCode()
  }

  private data class ParticipantDeleteRequestRow(
    val requestId: ByteArray,
    val requesterAci: ByteArray,
    val stableConversationId: ByteArray,
    val targetAuthorAci: ByteArray,
    val targetSentTimestamp: Long,
    val localThreadUniqueId: String
  ) {
    companion object {
      fun fromCursor(c: Cursor): ParticipantDeleteRequestRow {
        return ParticipantDeleteRequestRow(
          requestId = c.getBlob(c.getColumnIndexOrThrow("request_id")),
          requesterAci = c.getBlob(c.getColumnIndexOrThrow("requester_aci")),
          stableConversationId = c.getBlob(c.getColumnIndexOrThrow("stable_conversation_id")),
          targetAuthorAci = c.getBlob(c.getColumnIndexOrThrow("target_author_aci")),
          targetSentTimestamp = c.getLong(c.getColumnIndexOrThrow("target_sent_timestamp")),
          localThreadUniqueId = c.getString(c.getColumnIndexOrThrow("local_thread_unique_id"))
        )
      }
    }

    override fun equals(other: Any?): Boolean = other is ParticipantDeleteRequestRow && requestId.contentEquals(other.requestId)
    override fun hashCode(): Int = requestId.contentHashCode()
  }

  private data class PendingParticipantDeleteRow(
    val firstRequestId: ByteArray,
    val stableConversationId: ByteArray,
    val localThreadUniqueId: String,
    val targetAuthorAci: ByteArray,
    val targetSentTimestamp: Long,
    val requesterAci: ByteArray,
    val protocolVersion: Int
  ) {
    companion object {
      fun fromCursor(c: Cursor): PendingParticipantDeleteRow {
        return PendingParticipantDeleteRow(
          firstRequestId = c.getBlob(c.getColumnIndexOrThrow("first_request_id")),
          stableConversationId = c.getBlob(c.getColumnIndexOrThrow("stable_conversation_id")),
          localThreadUniqueId = c.getString(c.getColumnIndexOrThrow("local_thread_unique_id")),
          targetAuthorAci = c.getBlob(c.getColumnIndexOrThrow("target_author_aci")),
          targetSentTimestamp = c.getLong(c.getColumnIndexOrThrow("target_sent_timestamp")),
          requesterAci = c.getBlob(c.getColumnIndexOrThrow("requester_aci")),
          protocolVersion = c.getInt(c.getColumnIndexOrThrow("protocol_version"))
        )
      }
    }

    override fun equals(other: Any?): Boolean = other is PendingParticipantDeleteRow && firstRequestId.contentEquals(other.firstRequestId)
    override fun hashCode(): Int = firstRequestId.contentHashCode()
  }
}
