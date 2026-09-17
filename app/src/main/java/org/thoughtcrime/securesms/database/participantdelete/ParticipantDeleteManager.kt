package org.thoughtcrime.securesms.database.participantdelete

import android.database.Cursor
import org.signal.core.models.ServiceId
import org.signal.core.util.Hex
import org.signal.core.util.UuidUtil
import org.signal.core.util.logging.Log
import org.signal.core.util.orNull
import org.thoughtcrime.securesms.database.SignalDatabase
import org.thoughtcrime.securesms.database.model.MessageRecord
import org.thoughtcrime.securesms.recipients.Recipient
import org.thoughtcrime.securesms.recipients.RecipientId
import java.util.UUID

/**
 * Participant-delete protocol configuration. Mirrors iOS `ParticipantDeleteConfiguration`.
 */
object ParticipantDeleteConfig {
  const val PROTOCOL_VERSION: Int = 1
  const val RECEIPT_LIFETIME_MS: Long = 45L * 24 * 60 * 60 * 1000
  const val PENDING_LIFETIME_MS: Long = 45L * 24 * 60 * 60 * 1000
  const val MAX_PENDING_GLOBALLY: Int = 1000
  const val MAX_PENDING_PER_REQUESTER: Int = 50
  const val MAX_PENDING_PER_CONVERSATION: Int = 100
  const val MAX_ORPHAN_RECEIPTS: Int = 2000
  const val FEATURE_ENABLED: Boolean = true
}

/**
 * Mirror of iOS `SSKProtoDataMessageParticipantDeleteReceipt.Result`.
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

/** Why a process() / processLocalInitiation() call may short-circuit. */
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
 * Where a ParticipantDelete request originated.
 */
data class ParticipantDeleteOrigin(
  val requesterAci: UUID,
  val sourceDeviceId: Int?,
  val shouldSendReceipt: Boolean
) {
  companion object {
    fun remote(requesterAci: UUID, sourceDeviceId: Int) = ParticipantDeleteOrigin(requesterAci, sourceDeviceId, shouldSendReceipt = true)
    fun localSentTranscript(localAci: UUID, sourceDeviceId: Int) = ParticipantDeleteOrigin(localAci, sourceDeviceId, shouldSendReceipt = false)
    fun localInitiation(localAci: UUID) = ParticipantDeleteOrigin(localAci, sourceDeviceId = null, shouldSendReceipt = false)
  }
}

// =============================================================================
// Extension helpers
// =============================================================================

/** Convert whatever Signal's .aci returns (Optional<ACI>.orNull() -> ACI?, which is a ServiceId subclass) to UUID. */
private fun Any?.toUuid(): UUID? = when (val v = this) {
  is ServiceId -> runCatching { UuidUtil.parseOrThrow(v.toByteArray()) }.getOrNull()
  else -> null
}

// =============================================================================
// DAO helpers — raw SQL. All blob columns are queried via HEX(col) = ? with
// Hex.toStringCondensed() arg, because Android SQLite selectionArgs is Array<String>.
// =============================================================================

private object DAO {

  // --- participant_delete_request -----------------------------------------------------------

  fun requestByRequestId(requestId: ByteArray): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_request", null,
      "HEX(request_id) = UPPER(?)", arrayOf(Hex.toStringCondensed(requestId)),
      null, null, null
    )
  }

  fun insertRequest(values: android.content.ContentValues) {
    SignalDatabase.rawDatabase.insert("participant_delete_request", null, values)
  }

  fun updateProcessingResult(requestId: ByteArray, resultRaw: Int) {
    val cv = android.content.ContentValues().apply { put("processing_result", resultRaw) }
    SignalDatabase.rawDatabase.update("participant_delete_request", cv, "HEX(request_id) = UPPER(?)", arrayOf(Hex.toStringCondensed(requestId)))
  }

  fun requestsByTarget(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_request", null,
      "HEX(stable_conversation_id) = UPPER(?) AND HEX(target_author_aci) = UPPER(?) AND target_sent_dateSent = ?",
      arrayOf(Hex.toStringCondensed(stableConvId), Hex.toStringCondensed(targetAuthorAci), targetTimestamp.toString()),
      null, null, null
    )
  }

  // --- participant_delete_tombstone ----------------------------------------------------------

  fun tombstoneExists(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Boolean {
    SignalDatabase.rawDatabase.rawQuery(
      "SELECT 1 FROM participant_delete_tombstone " +
        "WHERE HEX(stable_conversation_id) = UPPER(?) AND HEX(target_author_aci) = UPPER(?) AND target_sent_dateSent = ? LIMIT 1",
      arrayOf(Hex.toStringCondensed(stableConvId), Hex.toStringCondensed(targetAuthorAci), targetTimestamp.toString())
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
      "HEX(stable_conversation_id) = UPPER(?) AND HEX(target_author_aci) = UPPER(?) AND target_sent_dateSent = ?",
      arrayOf(Hex.toStringCondensed(stableConvId), Hex.toStringCondensed(targetAuthorAci), targetTimestamp.toString())
    )
  }

  fun tombstoneByTarget(stableConvId: ByteArray, targetAuthorAci: ByteArray, targetTimestamp: Long): Cursor {
    return SignalDatabase.rawDatabase.query(
      "participant_delete_tombstone", null,
      "HEX(stable_conversation_id) = UPPER(?) AND HEX(target_author_aci) = UPPER(?) AND target_sent_dateSent = ?",
      arrayOf(Hex.toStringCondensed(stableConvId), Hex.toStringCondensed(targetAuthorAci), targetTimestamp.toString()),
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
      "HEX(stable_conversation_id) = UPPER(?) AND HEX(target_author_aci) = UPPER(?) AND target_sent_dateSent = ?",
      arrayOf(Hex.toStringCondensed(stableConvId), Hex.toStringCondensed(targetAuthorAci), targetTimestamp.toString()),
      null, null, null
    )
  }

  fun countPendingByRequester(requesterAci: ByteArray): Int {
    SignalDatabase.rawDatabase.rawQuery(
      "SELECT COUNT(*) FROM pending_participant_delete WHERE HEX(requester_aci) = UPPER(?)",
      arrayOf(Hex.toStringCondensed(requesterAci))
    ).use { c -> c.moveToFirst(); return c.getInt(0) }
  }

  fun countPendingByConversation(stableConvId: ByteArray): Int {
    SignalDatabase.rawDatabase.rawQuery(
      "SELECT COUNT(*) FROM pending_participant_delete WHERE HEX(stable_conversation_id) = UPPER(?)",
      arrayOf(Hex.toStringCondensed(stableConvId))
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
      "HEX(stable_conversation_id) = UPPER(?) AND HEX(target_author_aci) = UPPER(?) AND target_sent_dateSent = ?",
      arrayOf(Hex.toStringCondensed(stableConvId), Hex.toStringCondensed(targetAuthorAci), targetTimestamp.toString())
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
      "HEX(request_id) = UPPER(?)", arrayOf(Hex.toStringCondensed(requestId)),
      null, null, null
    )
  }

  fun orphanReceiptCount(): Int {
    SignalDatabase.rawDatabase.rawQuery(
      """
        SELECT COUNT(*) FROM participant_delete_device_receipt AS r
        WHERE NOT EXISTS (SELECT 1 FROM participant_delete_request AS q WHERE HEX(q.request_id) = HEX(r.request_id))
      """.trimIndent(), null
    ).use { c -> c.moveToFirst(); return c.getInt(0) }
  }

  fun deleteDeviceReceipt(requestId: ByteArray, responderAci: ByteArray, responderDeviceId: Long) {
    SignalDatabase.rawDatabase.delete(
      "participant_delete_device_receipt",
      "HEX(request_id) = UPPER(?) AND HEX(responder_aci) = UPPER(?) AND responder_device_id = ?",
      arrayOf(Hex.toStringCondensed(requestId), Hex.toStringCondensed(responderAci), responderDeviceId.toString())
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
// Public manager
// =============================================================================

class ParticipantDeleteManager {

  companion object {
    private val TAG = Log.tag(ParticipantDeleteManager::class.java)

    const val SCOPE_DIRECT_CHAT_BOTH_ACCOUNTS = 1
    const val SCOPE_GROUP_ALL_CURRENT_MEMBERS = 2

    fun directConversationId(localAci: ByteArray, contactAci: ByteArray): ByteArray {
      val prefix = byteArrayOf(0x01)
      val (lo, hi) = if (byteArrayCompare(localAci, contactAci) <= 0) localAci to contactAci else contactAci to localAci
      return prefix + lo + hi
    }

    fun groupConversationId(groupId: ByteArray): ByteArray = byteArrayOf(0x02) + groupId

    private fun byteArrayCompare(a: ByteArray, b: ByteArray): Int {
      val n = minOf(a.size, b.size)
      for (i in 0 until n) {
        val diff = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
        if (diff != 0) return diff
      }
      return a.size - b.size
    }

    fun isSupportedTarget(message: MessageRecord): Boolean {
      if (message.isViewOnce) return false
      return true
    }

    fun authorAci(message: MessageRecord, localAci: UUID): UUID? {
      return if (message.isOutgoing) localAci else Recipient.resolved(message.fromRecipient.id).aci.toUuid()
    }

    /** Mirrors iOS queueReceipt — enqueue a job to send our receipt back to the requester. */
    fun queueReceipt(requestId: ByteArray, resultRaw: Int, recipientAci: UUID) {
      org.thoughtcrime.securesms.jobs.OutgoingParticipantDeleteReceiptJob.enqueue(
        recipientAci = ServiceId.ACI.from(recipientAci),
        requestId = requestId,
        resultRaw = resultRaw
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  fun canParticipantDelete(message: MessageRecord, threadId: Long): Boolean {
    if (!ParticipantDeleteConfig.FEATURE_ENABLED) return false
    if (message.isRemoteDelete) return false
    if (message.dateSent <= 0) return false
    if (!isSupportedTarget(message)) return false

    val localAci = Recipient.self().aci.toUuid() ?: return false
    val authorAci = authorAci(message, localAci) ?: return false

    if (threadId <= 0) return false

    val threadRecipientId: RecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId) ?: return false
    val threadRecipient = Recipient.resolved(threadRecipientId)
    return if (threadRecipient.isGroup) {
      val group = SignalDatabase.groups.getGroup(threadRecipientId).orNull() ?: return false
      group.members.contains(Recipient.self().id)
    } else {
      val contactAci = threadRecipient.aci.toUuid() ?: return false
      authorAci == localAci || authorAci == contactAci
    }
  }

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
    val localAci = Recipient.self().aci.toUuid() ?: throw ParticipantDeleteException.InvalidThread
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
    val resultRaw = proto.result?.value ?: run {
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

      if (row != null) {
        tryResolvePending(row!!)
      }

      SignalDatabase.rawDatabase.setTransactionSuccessful()
    } finally {
      SignalDatabase.rawDatabase.endTransaction()
    }
  }

  fun applyPendingDeleteIfNecessary(message: MessageRecord) {
    try {
      val localAci = Recipient.self().aci.toUuid() ?: return
      val authorAci = authorAci(message, localAci) ?: return
      val stableConvId = stableConversationIdForThread(message.threadId, localAci) ?: return
      if (!isSupportedTarget(message)) return

      SignalDatabase.rawDatabase.beginTransaction()
      try {
        if (DAO.tombstoneExists(stableConvId, UuidUtil.toByteArray(authorAci), message.dateSent)) {
          DAO.tombstoneByTarget(stableConvId, UuidUtil.toByteArray(authorAci), message.dateSent).use { c ->
            if (c.moveToFirst()) {
              val tombstoneAciBytes = c.getBlob(c.getColumnIndexOrThrow("requester_aci"))
              DAO.updateTombstoneInteractionId(
                stableConvId, UuidUtil.toByteArray(authorAci), message.dateSent,
                message.id, threadUniqueIdOf(message.threadId)
              )
              DAO.insertAuthor(message.id, tombstoneAciBytes)
            }
          }
          markMessageAsRemoteDelete(message.id)
          SignalDatabase.rawDatabase.setTransactionSuccessful()
          return
        }

        DAO.pendingByTarget(stableConvId, UuidUtil.toByteArray(authorAci), message.dateSent).use { c ->
          if (c.moveToFirst()) {
            val pendingRow = PendingParticipantDeleteRow.fromCursor(c)
            DAO.insertTombstone(android.content.ContentValues().apply {
              put("stable_conversation_id", pendingRow.stableConversationId)
              put("local_thread_unique_id", threadUniqueIdOf(message.threadId))
              put("target_author_aci", pendingRow.targetAuthorAci)
              put("target_sent_dateSent", message.dateSent)
              put("interaction_id", message.id)
              put("first_request_id", pendingRow.firstRequestId)
              put("requester_aci", pendingRow.requesterAci)
              put("applied_at", System.currentTimeMillis())
              put("protocol_version", pendingRow.protocolVersion)
            })
            DAO.deletePendingByTarget(stableConvId, pendingRow.targetAuthorAci, message.dateSent)
            DAO.insertAuthor(message.id, pendingRow.requesterAci)
            DAO.requestsByTarget(stableConvId, pendingRow.targetAuthorAci, message.dateSent).use { rc ->
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
  // Core state machine
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
    val localAci = Recipient.self().aci.toUuid() ?: throw ParticipantDeleteException.InvalidThread

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
      val requesterRecipientId = RecipientId.from(ServiceId.ACI.from(requesterAci))
      if (!group.members.contains(requesterRecipientId)) {
        throw ParticipantDeleteException.RequesterNotCurrentMember
      }
      stableConvId = groupConversationId(group.id.decodedId)
    } else {
      if (scope != SCOPE_DIRECT_CHAT_BOTH_ACCOUNTS) throw ParticipantDeleteException.ScopeMismatch
      val contactAci = threadRecipient.aci.toUuid() ?: throw ParticipantDeleteException.InvalidThread
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

    val requesterAciBytes = UuidUtil.toByteArray(requesterAci)
    val localThreadUniqueId = threadUniqueIdOf(threadId)

    SignalDatabase.rawDatabase.beginTransaction()
    try {
      // Dedupe
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

      // Tombstone shortcut
      val targetAuthorAciBytes = UuidUtil.toByteArray(targetAuthorAci)
      if (DAO.tombstoneExists(stableConvId, targetAuthorAciBytes, targetSentTimestamp)) {
        DAO.insertRequest(android.content.ContentValues().apply {
          put("request_id", requestId)
          put("requester_aci", requesterAciBytes)
          requesterDeviceId?.let { put("requester_device_id", it.toLong()) }
          put("stable_conversation_id", stableConvId)
          put("local_thread_unique_id", localThreadUniqueId)
          put("target_author_aci", targetAuthorAciBytes)
          put("target_sent_dateSent", targetSentTimestamp)
          put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
          put("processing_result", ParticipantDeleteReceiptResult.ALREADY_APPLIED.rawValue)
          put("created_at", System.currentTimeMillis())
        })
        if (originShouldSendReceipt) queueReceipt(requestId, ParticipantDeleteReceiptResult.ALREADY_APPLIED.rawValue, requesterAci)
        SignalDatabase.rawDatabase.setTransactionSuccessful()
        return ParticipantDeleteReceiptResult.ALREADY_APPLIED
      }

      // Try to find target message locally
      val targetMessage: MessageRecord? = try {
        SignalDatabase.messages.getMessageFor(targetSentTimestamp, RecipientId.from(ServiceId.ACI.from(targetAuthorAci)))
      } catch (t: Throwable) { null }

      if (targetMessage == null) {
        // Enqueue pending
        val nowMs = System.currentTimeMillis()
        DAO.pruneExpiredPending(nowMs)

        DAO.pendingByTarget(stableConvId, targetAuthorAciBytes, targetSentTimestamp).use { c ->
          if (!c.moveToFirst()) {
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
              put("target_sent_dateSent", targetSentTimestamp)
              put("requester_aci", requesterAciBytes)
              requesterDeviceId?.let { put("requester_device_id", it.toLong()) }
              put("request_server_dateSent", trustedServerTimestamp ?: nowMs)
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
          put("target_sent_dateSent", targetSentTimestamp)
          put("protocol_version", ParticipantDeleteConfig.PROTOCOL_VERSION)
          put("processing_result", ParticipantDeleteReceiptResult.TARGET_PENDING.rawValue)
          put("created_at", System.currentTimeMillis())
        })
        if (originShouldSendReceipt) queueReceipt(requestId, ParticipantDeleteReceiptResult.TARGET_PENDING.rawValue, requesterAci)
        SignalDatabase.rawDatabase.setTransactionSuccessful()
        return ParticipantDeleteReceiptResult.TARGET_PENDING
      }

      // Target message exists → apply
      if (!isSupportedTarget(targetMessage)) {
        throw ParticipantDeleteException.InvalidTarget
      }
      if (targetMessage.threadId != threadId || targetMessage.dateSent != targetSentTimestamp) {
        throw ParticipantDeleteException.InvalidTarget
      }

      if (!targetMessage.isRemoteDelete) {
        markMessageAsRemoteDelete(targetMessage.id)
      }
      DAO.insertTombstone(android.content.ContentValues().apply {
        put("stable_conversation_id", stableConvId)
        put("local_thread_unique_id", localThreadUniqueId)
        put("target_author_aci", targetAuthorAciBytes)
        put("target_sent_dateSent", targetSentTimestamp)
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
        put("target_sent_dateSent", targetSentTimestamp)
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
      try { queueReceipt(requestId, fallback.rawValue, requesterAci) } catch (_: Throwable) { }
      throw pe
    } finally {
      try { SignalDatabase.rawDatabase.endTransaction() } catch (_: Throwable) { }
    }
  }

  /** If every current conversation member has ACKed the pending row, apply it. */
  private fun tryResolvePending(requestRow: ParticipantDeleteRequestRow) {
    val nowMs = System.currentTimeMillis()
    DAO.pruneExpiredPending(nowMs)

    DAO.pendingByTarget(requestRow.stableConversationId, requestRow.targetAuthorAci, requestRow.targetSentTimestamp).use { c ->
      if (!c.moveToFirst()) return
      val pending = PendingParticipantDeleteRow.fromCursor(c)

      val threadId = threadIdFromUniqueId(pending.localThreadUniqueId) ?: return
      val threadRecipientId = SignalDatabase.threads.getRecipientIdForThreadId(threadId) ?: return
      val threadRecipient = Recipient.resolved(threadRecipientId)

      val fullMemberAcis: List<UUID> = if (threadRecipient.isGroup) {
        val group = SignalDatabase.groups.getGroup(threadRecipientId).orNull() ?: return
        val members = group.members.map { Recipient.resolved(it) }
        members.mapNotNull { it.aci.toUuid() }
      } else {
        listOfNotNull(
          Recipient.self().aci.toUuid(),
          threadRecipient.aci.toUuid()
        )
      }

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

      val targetMessage = try {
        SignalDatabase.messages.getMessageFor(pending.targetSentTimestamp, RecipientId.from(ServiceId.ACI.from(UuidUtil.parseOrThrow(pending.targetAuthorAci))))
      } catch (t: Throwable) { null }
      if (targetMessage == null) return

      if (!targetMessage.isRemoteDelete) markMessageAsRemoteDelete(targetMessage.id)
      DAO.insertTombstone(android.content.ContentValues().apply {
        put("stable_conversation_id", pending.stableConversationId)
        put("local_thread_unique_id", pending.localThreadUniqueId)
        put("target_author_aci", pending.targetAuthorAci)
        put("target_sent_dateSent", pending.targetSentTimestamp)
        put("interaction_id", targetMessage.id)
        put("first_request_id", pending.firstRequestId)
        put("requester_aci", pending.requesterAci)
        put("applied_at", System.currentTimeMillis())
        put("protocol_version", pending.protocolVersion)
      })
      DAO.insertAuthor(targetMessage.id, pending.requesterAci)
      DAO.deletePendingByTarget(pending.stableConversationId, pending.targetAuthorAci, pending.targetSentTimestamp)

      DAO.requestsByTarget(pending.stableConversationId, pending.targetAuthorAci, pending.targetSentTimestamp).use { rc ->
        while (rc.moveToNext()) {
          val reqId = rc.getBlob(rc.getColumnIndexOrThrow("request_id"))
          val requesterAciBytes = rc.getBlob(rc.getColumnIndexOrThrow("requester_aci"))
          DAO.updateProcessingResult(reqId, ParticipantDeleteReceiptResult.APPLIED.rawValue)
          val requestingAci = runCatching { UuidUtil.parseOrThrow(requesterAciBytes) }.getOrNull()
          val selfAci = Recipient.self().aci.toUuid()
          if (requestingAci != null && requestingAci != selfAci) {
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
      val members = group.members.map { Recipient.resolved(it) }
      members.any { it.aci.toUuid() == responderAci }
    } else {
      val selfAci = Recipient.self().aci.toUuid()
      val contactAci = threadRecipient.aci.toUuid()
      contactAci == responderAci || selfAci == responderAci
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
      groupConversationId(threadRecipient.requireGroupId().decodedId)
    } else {
      val contactAci = threadRecipient.aci.toUuid() ?: return null
      directConversationId(UuidUtil.toByteArray(localAci), UuidUtil.toByteArray(contactAci))
    }
  }

  private fun threadUniqueIdOf(threadId: Long): String = "thread_$threadId"
  private fun threadIdFromUniqueId(uniqueId: String): Long? = uniqueId.removePrefix("thread_").toLongOrNull()

  private fun markMessageAsRemoteDelete(messageId: Long) {
    runCatching {
      SignalDatabase.rawDatabase.execSQL("UPDATE message SET remote_deleted = 1 WHERE _id = ?", arrayOf(messageId))
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
          targetSentTimestamp = c.getLong(c.getColumnIndexOrThrow("target_sent_dateSent")),
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
          targetSentTimestamp = c.getLong(c.getColumnIndexOrThrow("target_sent_dateSent")),
          requesterAci = c.getBlob(c.getColumnIndexOrThrow("requester_aci")),
          protocolVersion = c.getInt(c.getColumnIndexOrThrow("protocol_version"))
        )
      }
    }

    override fun equals(other: Any?): Boolean = other is PendingParticipantDeleteRow && firstRequestId.contentEquals(other.firstRequestId)
    override fun hashCode(): Int = firstRequestId.contentHashCode()
  }
}
