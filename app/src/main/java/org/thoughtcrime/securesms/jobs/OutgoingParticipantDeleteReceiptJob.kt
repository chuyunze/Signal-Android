package org.thoughtcrime.securesms.jobs

import org.signal.core.models.ServiceId
import org.signal.core.util.logging.Log
import org.thoughtcrime.securesms.database.SignalDatabase
import org.thoughtcrime.securesms.database.model.MessageId
import org.thoughtcrime.securesms.database.participantdelete.ParticipantDeleteConfig
import org.thoughtcrime.securesms.dependencies.AppDependencies
import org.thoughtcrime.securesms.jobmanager.Job
import org.thoughtcrime.securesms.jobmanager.impl.NetworkConstraint
import org.thoughtcrime.securesms.jobmanager.impl.SealedSenderConstraint
import org.thoughtcrime.securesms.jobs.protos.OutgoingParticipantDeleteReceiptJobData
import org.thoughtcrime.securesms.keyvalue.SignalStore
import org.thoughtcrime.securesms.messages.GroupSendUtil
import org.thoughtcrime.securesms.recipients.Recipient
import org.thoughtcrime.securesms.recipients.RecipientId
import org.thoughtcrime.securesms.recipients.RecipientUtil
import org.whispersystems.signalservice.api.crypto.ContentHint
import org.whispersystems.signalservice.api.messages.SignalServiceDataMessage
import org.whispersystems.signalservice.api.messages.SignalServiceDataMessage.Companion.newBuilder
import kotlin.time.Duration.Companion.days

/**
 * Sends a single [SignalServiceDataMessage.ParticipantDeleteReceipt] to one specific member.
 *
 * Receipts are always 1-to-1 (group members ACK each delete independently), so we always send
 * to exactly one recipient. Uses [GroupSendUtil.sendResendableDataMessage] with groupId=null
 * for direct-chat delivery (same pattern as [ParticipantDeleteSendJob]).
 */
class OutgoingParticipantDeleteReceiptJob private constructor(
  private val recipientId: Long,
  private val requestId: ByteArray,
  private val resultRaw: Int,
  parameters: Parameters
) : Job(parameters) {

  companion object {
    const val KEY: String = "OutgoingParticipantDeleteReceiptJob"

    private val TAG = Log.tag(OutgoingParticipantDeleteReceiptJob::class.java)

    /** Enqueues a receipt delivery for one conversation member. */
    @JvmStatic
    fun enqueue(recipientAci: ServiceId, requestId: ByteArray, resultRaw: Int) {
      val recipientId = RecipientId.from(recipientAci)
      AppDependencies.jobManager.add(
        OutgoingParticipantDeleteReceiptJob(
          recipientId = recipientId.toLong(),
          requestId = requestId,
          resultRaw = resultRaw,
          parameters = Parameters.Builder()
            .setQueue("ParticipantDeleteReceipt::${recipientId.toLong()}")
            .addConstraint(NetworkConstraint.KEY)
            .addConstraint(SealedSenderConstraint.KEY)
            .setLifespan(1.days.inWholeMilliseconds)
            .setMaxAttempts(Parameters.UNLIMITED)
            .build()
        )
      )
    }
  }

  override fun serialize(): ByteArray? {
    return OutgoingParticipantDeleteReceiptJobData(
      recipientId = recipientId,
      requestId = okio.ByteString.of(*requestId),
      resultRaw = resultRaw
    ).encode()
  }

  override fun getFactoryKey(): String = KEY

  override fun run(): Result {
    if (!SignalStore.account.isRegistered) {
      Log.w(TAG, "Not registered, skip.")
      return Result.failure()
    }

    val recipient = Recipient.resolved(RecipientId.from(recipientId))
    if (!recipient.hasServiceId) {
      Log.w(TAG, "Recipient has no ServiceId, skip.")
      return Result.failure()
    }

    val eligible = RecipientUtil.getEligibleForSending(listOf(recipient))
    if (eligible.isEmpty()) {
      Log.w(TAG, "Recipient not eligible for sending, skip.")
      return Result.failure()
    }

    val dataMessage = newBuilder()
      .withTimestamp(System.currentTimeMillis())
      .withParticipantDeleteReceipt(
        SignalServiceDataMessage.ParticipantDeleteReceipt(
          version = ParticipantDeleteConfig.PROTOCOL_VERSION,
          requestId = requestId,
          result = resultRaw
        )
      )
      .build()

    return try {
      GroupSendUtil.sendResendableDataMessage(
        context,
        null,
        null,
        eligible,
        false,
        ContentHint.RESENDABLE,
        MessageId(-1),
        dataMessage,
        true,
        false,
        null,
        null
      )
      Result.success()
    } catch (e: Exception) {
      Log.w(TAG, "send failed", e)
      Result.retry(defaultBackoff())
    }
  }

  override fun onFailure() {
    Log.w(TAG, "Receipt send failed.")
  }

  class Factory : Job.Factory<OutgoingParticipantDeleteReceiptJob> {
    override fun create(parameters: Parameters, serializedData: ByteArray?): OutgoingParticipantDeleteReceiptJob {
      val data = OutgoingParticipantDeleteReceiptJobData.ADAPTER.decode(serializedData!!)
      return OutgoingParticipantDeleteReceiptJob(
        recipientId = data.recipientId,
        requestId = data.requestId.toByteArray(),
        resultRaw = data.resultRaw,
        parameters = parameters
      )
    }
  }
}
