package org.thoughtcrime.securesms.jobs

import org.signal.core.models.ServiceId
import org.signal.core.util.logging.Log
import org.thoughtcrime.securesms.database.SignalDatabase
import org.thoughtcrime.securesms.dependencies.AppDependencies
import org.thoughtcrime.securesms.jobmanager.Job
import org.thoughtcrime.securesms.jobmanager.impl.NetworkConstraint
import org.thoughtcrime.securesms.jobmanager.impl.SealedSenderConstraint
import org.thoughtcrime.securesms.keyvalue.SignalStore
import org.thoughtcrime.securesms.messages.GroupSendUtil
import org.thoughtcrime.securesms.recipients.Recipient
import org.thoughtcrime.securesms.recipients.RecipientId
import org.thoughtcrime.securesms.recipients.RecipientUtil
import org.thoughtcrime.securesms.util.GroupUtil
import org.whispersystems.signalservice.api.crypto.ContentHint
import org.whispersystems.signalservice.api.messages.SignalServiceDataMessage
import org.whispersystems.signalservice.api.messages.SignalServiceDataMessage.Companion.newBuilder
import kotlin.time.Duration.Companion.days

/**
 * Sends a single [SignalServiceDataMessage.ParticipantDeleteReceipt] to one specific member.
 *
 * Receipts are always 1-to-1 (group members ACK each delete independently), so we always send
 * to exactly one recipient. AdminDeleteSendJob handles multi-recipient fan-out because the
 * delete itself goes to every group member; receipts are point-to-point replies.
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
      AppDependencies.getJobManager().add(
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
      requestId = requestId,
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
          version = org.thoughtcrime.securesms.database.participantdelete.ParticipantDeleteConfig.PROTOCOL_VERSION,
          requestId = requestId,
          result = resultRaw
        )
      )
      .build()

    return try {
      AppDependencies.signalServiceMessageSender.sendIndividualMessage(
        SignalServiceDataMessage.Companion.toContent(dataMessage),
        eligible.first().requireServiceId(),
        GroupUtil.DEFAULT_GROUPS_V2,
        emptyList()
      )
      Result.success()
    } catch (e: Exception) {
      Log.w(TAG, "send failed", e)
      Result.retry(defaultBackoff())
    }
  }

  class Factory : Job.Factory<OutgoingParticipantDeleteReceiptJob?> {
    override fun create(parameters: Parameters, serializedData: ByteArray?): OutgoingParticipantDeleteReceiptJob {
      val data = OutgoingParticipantDeleteReceiptJobData.ADAPTER.decode(serializedData!!)
      return OutgoingParticipantDeleteReceiptJob(
        recipientId = data.recipientId,
        requestId = data.requestId,
        resultRaw = data.resultRaw,
        parameters = parameters
      )
    }
  }
}
