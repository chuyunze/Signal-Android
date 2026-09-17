package org.thoughtcrime.securesms.database.helpers.migration

import android.app.Application
import org.thoughtcrime.securesms.database.SQLiteDatabase

/**
 * Creates 5 tables backing cooperative participant-message deletion, ported from iOS commit 69077f4.
 *
 * Unlike admin delete (only group admins can delete others' messages, takes effect instantly),
 * participant delete is a multi-party protocol: any current conversation member can request
 * deletion of any message, and the request only applies once *every* current member has
 * confirmed (via receipt). See iOS AdminDeleteManager.swift → ParticipantDeleteManager.
 *
 * Tables created:
 * - participant_delete_request       (per-request bookkeeping, receipts aggregation target)
 * - participant_delete_tombstone     (already-applied deletes, matched by conversation+author+timestamp)
 * - pending_participant_delete       (waiting for receipts; expires after ~45 days)
 * - participant_delete_device_receipt (per-device acknowledgment records)
 * - participant_delete_author        (metadata: *who* requested a tombstone, for UI display)
 *
 * NOTE: We intentionally do NOT add REFERENCES constraints here. Signal Android migrations
 * historically create FK-less tables for simplicity; referential integrity is enforced by
 * application-layer logic (matching iOS AdminDeleteAuthorRecord which *does* declare FK, but
 * we defer that to a future migration if needed).
 */
@Suppress("ClassName")
object V327_ParticipantDeleteTables : SignalDatabaseMigration {

  override fun migrate(context: Application, db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {

    // --- 1. participant_delete_request ---------------------------------------------------------
    // One row per participant-delete request, keyed by the client-generated 16-byte requestId.
    // Indexed by conversation+target to match incoming messages for the applyPending flow.
    db.execSQL(
      """
      CREATE TABLE participant_delete_request (
          request_id             BLOB PRIMARY KEY NOT NULL,
          requester_aci          BLOB NOT NULL,
          requester_device_id    INTEGER,
          stable_conversation_id BLOB NOT NULL,
          local_thread_unique_id TEXT NOT NULL,
          target_author_aci      BLOB NOT NULL,
          target_sent_timestamp  INTEGER NOT NULL,
          protocol_version       INTEGER NOT NULL,
          processing_result      INTEGER NOT NULL,
          created_at             INTEGER NOT NULL
      )
      """
    )
    db.execSQL(
      """
      CREATE INDEX participant_delete_request_target
          ON participant_delete_request (stable_conversation_id, target_author_aci, target_sent_timestamp)
      """
    )

    // --- 2. participant_delete_tombstone --------------------------------------------------------
    // Applied deletes. When a message matching (stableConvId, targetAuthorAci, timestamp) arrives
    // in the future, applyPendingDeleteIfNecessary turns it into a "deleted by participant" tombstone
    // and links it to the local message row via interaction_id.
    db.execSQL(
      """
      CREATE TABLE participant_delete_tombstone (
          stable_conversation_id BLOB NOT NULL,
          local_thread_unique_id TEXT NOT NULL,
          target_author_aci      BLOB NOT NULL,
          target_sent_timestamp  INTEGER NOT NULL,
          interaction_id         INTEGER,
          first_request_id       BLOB NOT NULL,
          requester_aci          BLOB NOT NULL,
          applied_at             INTEGER NOT NULL,
          protocol_version       INTEGER NOT NULL,
          PRIMARY KEY (stable_conversation_id, target_author_aci, target_sent_timestamp)
      )
      """
    )

    // --- 3. pending_participant_delete ----------------------------------------------------------
    // Active request waiting for receipts from all conversation members. Deletes itself when
    // either everyone has confirmed (moved to tombstone) or expires (default 45 days).
    db.execSQL(
      """
      CREATE TABLE pending_participant_delete (
          first_request_id       BLOB NOT NULL,
          stable_conversation_id BLOB NOT NULL,
          local_thread_unique_id TEXT NOT NULL,
          target_author_aci      BLOB NOT NULL,
          target_sent_timestamp  INTEGER NOT NULL,
          requester_aci          BLOB NOT NULL,
          requester_device_id    INTEGER,
          request_server_timestamp INTEGER NOT NULL,
          conversation_scope     INTEGER NOT NULL,
          group_revision         INTEGER,
          expires_at             INTEGER NOT NULL,
          protocol_version       INTEGER NOT NULL,
          PRIMARY KEY (stable_conversation_id, target_author_aci, target_sent_timestamp)
      )
      """
    )
    db.execSQL(
      """
      CREATE INDEX pending_participant_delete_expires
          ON pending_participant_delete (expires_at)
      """
    )
    db.execSQL(
      """
      CREATE INDEX pending_participant_delete_requester
          ON pending_participant_delete (requester_aci)
      """
    )

    // --- 4. participant_delete_device_receipt ---------------------------------------------------
    // Each member's acknowledgment of a participant-delete request. Composite key ensures
    // one row per (request, member-device) pair. Pruned at apply time by received_at.
    db.execSQL(
      """
      CREATE TABLE participant_delete_device_receipt (
          request_id          BLOB NOT NULL,
          responder_aci       BLOB NOT NULL,
          responder_device_id INTEGER NOT NULL,
          result              INTEGER NOT NULL,
          received_at         INTEGER NOT NULL,
          PRIMARY KEY (request_id, responder_aci, responder_device_id)
      )
      """
    )
    db.execSQL(
      """
      CREATE INDEX participant_delete_device_receipt_received
          ON participant_delete_device_receipt (received_at)
      """
    )

    // --- 5. participant_delete_author -----------------------------------------------------------
    // One row per tombstone, linking the local interaction row to the requester's ACI so the
    // UI can render "Deleted by Alice" alongside the tombstone. Row-level PK mirrors how
    // iOS ParticipantDeleteAuthorRecord keys on interactionId.
    db.execSQL(
      """
      CREATE TABLE participant_delete_author (
          interaction_id INTEGER PRIMARY KEY NOT NULL,
          requester_aci  BLOB NOT NULL
      )
      """
    )
  }
}
