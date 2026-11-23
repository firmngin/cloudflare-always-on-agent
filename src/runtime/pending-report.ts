/**
 * Durable fence for a finished turn whose terminal report was never
 * acknowledged.
 *
 * The paid model/tool work is already done, so re-running the turn only to
 * resend its report would spend the user's key twice: `ClaimMessage` resumes a
 * claim for the same token, so a retried wake re-enters the turn. Instead the
 * report is recorded on the agent's Durable Object and replayed by the next
 * wake for that run.
 */

import type { TerminalRunReport } from './turn'

export interface PendingRunReport extends TerminalRunReport {
  recorded_at: number
}

export interface PendingReportState {
  reports: Record<string, PendingRunReport>
}

/**
 * Bounds how long a fence may be replayed. Past it the backend's stale-run
 * reaper owns the run, so replaying a long-dead answer would only resurrect an
 * outcome the run row no longer reflects.
 */
export const PENDING_REPORT_TTL_MS = 30 * 60_000
/** One entry per unfinished report; older ones are dropped first. */
export const MAX_PENDING_REPORTS = 32
/**
 * A Durable Object value is capped at 128 KiB. A larger answer keeps only its
 * status/error: the backend already stored the streamed `message.final` event
 * and `FinishRun` falls back to it when the report carries no content.
 */
export const MAX_PENDING_CONTENT_BYTES = 48 * 1024

/** The pending report for this run, or null once it expired. */
export function pendingReportFor(
  state: PendingReportState | undefined,
  runId: string,
  now: number,
): PendingRunReport | null {
  const report = state?.reports?.[runId]
  if (!report) return null
  return now - report.recorded_at > PENDING_REPORT_TTL_MS ? null : report
}

export function recordPendingReportState(
  state: PendingReportState | undefined,
  runId: string,
  report: TerminalRunReport,
  now: number,
): PendingReportState {
  const reports = liveReports(state, now)
  // content and output are always the same value in this runtime; store one.
  const content = report.content || report.output || ''
  reports[runId] = {
    status: report.status,
    // undefined fields are dropped by JSON.stringify, and the backend decodes
    // the report strictly: absent is correct, empty is not.
    content: content && utf8Length(content) <= MAX_PENDING_CONTENT_BYTES ? content : undefined,
    error_code: report.error_code || undefined,
    error_message: report.error_message || undefined,
    // Stored, never recomputed: the replay must PATCH the same value.
    tool_loop_hit: report.tool_loop_hit === true,
    recorded_at: now,
  }
  return { reports: boundReports(reports) }
}

export function clearPendingReportState(
  state: PendingReportState | undefined,
  runId: string,
  now: number,
): PendingReportState {
  const reports = liveReports(state, now)
  delete reports[runId]
  return { reports }
}

/** The report actually PATCHed: `recorded_at` is object bookkeeping, never sent. */
export function toTerminalReport(pending: PendingRunReport): TerminalRunReport {
  return {
    status: pending.status,
    content: pending.content || undefined,
    error_code: pending.error_code || undefined,
    error_message: pending.error_message || undefined,
    tool_loop_hit: pending.tool_loop_hit === true,
  }
}

function liveReports(
  state: PendingReportState | undefined,
  now: number,
): Record<string, PendingRunReport> {
  return Object.fromEntries(
    Object.entries(state?.reports || {})
      .filter(([, report]) => now - report.recorded_at <= PENDING_REPORT_TTL_MS),
  )
}

function boundReports(reports: Record<string, PendingRunReport>): Record<string, PendingRunReport> {
  const newest = Object.entries(reports)
    .sort((a, b) => b[1].recorded_at - a[1].recorded_at)
    .slice(0, MAX_PENDING_REPORTS)
  return Object.fromEntries(newest)
}

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).length
}
