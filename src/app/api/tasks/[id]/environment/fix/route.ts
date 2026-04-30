import { exec, type ExecException } from 'child_process';
import { NextRequest, NextResponse } from 'next/server';
import { queryAll, queryOne, run } from '@/lib/db';
import { broadcast } from '@/lib/events';
import { dispatchTaskFromServer } from '@/lib/server-dispatch';
import {
  suggestEnvironmentFixCommand,
  type EnvironmentCommandSuggestion,
} from '@/lib/environment-command-suggestion';
import {
  classifyEnvironmentIssueFromTexts,
  hasEnvironmentIssueCommand,
  type EnvironmentIssue,
  type EnvironmentIssueCode,
} from '@/lib/environment-issues';
import type { Task } from '@/lib/types';

export const dynamic = 'force-dynamic';

const COMMAND_TIMEOUT_MS = 10 * 60 * 1000;
const RUNNING_STALE_MS = COMMAND_TIMEOUT_MS + 60 * 1000;
const MAX_OUTPUT_CHARS = 6000;

const runningEnvironmentFixes = new Map<string, { command: string; startedAt: string }>();

interface RecentActivity {
  created_at?: string;
  activity_type?: string;
  message: string;
  metadata?: string | null;
}

interface EnvironmentFixActivity {
  created_at: string;
  activity_type: string;
  message: string;
  metadata?: string | null;
}

interface EnvironmentFixRun {
  task: Task;
  issue: EnvironmentIssue;
  suggestion: EnvironmentCommandSuggestion | null;
  command: string;
  commandSource: string;
  retry: boolean;
}

function compactOutput(value: string | Buffer | undefined): string {
  const text = Buffer.isBuffer(value) ? value.toString('utf8') : value || '';
  return text.length > MAX_OUTPUT_CHARS ? text.slice(-MAX_OUTPUT_CHARS) : text;
}

function formatCommandFailure(
  command: string,
  error: ExecException,
  stdout: string | Buffer | undefined,
  stderr: string | Buffer | undefined
): string {
  const stderrText = compactOutput(stderr);
  const stdoutText = compactOutput(stdout);
  const timeoutText = error.killed
    ? `Command did not finish before the ${Math.round(COMMAND_TIMEOUT_MS / 1000)}s timeout and was stopped.`
    : '';
  const detail = [stderrText, stdoutText, timeoutText, error.message].filter(Boolean).join('\n').trim();
  return detail || `Command failed: ${command}`;
}

function collectIssueText(task: Task, activities: RecentActivity[], requestText?: string): Array<string | null | undefined> {
  return [
    requestText,
    task.status_reason,
    task.planning_dispatch_error,
    ...activities.flatMap((activity) => [activity.message, activity.metadata || undefined]),
  ];
}

function recordActivity(taskId: string, agentId: string | null, type: string, message: string, metadata?: Record<string, unknown>) {
  run(
    `INSERT INTO task_activities (id, task_id, agent_id, activity_type, message, metadata, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`,
    [crypto.randomUUID(), taskId, agentId, type, message, metadata ? JSON.stringify(metadata) : null, new Date().toISOString()]
  );
}

function getRunningEnvironmentFix(taskId: string): { command: string; startedAt: string } | null {
  const inMemory = runningEnvironmentFixes.get(taskId);
  if (inMemory) return inMemory;

  const latest = queryOne<EnvironmentFixActivity>(
    `SELECT created_at, activity_type, message, metadata
     FROM task_activities
     WHERE task_id = ?
       AND activity_type IN ('environment_fix_started', 'environment_fix_failed', 'environment_fix_completed', 'environment_fix_retry_failed')
     ORDER BY created_at DESC
     LIMIT 1`,
    [taskId]
  );

  if (!latest || latest.activity_type !== 'environment_fix_started') return null;

  const startedAtMs = new Date(latest.created_at).getTime();
  if (!Number.isFinite(startedAtMs) || Date.now() - startedAtMs > RUNNING_STALE_MS) return null;

  let command = latest.message.replace(/^Running approved environment command:\s*/i, '').trim();
  try {
    const metadata = latest.metadata ? JSON.parse(latest.metadata) as { command?: string } : null;
    if (metadata?.command) command = metadata.command;
  } catch {
    // Ignore malformed historical metadata; the activity message still carries the command.
  }

  return { command, startedAt: latest.created_at };
}

function updateTaskState(taskId: string, statusReason: string, planningError?: string | null): Task | null {
  if (planningError === undefined) {
    run(
      `UPDATE tasks
       SET status_reason = ?,
           updated_at = ?
       WHERE id = ?`,
      [statusReason, new Date().toISOString(), taskId]
    );
  } else {
    run(
      `UPDATE tasks
       SET planning_dispatch_error = ?,
           status_reason = ?,
           updated_at = ?
       WHERE id = ?`,
      [planningError, statusReason, new Date().toISOString(), taskId]
    );
  }

  const updatedTask = queryOne<Task>('SELECT * FROM tasks WHERE id = ?', [taskId]);
  if (updatedTask) broadcast({ type: 'task_updated', payload: updatedTask });
  return updatedTask ?? null;
}

async function finishEnvironmentFix(
  runConfig: EnvironmentFixRun,
  error: ExecException | null,
  stdout: string | Buffer | undefined,
  stderr: string | Buffer | undefined
) {
  const { task, issue, suggestion, command, retry } = runConfig;
  runningEnvironmentFixes.delete(task.id);

  if (error) {
    const message = formatCommandFailure(command, error, stdout, stderr);
    updateTaskState(
      task.id,
      `Environment fix failed: ${issue.title}`,
      `Environment fix failed (${issue.code}): ${message}`
    );

    recordActivity(task.id, task.assigned_agent_id, 'environment_fix_failed', `Environment fix failed: ${issue.title}`, {
      issue,
      suggestion,
      command,
      error: message,
    });
    return;
  }

  const stdoutText = compactOutput(stdout);
  const stderrText = compactOutput(stderr);
  recordActivity(task.id, task.assigned_agent_id, 'environment_fix_completed', `Environment fix completed: ${issue.title}`, {
    issue,
    suggestion,
    command,
    stdout: stdoutText,
    stderr: stderrText,
  });

  updateTaskState(
    task.id,
    retry
      ? `Environment fix completed: ${issue.title}. Retrying assigned agent.`
      : `Environment fix completed: ${issue.title}.`,
    null
  );

  if (!retry) return;

  const retryResult = await dispatchTaskFromServer(task.id);
  if (!retryResult.success) {
    updateTaskState(
      task.id,
      `Environment fix completed: ${issue.title}, but retry failed.`,
      retryResult.error || 'Environment fixed, but retry failed.'
    );
    recordActivity(task.id, task.assigned_agent_id, 'environment_fix_retry_failed', `Environment fix retry failed: ${issue.title}`, {
      issue,
      suggestion,
      command,
      error: retryResult.error || 'Environment fixed, but retry failed.',
    });
    return;
  }

  const updatedTask = queryOne<Task>('SELECT * FROM tasks WHERE id = ?', [task.id]);
  if (updatedTask) broadcast({ type: 'task_updated', payload: updatedTask });
}

function startEnvironmentFix(runConfig: EnvironmentFixRun): { command: string; startedAt: string } {
  const startedAt = new Date().toISOString();
  const { task, issue, suggestion, command, commandSource } = runConfig;
  runningEnvironmentFixes.set(task.id, { command, startedAt });

  recordActivity(task.id, task.assigned_agent_id, 'environment_fix_started', `Running approved environment command: ${command}`, {
    issue,
    suggestion,
    command,
    commandSource,
    timeoutMs: COMMAND_TIMEOUT_MS,
  });

  updateTaskState(task.id, `Environment fix running: ${issue.title}`);

  exec(
    command,
    {
      cwd: task.workspace_path || undefined,
      timeout: COMMAND_TIMEOUT_MS,
      killSignal: 'SIGTERM',
      maxBuffer: 1024 * 1024 * 8,
      env: process.env,
    },
    (error, stdout, stderr) => {
      void finishEnvironmentFix(runConfig, error, stdout, stderr).catch((finishError) => {
        runningEnvironmentFixes.delete(task.id);
        console.error('[Environment Fix] Failed to finish background command:', finishError);
      });
    }
  );

  return { command, startedAt };
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id: taskId } = await params;

  try {
    const body = await request.json().catch(() => ({} as {
      code?: EnvironmentIssueCode;
      retry?: boolean;
      reason?: string;
      approvedCommand?: string;
      userProvidedCommand?: boolean;
      autoSuggestCommand?: boolean;
    }));
    const task = queryOne<Task>('SELECT * FROM tasks WHERE id = ?', [taskId]);

    if (!task) {
      return NextResponse.json({ error: 'Task not found' }, { status: 404 });
    }

    if (task.status === 'done') {
      return NextResponse.json({ error: 'Completed tasks cannot be retried.' }, { status: 409 });
    }

    const activities = queryAll<RecentActivity>(
      `SELECT created_at, activity_type, message, metadata
       FROM task_activities
       WHERE task_id = ?
         AND activity_type IN ('environment_blocked', 'status_changed')
       ORDER BY created_at DESC
       LIMIT 10`,
      [taskId]
    );
    const issue = classifyEnvironmentIssueFromTexts(collectIssueText(task, activities, body.reason));

    if (!issue) {
      return NextResponse.json({
        error: 'No known environment issue is currently recorded for this task.',
      }, { status: 400 });
    }

    if (body.code && body.code !== issue.code) {
      return NextResponse.json({
        error: `Recorded issue is ${issue.code}, not ${body.code}. Refresh the task and try again.`,
        issue,
      }, { status: 409 });
    }

    const runningFix = getRunningEnvironmentFix(taskId);
    if (runningFix) {
      const runningTask = task.status_reason?.toLowerCase().startsWith('environment fix running:')
        ? task
        : updateTaskState(taskId, `Environment fix running: ${issue.title}`);

      return NextResponse.json({
        success: true,
        running: true,
        fixed: false,
        retried: false,
        issue,
        suggestion: null,
        fix: { command: runningFix.command, startedAt: runningFix.startedAt },
        task: runningTask,
      }, { status: 202 });
    }

    const approvedCommand = body.approvedCommand?.trim();
    let commandToRun = approvedCommand;
    let commandSource = hasEnvironmentIssueCommand(issue) ? issue.action.commandSource || 'detected' : 'user_input';
    let suggestion: EnvironmentCommandSuggestion | null = null;

    if (!commandToRun && body.autoSuggestCommand) {
      try {
        suggestion = await suggestEnvironmentFixCommand({
          task,
          issue,
          activities,
          requestText: body.reason,
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Failed to suggest an environment command';
        return NextResponse.json({
          error: `Could not determine a setup command: ${message}`,
          issue,
        }, { status: 502 });
      }

      if (suggestion.command) {
        commandToRun = suggestion.command;
        commandSource = 'agent_suggestion';
      }
    }

    if (!commandToRun) {
      return NextResponse.json({
        error: 'Mission Control could not determine a setup command to run.',
        issue,
        suggestion,
      }, { status: 409 });
    }

    if (approvedCommand && hasEnvironmentIssueCommand(issue) && approvedCommand !== issue.action.command) {
      return NextResponse.json({
        error: 'The command must be explicitly approved and must match the command shown in the UI.',
        issue,
      }, { status: 409 });
    }

    if (!hasEnvironmentIssueCommand(issue) && !body.userProvidedCommand && !body.autoSuggestCommand) {
      return NextResponse.json({
        error: 'Manual environment fixes require a user-provided or agent-suggested command.',
        issue,
      }, { status: 409 });
    }

    const fixRun = startEnvironmentFix({
      task,
      issue,
      suggestion,
      command: commandToRun,
      commandSource,
      retry: body.retry !== false,
    });
    const updatedTask = queryOne<Task>('SELECT * FROM tasks WHERE id = ?', [taskId]);

    return NextResponse.json({
      success: true,
      running: true,
      started: true,
      fixed: false,
      retried: false,
      issue,
      suggestion,
      fix: fixRun,
      task: updatedTask,
    }, { status: 202 });
  } catch (error) {
    console.error('[Environment Fix] Failed:', error);
    return NextResponse.json({
      error: error instanceof Error ? error.message : 'Environment fix failed',
    }, { status: 500 });
  }
}
