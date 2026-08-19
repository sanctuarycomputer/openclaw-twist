// Thin Twist REST v3 client. No OpenCLAW coupling so the read paths can be
// smoke-tested directly. IMPORTANT: the bearer value includes the literal
// "oauth2:" prefix (Twist returns 403 "Invalid token" without it).

const BASE = "https://api.twist.com/api/v3";

export class TwistError extends Error {
  constructor(message, { status, body } = {}) {
    super(message);
    this.name = "TwistError";
    this.status = status;
    this.body = body;
  }
}

/**
 * @param {object} opts
 * @param {string} opts.token        full token incl. "oauth2:" prefix
 * @param {number|string} opts.workspaceId
 * @param {typeof fetch} [opts.fetchImpl]
 */
export function createTwistClient({ token, workspaceId, fetchImpl = fetch }) {
  if (!token) throw new Error("twist: token is required");
  if (!workspaceId) throw new Error("twist: workspaceId is required");

  const MAX_RETRIES = 3;
  /**
   * Abortable delay. The plain version made the retry ladder un-cancellable: a 429 storm
   * meant up to ~7s of dead sleep per attempt that no abort signal could interrupt, so a
   * caller's deadline could not fire and its slot stayed pinned. Rejects with the signal's
   * own reason (a TimeoutError for AbortSignal.timeout), which callers already classify.
   */
  const sleep = (ms, signal) =>
    new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(signal.reason ?? new Error("aborted"));
        return;
      }
      const onAbort = () => {
        clearTimeout(timer);
        reject(signal.reason ?? new Error("aborted"));
      };
      const timer = setTimeout(() => {
        signal?.removeEventListener?.("abort", onAbort);
        resolve();
      }, ms);
      signal?.addEventListener?.("abort", onAbort, { once: true });
    });

  async function request(path, { method = "GET", query, body, signal } = {}) {
    let url = `${BASE}/${path}`;
    if (query) {
      const qs = new URLSearchParams();
      for (const [k, v] of Object.entries(query)) {
        if (v !== undefined && v !== null) qs.set(k, String(v));
      }
      url += `?${qs.toString()}`;
    }
    const headers = { Authorization: `Bearer ${token}` };
    let payload;
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      payload = JSON.stringify(body);
    }

    for (let attempt = 0; ; attempt++) {
      const res = await fetchImpl(url, { method, headers, body: payload, signal });
      // Retry on rate-limit / transient server errors with backoff.
      if ((res.status === 429 || res.status === 502 || res.status === 503) && attempt < MAX_RETRIES) {
        const retryAfter = Number(res.headers?.get?.("retry-after"));
        const delay = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 2 ** attempt * 1000;
        await sleep(delay, signal);
        continue;
      }
      const text = await res.text();
      let parsed;
      try {
        parsed = text ? JSON.parse(text) : null;
      } catch {
        parsed = text;
      }
      if (!res.ok) {
        const msg =
          parsed && typeof parsed === "object" && parsed.error_string
            ? parsed.error_string
            : `HTTP ${res.status}`;
        throw new TwistError(`twist ${path}: ${msg}`, { status: res.status, body: parsed });
      }
      return parsed;
    }
  }

  return {
    request,

    /** Identity behind the token (id, name, bot, default_workspace). */
    getSessionUser: (signal) => request("users/get_session_user", { signal }),

    /** Unread channel threads: [{thread_id, channel_id, direct_mention, obj_index}]. */
    getUnreadThreads: (signal) =>
      request("threads/get_unread", { query: { workspace_id: workspaceId }, signal }),

    /** Unread conversations (DMs/group DMs): [{conversation_id, direct_mention, obj_index}]. */
    getUnreadConversations: (signal) =>
      request("conversations/get_unread", { query: { workspace_id: workspaceId }, signal }),

    /** All conversations in the workspace (carries participant `user_ids`). */
    getConversations: (signal) =>
      request("conversations/get", { query: { workspace_id: workspaceId }, signal }),

    /** Single conversation (for participant lookup). */
    getConversation: (id, signal) =>
      request("conversations/getone", { query: { id }, signal }),

    /**
     * Comments on a thread.
     *
     * DEFAULT (no `fromObjIndex`): Twist orders `comments/get` **descending**, so a bare
     * `{limit}` call returns the NEWEST `limit` comments — a backlog baseline, not a page.
     * Pass `fromObjIndex` to page FORWARD from a cursor instead: that switches the call to
     * `order_by=asc` + `from_obj_index`, returning the OLDEST `limit` comments at/after that
     * index. VERIFIED LIVE: `from_obj_index` without `order_by=asc` is silently ignored (you
     * get the newest window back), so the two parameters are always sent together.
     */
    getThreadComments: (threadId, { limit = 30, fromObjIndex = null, signal } = {}) =>
      request("comments/get", {
        query: {
          thread_id: threadId,
          limit,
          ...(fromObjIndex != null ? { order_by: "asc", from_obj_index: fromObjIndex } : {}),
        },
        signal,
      }),

    /** Messages in a conversation. Same ordering contract as getThreadComments. */
    getConversationMessages: (conversationId, { limit = 30, fromObjIndex = null, signal } = {}) =>
      request("conversation_messages/get", {
        query: {
          conversation_id: conversationId,
          limit,
          ...(fromObjIndex != null ? { order_by: "asc", from_obj_index: fromObjIndex } : {}),
        },
        signal,
      }),

    /** Thread metadata (channel_id, title, etc.). */
    getThread: (id, signal) => request("threads/getone", { query: { id }, signal }),

    /** Channel metadata (name, etc.). */
    getChannel: (id, signal) => request("channels/getone", { query: { id }, signal }),

    // ---- mutating ----

    /** Post a reply comment to a thread. */
    addThreadComment: (threadId, content, { recipients, signal } = {}) =>
      request("comments/add", {
        method: "POST",
        body: {
          thread_id: threadId,
          content,
          ...(Array.isArray(recipients) && recipients.length ? { recipients: JSON.stringify(recipients) } : {}),
        },
        signal,
      }),

    /** Post a reply message to a conversation (DM / group DM). */
    addConversationMessage: (conversationId, content, signal) =>
      request("conversation_messages/add", {
        method: "POST",
        body: { conversation_id: conversationId, content },
        signal,
      }),

    /** Mark a thread read up to obj_index so we stop re-processing it. */
    markThreadRead: (threadId, objIndex, signal) =>
      request("threads/mark_read", {
        method: "POST",
        body: { id: threadId, obj_index: objIndex },
        signal,
      }),

    /** Mark a conversation read up to obj_index. */
    markConversationRead: (conversationId, objIndex, signal) =>
      request("conversations/mark_read", {
        method: "POST",
        body: { id: conversationId, obj_index: objIndex },
        signal,
      }),

    /**
     * Add an emoji reaction. Targets are mutually exclusive: a thread comment
     * ({commentId}), a conversation message ({messageId}), or a thread's OPENING POST
     * ({threadId}) — reactions/add|remove accept `thread_id` as a first-class target, so
     * an opening post is reactable just like a comment.
     */
    addReaction: (target, signal) =>
      request("reactions/add", { method: "POST", body: reactionBody(target), signal }),

    /** Remove an emoji reaction (same targeting as addReaction). */
    removeReaction: (target, signal) =>
      request("reactions/remove", { method: "POST", body: reactionBody(target), signal }),
  };
}

/** reactions/add|remove body for exactly one target: comment, message, or thread post. */
function reactionBody({ commentId, messageId, threadId, reaction }) {
  if (commentId != null) return { comment_id: commentId, reaction };
  if (messageId != null) return { message_id: messageId, reaction };
  return { thread_id: threadId, reaction };
}

/** Extract participant count from a conversation object (handles field variants). */
export function conversationParticipantCount(conv) {
  const ids = conv?.user_ids ?? conv?.participants ?? [];
  return Array.isArray(ids) ? ids.length : 0;
}
