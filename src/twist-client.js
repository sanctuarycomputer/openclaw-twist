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
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
        await sleep(delay);
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

    /** Comments on a thread, chronological by obj_index. */
    getThreadComments: (threadId, { limit = 30, signal } = {}) =>
      request("comments/get", { query: { thread_id: threadId, limit }, signal }),

    /** Messages in a conversation, chronological by obj_index. */
    getConversationMessages: (conversationId, { limit = 30, signal } = {}) =>
      request("conversation_messages/get", {
        query: { conversation_id: conversationId, limit },
        signal,
      }),

    /** Thread metadata (channel_id, title, etc.). */
    getThread: (id, signal) => request("threads/getone", { query: { id }, signal }),

    /** Channel metadata (name, etc.). */
    getChannel: (id, signal) => request("channels/getone", { query: { id }, signal }),

    // ---- mutating ----

    /** Post a reply comment to a thread. */
    addThreadComment: (threadId, content, signal) =>
      request("comments/add", { method: "POST", body: { thread_id: threadId, content }, signal }),

    /** Post a reply message to a conversation (DM / group DM). */
    addConversationMessage: (conversationId, content, signal) =>
      request("conversation_messages/add", {
        method: "POST",
        body: { conversation_id: conversationId, content },
        signal,
      }),

    /** Mark a thread read up to obj_index so we stop re-processing it. */
    markThreadRead: (threadId, objIndex, signal) =>
      request("threads/mark_as_read", {
        method: "POST",
        body: { id: threadId, obj_index: objIndex },
        signal,
      }),

    /** Mark a conversation read up to obj_index. */
    markConversationRead: (conversationId, objIndex, signal) =>
      request("conversations/mark_as_read", {
        method: "POST",
        body: { id: conversationId, obj_index: objIndex },
        signal,
      }),

    /**
     * Add an emoji reaction. Target a thread comment with {commentId} or a
     * conversation message with {messageId}.
     */
    addReaction: ({ commentId, messageId, reaction }, signal) =>
      request("reactions/add", {
        method: "POST",
        body: commentId != null ? { comment_id: commentId, reaction } : { message_id: messageId, reaction },
        signal,
      }),

    /** Remove an emoji reaction (same targeting as addReaction). */
    removeReaction: ({ commentId, messageId, reaction }, signal) =>
      request("reactions/remove", {
        method: "POST",
        body: commentId != null ? { comment_id: commentId, reaction } : { message_id: messageId, reaction },
        signal,
      }),
  };
}

/** Extract participant count from a conversation object (handles field variants). */
export function conversationParticipantCount(conv) {
  const ids = conv?.user_ids ?? conv?.participants ?? [];
  return Array.isArray(ids) ? ids.length : 0;
}
