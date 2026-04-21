import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { ChatInfo, StoredAttachment, StoredMessage, UserInfo } from "./types.js";

const DEFAULT_STORAGE_PATH = path.join(
  os.homedir(),
  ".codex",
  "tg-bot-mcp",
  "storage.json"
);
const MAX_MESSAGES = 500;

type JsonRecord = Record<string, unknown>;

type PersistedState = {
  version: 3;
  nextId: number;
  lastUpdateId: number | null;
  messages: StoredMessage[];
};

type TelegramUser = JsonRecord & {
  id?: number;
  first_name?: string;
  last_name?: string;
  username?: string;
};

type TelegramChat = JsonRecord & {
  id?: number;
  type?: string;
  title?: string;
  username?: string;
  first_name?: string;
  last_name?: string;
};

type TelegramFile = JsonRecord & {
  file_id?: string;
  file_name?: string;
  mime_type?: string;
  duration?: number;
  width?: number;
  height?: number;
  file_size?: number;
  title?: string;
  performer?: string;
  emoji?: string;
  set_name?: string;
  length?: number;
};

type TelegramPoll = JsonRecord & {
  id?: string;
  question?: string;
  total_voter_count?: number;
  is_anonymous?: boolean;
  type?: string;
  allows_multiple_answers?: boolean;
  options?: Array<JsonRecord>;
};

type TelegramMessage = JsonRecord & {
  message_id?: number;
  date?: number;
  edit_date?: number;
  text?: string;
  caption?: string;
  chat?: TelegramChat;
  from?: TelegramUser;
  sender_chat?: TelegramChat;
  reply_to_message?: JsonRecord;
  voice?: TelegramFile;
  audio?: TelegramFile;
  photo?: TelegramFile[];
  video?: TelegramFile;
  video_note?: TelegramFile;
  animation?: TelegramFile;
  document?: TelegramFile;
  sticker?: TelegramFile;
  location?: JsonRecord;
  venue?: JsonRecord;
  contact?: JsonRecord;
  poll?: TelegramPoll;
  dice?: JsonRecord;
  game?: JsonRecord;
  invoice?: JsonRecord;
  successful_payment?: JsonRecord;
  story?: JsonRecord;
};

type TelegramCallbackQuery = JsonRecord & {
  id?: string;
  from?: TelegramUser;
  message?: TelegramMessage;
  data?: string;
  chat_instance?: string;
  inline_message_id?: string;
  game_short_name?: string;
};

type TelegramInlineQuery = JsonRecord & {
  id?: string;
  from?: TelegramUser;
  query?: string;
  offset?: string;
  chat_type?: string;
  location?: JsonRecord;
};

type TelegramChosenInlineResult = JsonRecord & {
  result_id?: string;
  from?: TelegramUser;
  query?: string;
  inline_message_id?: string;
  location?: JsonRecord;
};

type TelegramShippingQuery = JsonRecord & {
  id?: string;
  from?: TelegramUser;
  invoice_payload?: string;
};

type TelegramPreCheckoutQuery = JsonRecord & {
  id?: string;
  from?: TelegramUser;
  currency?: string;
  total_amount?: number;
  invoice_payload?: string;
};

type TelegramChatMemberUpdate = JsonRecord & {
  from?: TelegramUser;
  chat?: TelegramChat;
  date?: number;
  old_chat_member?: JsonRecord;
  new_chat_member?: JsonRecord;
  invite_link?: JsonRecord;
};

type TelegramChatJoinRequest = JsonRecord & {
  from?: TelegramUser;
  chat?: TelegramChat;
  date?: number;
  bio?: string;
  invite_link?: JsonRecord;
};

type TelegramMessageReaction = JsonRecord & {
  user?: TelegramUser;
  actor_chat?: TelegramChat;
  chat?: TelegramChat;
  date?: number;
  message_id?: number;
  old_reaction?: Array<JsonRecord>;
  new_reaction?: Array<JsonRecord>;
};

type TelegramMessageReactionCount = JsonRecord & {
  chat?: TelegramChat;
  date?: number;
  message_id?: number;
  reactions?: Array<JsonRecord>;
};

type TelegramPollAnswer = JsonRecord & {
  poll_id?: string;
  user?: TelegramUser;
  option_ids?: number[];
};

type TelegramDeletedBusinessMessages = JsonRecord & {
  chat?: TelegramChat;
  message_ids?: number[];
};

type TelegramUpdate = JsonRecord & {
  update_id: number;
  message?: TelegramMessage;
  edited_message?: TelegramMessage;
  channel_post?: TelegramMessage;
  edited_channel_post?: TelegramMessage;
  business_message?: TelegramMessage;
  edited_business_message?: TelegramMessage;
  deleted_business_messages?: TelegramDeletedBusinessMessages;
  callback_query?: TelegramCallbackQuery;
  inline_query?: TelegramInlineQuery;
  chosen_inline_result?: TelegramChosenInlineResult;
  shipping_query?: TelegramShippingQuery;
  pre_checkout_query?: TelegramPreCheckoutQuery;
  poll?: TelegramPoll;
  poll_answer?: TelegramPollAnswer;
  my_chat_member?: TelegramChatMemberUpdate;
  chat_member?: TelegramChatMemberUpdate;
  chat_join_request?: TelegramChatJoinRequest;
  message_reaction?: TelegramMessageReaction;
  message_reaction_count?: TelegramMessageReactionCount;
};

type NormalizedIncomingUpdate = {
  chatId: number;
  date: number;
  fromUser: UserInfo;
  kind: string;
  text: string;
  updateType: string;
  telegramMessageId?: number;
  mediaFileId?: string;
  caption?: string;
  attachments?: StoredAttachment[];
  metadata?: Record<string, unknown>;
  raw?: Record<string, unknown>;
};

export class MessageStorage {
  private readonly storagePath: string;
  private messages: StoredMessage[] = [];
  private nextId = 1;
  private lastUpdateId: number | null = null;
  private loaded = false;
  private persistQueue: Promise<void> = Promise.resolve();

  constructor(storagePath = process.env.TELEGRAM_MCP_STORAGE_PATH ?? DEFAULT_STORAGE_PATH) {
    this.storagePath = storagePath;
  }

  async start(): Promise<void> {
    if (this.loaded) {
      return;
    }

    try {
      const raw = await readFile(this.storagePath, "utf8");
      const parsed = JSON.parse(raw) as Partial<PersistedState>;
      this.nextId = typeof parsed.nextId === "number" && parsed.nextId > 0 ? parsed.nextId : 1;
      this.lastUpdateId =
        typeof parsed.lastUpdateId === "number" ? parsed.lastUpdateId : null;
      this.messages = Array.isArray(parsed.messages)
        ? parsed.messages
            .filter((message): message is StoredMessage => {
              return (
                typeof message?.id === "number" &&
                typeof message.chatId === "number" &&
                typeof message.text === "string" &&
                typeof message.date === "number" &&
                Boolean(message.fromUser) &&
                typeof message.fromUser.id === "number" &&
                typeof message.fromUser.firstName === "string" &&
                typeof message.read === "boolean"
              );
            })
            .map((message) => ({
              ...message,
              kind: typeof message.kind === "string" && message.kind ? message.kind : "text",
              updateType:
                typeof message.updateType === "string" && message.updateType
                  ? message.updateType
                  : "message",
              attachments: Array.isArray(message.attachments) ? message.attachments : undefined,
              metadata: isRecord(message.metadata) ? message.metadata : undefined,
              raw: isRecord(message.raw) ? message.raw : undefined,
            }))
            .slice(-MAX_MESSAGES)
        : [];
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException;
      if (nodeError.code !== "ENOENT") {
        throw error;
      }
    }

    this.loaded = true;
  }

  getNextUpdateOffset(): number | undefined {
    return this.lastUpdateId === null ? undefined : this.lastUpdateId + 1;
  }

  async ingestUpdates(updates: TelegramUpdate[]): Promise<void> {
    await this.start();

    let changed = false;

    for (const update of updates) {
      if (typeof update.update_id === "number") {
        this.lastUpdateId = Math.max(this.lastUpdateId ?? update.update_id, update.update_id);
        changed = true;
      }

      const normalized = normalizeIncomingUpdate(update);
      if (!normalized) {
        continue;
      }

      const duplicate = this.messages.some((message) => {
        return (
          message.chatId === normalized.chatId &&
          normalized.telegramMessageId !== undefined &&
          message.telegramMessageId === normalized.telegramMessageId &&
          message.updateType === normalized.updateType
        );
      });
      if (duplicate) {
        continue;
      }

      this.messages.push({
        id: this.nextId++,
        chatId: normalized.chatId,
        kind: normalized.kind,
        text: normalized.text,
        fromUser: normalized.fromUser,
        date: normalized.date,
        read: false,
        telegramUpdateId: update.update_id,
        telegramMessageId: normalized.telegramMessageId,
        updateType: normalized.updateType,
        mediaFileId: normalized.mediaFileId,
        caption: normalized.caption,
        attachments: normalized.attachments,
        metadata: normalized.metadata,
        raw: normalized.raw,
      });
      changed = true;
    }

    if (this.messages.length > MAX_MESSAGES) {
      this.messages = this.messages.slice(-MAX_MESSAGES);
      changed = true;
    }

    if (changed) {
      await this.persist();
    }
  }

  async getNewMessages(chatId?: number): Promise<StoredMessage[]> {
    await this.start();

    const unread = this.messages.filter(
      (message) => !message.read && (chatId === undefined || message.chatId === chatId)
    );
    if (unread.length === 0) {
      return [];
    }

    for (const message of unread) {
      message.read = true;
    }

    await this.persist();
    return unread;
  }

  async listChats(): Promise<ChatInfo[]> {
    await this.start();

    const latestByChat = new Map<number, StoredMessage>();
    for (const message of this.messages) {
      const previous = latestByChat.get(message.chatId);
      if (!previous || previous.date < message.date) {
        latestByChat.set(message.chatId, message);
      }
    }

    const result: ChatInfo[] = [];
    for (const [chatId, message] of latestByChat) {
      const unreadCount = this.messages.filter(
        (candidate) => candidate.chatId === chatId && !candidate.read
      ).length;
      result.push({
        chatId,
        user: message.fromUser,
        lastMessage: message.text,
        lastMessageDate: message.date,
        unreadCount,
      });
    }

    return result.sort((left, right) => right.lastMessageDate - left.lastMessageDate);
  }

  private async persist(): Promise<void> {
    this.persistQueue = this.persistQueue
      .catch(() => undefined)
      .then(async () => {
        const payload: PersistedState = {
          version: 3,
          nextId: this.nextId,
          lastUpdateId: this.lastUpdateId,
          messages: this.messages,
        };

        await mkdir(path.dirname(this.storagePath), { recursive: true });
        const tempPath = `${this.storagePath}.tmp`;
        await writeFile(tempPath, JSON.stringify(payload, null, 2), "utf8");
        await rename(tempPath, this.storagePath);
      });

    await this.persistQueue;
  }
}

function normalizeIncomingUpdate(update: TelegramUpdate): NormalizedIncomingUpdate | null {
  if (update.message) {
    return normalizeMessageEnvelope("message", update.message);
  }
  if (update.edited_message) {
    return normalizeMessageEnvelope("edited_message", update.edited_message);
  }
  if (update.channel_post) {
    return normalizeMessageEnvelope("channel_post", update.channel_post);
  }
  if (update.edited_channel_post) {
    return normalizeMessageEnvelope("edited_channel_post", update.edited_channel_post);
  }
  if (update.business_message) {
    return normalizeMessageEnvelope("business_message", update.business_message);
  }
  if (update.edited_business_message) {
    return normalizeMessageEnvelope("edited_business_message", update.edited_business_message);
  }
  if (update.deleted_business_messages) {
    return normalizeDeletedBusinessMessages(update.deleted_business_messages);
  }
  if (update.callback_query) {
    return normalizeCallbackQuery(update.callback_query);
  }
  if (update.inline_query) {
    return normalizeInlineQuery(update.inline_query);
  }
  if (update.chosen_inline_result) {
    return normalizeChosenInlineResult(update.chosen_inline_result);
  }
  if (update.shipping_query) {
    return normalizeShippingQuery(update.shipping_query);
  }
  if (update.pre_checkout_query) {
    return normalizePreCheckoutQuery(update.pre_checkout_query);
  }
  if (update.poll) {
    return normalizePoll(update.poll);
  }
  if (update.poll_answer) {
    return normalizePollAnswer(update.poll_answer);
  }
  if (update.chat_member) {
    return normalizeChatMemberUpdate("chat_member", update.chat_member);
  }
  if (update.my_chat_member) {
    return normalizeChatMemberUpdate("my_chat_member", update.my_chat_member);
  }
  if (update.chat_join_request) {
    return normalizeChatJoinRequest(update.chat_join_request);
  }
  if (update.message_reaction) {
    return normalizeMessageReaction(update.message_reaction);
  }
  if (update.message_reaction_count) {
    return normalizeMessageReactionCount(update.message_reaction_count);
  }

  const payloadKeys = Object.keys(update).filter((key) => key !== "update_id");
  if (payloadKeys.length === 0) {
    return null;
  }

  const updateType = payloadKeys[0];
  const raw = asRecord(update[updateType]);
  const fromUser = normalizeUser(raw?.from);
  const chatId = resolveChatId(raw?.chat, raw?.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: resolveUnixMs(raw?.date),
    fromUser,
    kind: updateType,
    text: buildSummary(updateType, "", ""),
    updateType,
    metadata: compactRecord({
      payload_fields: Object.keys(raw ?? {}).sort(),
    }),
    raw,
  };
}

function normalizeMessageEnvelope(
  updateType: string,
  message: TelegramMessage
): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(message.chat, message.from, message.sender_chat);
  if (chatId === undefined) {
    return null;
  }

  const normalized = normalizeMessageContent(message);
  return {
    chatId,
    date: resolveUnixMs(message.date ?? message.edit_date),
    fromUser: normalizeUser(message.from, message.sender_chat),
    kind: normalized.kind,
    text: normalized.text,
    updateType,
    telegramMessageId: getNumber(message.message_id),
    mediaFileId: normalized.mediaFileId,
    caption: normalized.caption,
    attachments: normalized.attachments,
    metadata: normalized.metadata,
    raw: normalized.raw,
  };
}

function normalizeMessageContent(message: TelegramMessage): {
  kind: string;
  text: string;
  mediaFileId?: string;
  caption?: string;
  attachments?: StoredAttachment[];
  metadata?: Record<string, unknown>;
  raw?: Record<string, unknown>;
} {
  const caption = cleanText(message.caption);
  const text = cleanText(message.text);
  const handledFields = new Set<string>([
    "message_id",
    "date",
    "edit_date",
    "text",
    "caption",
    "chat",
    "from",
    "sender_chat",
    "reply_to_message",
    "forward_origin",
    "forward_from",
    "forward_from_chat",
    "forward_sender_name",
    "forward_signature",
    "forward_date",
    "via_bot",
    "message_thread_id",
    "is_topic_message",
    "has_media_spoiler",
    "entities",
    "caption_entities",
    "author_signature",
    "reply_markup",
  ]);

  const attachments: StoredAttachment[] = [];
  addMessageAttachments(message, attachments, handledFields);

  const serviceFields = detectServiceFields(message);
  if (serviceFields.length > 0) {
    attachments.push({
      kind: "service",
      summary: serviceFields.join(", "),
      metadata: compactRecord({
        fields: serviceFields,
      }),
    });
  }

  const firstFileAttachment = attachments.find((attachment) => attachment.fileId);
  const kind =
    attachments[0]?.kind ??
    (text ? "text" : caption ? "caption" : serviceFields.length > 0 ? "service" : "unknown");
  const summaryDetail = buildAttachmentSummary(attachments);
  const summaryText = text || buildSummary(kind, summaryDetail, caption);
  const payloadFields = Object.keys(message).sort();
  const unhandledFields = payloadFields.filter((field) => !handledFields.has(field));

  return {
    kind,
    text: summaryText,
    mediaFileId: firstFileAttachment?.fileId,
    caption: caption || undefined,
    attachments: attachments.length > 0 ? attachments : undefined,
    metadata: compactRecord({
      chat: normalizeChat(message.chat),
      sender_chat: normalizeChat(message.sender_chat),
      reply_to_message_id: getNumber(asRecord(message.reply_to_message)?.message_id),
      reply_preview: cleanText(asRecord(message.reply_to_message)?.text)
        || cleanText(asRecord(message.reply_to_message)?.caption)
        || undefined,
      forward: normalizeForward(message),
      via_bot: normalizeUserMetadata(message.via_bot),
      message_thread_id: getNumber(message.message_thread_id),
      is_topic_message: getBoolean(message.is_topic_message),
      has_media_spoiler: getBoolean(message.has_media_spoiler),
      edit_date: formatUnixIso(message.edit_date),
      payload_fields: payloadFields,
      unhandled_fields: unhandledFields.length > 0 ? unhandledFields : undefined,
    }),
    raw: unhandledFields.length > 0 ? pickFields(message, unhandledFields) : undefined,
  };
}

function addMessageAttachments(
  message: TelegramMessage,
  attachments: StoredAttachment[],
  handledFields: Set<string>
): void {
  if (message.voice?.file_id) {
    handledFields.add("voice");
    attachments.push({
      kind: "voice",
      fileId: message.voice.file_id,
      summary: formatDuration(message.voice.duration),
      metadata: compactRecord({
        duration_seconds: getNumber(message.voice.duration),
        mime_type: cleanText(message.voice.mime_type),
        file_size: getNumber(message.voice.file_size),
      }),
    });
  }

  if (message.audio?.file_id) {
    handledFields.add("audio");
    const titleBits = [cleanText(message.audio.performer), cleanText(message.audio.title)]
      .filter(Boolean)
      .join(" - ");
    attachments.push({
      kind: "audio",
      fileId: message.audio.file_id,
      summary:
        titleBits ||
        cleanText(message.audio.file_name) ||
        formatDuration(message.audio.duration) ||
        undefined,
      metadata: compactRecord({
        duration_seconds: getNumber(message.audio.duration),
        performer: cleanText(message.audio.performer),
        title: cleanText(message.audio.title),
        file_name: cleanText(message.audio.file_name),
        mime_type: cleanText(message.audio.mime_type),
        file_size: getNumber(message.audio.file_size),
      }),
    });
  }

  if (Array.isArray(message.photo) && message.photo.length > 0) {
    handledFields.add("photo");
    const largest = message.photo[message.photo.length - 1];
    attachments.push({
      kind: "photo",
      fileId: largest?.file_id,
      summary: formatDimensions(largest?.width, largest?.height),
      metadata: compactRecord({
        variants: message.photo
          .map((photo) =>
            compactRecord({
              file_id: cleanText(photo?.file_id),
              width: getNumber(photo?.width),
              height: getNumber(photo?.height),
              file_size: getNumber(photo?.file_size),
            })
          )
          .filter((photo): photo is Record<string, unknown> => !!photo && Object.keys(photo).length > 0),
      }),
    });
  }

  if (message.video?.file_id) {
    handledFields.add("video");
    attachments.push({
      kind: "video",
      fileId: message.video.file_id,
      summary: [cleanText(message.video.file_name), formatDuration(message.video.duration)]
        .filter(Boolean)
        .join(", "),
      metadata: compactRecord({
        file_name: cleanText(message.video.file_name),
        mime_type: cleanText(message.video.mime_type),
        duration_seconds: getNumber(message.video.duration),
        width: getNumber(message.video.width),
        height: getNumber(message.video.height),
        file_size: getNumber(message.video.file_size),
      }),
    });
  }

  if (message.video_note?.file_id) {
    handledFields.add("video_note");
    attachments.push({
      kind: "video_note",
      fileId: message.video_note.file_id,
      summary: [formatDuration(message.video_note.duration), formatSquare(message.video_note.length)]
        .filter(Boolean)
        .join(", "),
      metadata: compactRecord({
        duration_seconds: getNumber(message.video_note.duration),
        length: getNumber(message.video_note.length),
        file_size: getNumber(message.video_note.file_size),
      }),
    });
  }

  if (message.animation?.file_id) {
    handledFields.add("animation");
    attachments.push({
      kind: "animation",
      fileId: message.animation.file_id,
      summary:
        [cleanText(message.animation.file_name), formatDuration(message.animation.duration)]
          .filter(Boolean)
          .join(", ") || undefined,
      metadata: compactRecord({
        file_name: cleanText(message.animation.file_name),
        mime_type: cleanText(message.animation.mime_type),
        duration_seconds: getNumber(message.animation.duration),
        width: getNumber(message.animation.width),
        height: getNumber(message.animation.height),
        file_size: getNumber(message.animation.file_size),
      }),
    });
  }

  if (message.document?.file_id) {
    handledFields.add("document");
    attachments.push({
      kind: "document",
      fileId: message.document.file_id,
      summary:
        [cleanText(message.document.file_name), cleanText(message.document.mime_type)]
          .filter(Boolean)
          .join(", ") || undefined,
      metadata: compactRecord({
        file_name: cleanText(message.document.file_name),
        mime_type: cleanText(message.document.mime_type),
        file_size: getNumber(message.document.file_size),
      }),
    });
  }

  if (message.sticker?.file_id) {
    handledFields.add("sticker");
    attachments.push({
      kind: "sticker",
      fileId: message.sticker.file_id,
      summary: [cleanText(message.sticker.emoji), cleanText(message.sticker.set_name)]
        .filter(Boolean)
        .join(" "),
      metadata: compactRecord({
        emoji: cleanText(message.sticker.emoji),
        set_name: cleanText(message.sticker.set_name),
        width: getNumber(message.sticker.width),
        height: getNumber(message.sticker.height),
        file_size: getNumber(message.sticker.file_size),
      }),
    });
  }

  if (isRecord(message.location)) {
    handledFields.add("location");
    attachments.push({
      kind: "location",
      summary: formatCoordinates(message.location.latitude, message.location.longitude),
      metadata: compactRecord({
        latitude: getNumber(message.location.latitude),
        longitude: getNumber(message.location.longitude),
        horizontal_accuracy: getNumber(message.location.horizontal_accuracy),
        live_period: getNumber(message.location.live_period),
        heading: getNumber(message.location.heading),
        proximity_alert_radius: getNumber(message.location.proximity_alert_radius),
      }),
    });
  }

  if (isRecord(message.venue)) {
    handledFields.add("venue");
    attachments.push({
      kind: "venue",
      summary: [cleanText(message.venue.title), cleanText(message.venue.address)]
        .filter(Boolean)
        .join(", "),
      metadata: compactRecord({
        title: cleanText(message.venue.title),
        address: cleanText(message.venue.address),
        foursquare_id: cleanText(message.venue.foursquare_id),
        google_place_id: cleanText(message.venue.google_place_id),
        location: compactRecord({
          latitude: getNumber(asRecord(message.venue.location)?.latitude),
          longitude: getNumber(asRecord(message.venue.location)?.longitude),
        }),
      }),
    });
  }

  if (isRecord(message.contact)) {
    handledFields.add("contact");
    attachments.push({
      kind: "contact",
      summary: [
        [cleanText(message.contact.first_name), cleanText(message.contact.last_name)]
          .filter(Boolean)
          .join(" "),
        cleanText(message.contact.phone_number),
      ]
        .filter(Boolean)
        .join(", "),
      metadata: compactRecord({
        phone_number: cleanText(message.contact.phone_number),
        first_name: cleanText(message.contact.first_name),
        last_name: cleanText(message.contact.last_name),
        user_id: getNumber(message.contact.user_id),
        vcard: cleanText(message.contact.vcard),
      }),
    });
  }

  if (isRecord(message.poll)) {
    handledFields.add("poll");
    attachments.push({
      kind: "poll",
      summary: cleanText(message.poll.question),
      metadata: compactRecord({
        id: cleanText(message.poll.id),
        question: cleanText(message.poll.question),
        type: cleanText(message.poll.type),
        total_voter_count: getNumber(message.poll.total_voter_count),
        allows_multiple_answers: getBoolean(message.poll.allows_multiple_answers),
        is_anonymous: getBoolean(message.poll.is_anonymous),
        options: Array.isArray(message.poll.options)
          ? message.poll.options
              .map((option) =>
                compactRecord({
                  text: cleanText(asRecord(option)?.text),
                  voter_count: getNumber(asRecord(option)?.voter_count),
                })
              )
              .filter((option): option is Record<string, unknown> => !!option && Object.keys(option).length > 0)
          : undefined,
      }),
    });
  }

  if (isRecord(message.dice)) {
    handledFields.add("dice");
    attachments.push({
      kind: "dice",
      summary: [cleanText(message.dice.emoji), getNumber(message.dice.value)]
        .filter((value) => value !== undefined && value !== "")
        .join(" "),
      metadata: compactRecord({
        emoji: cleanText(message.dice.emoji),
        value: getNumber(message.dice.value),
      }),
    });
  }

  if (isRecord(message.game)) {
    handledFields.add("game");
    attachments.push({
      kind: "game",
      summary: cleanText(message.game.title) || cleanText(message.game.description) || undefined,
      metadata: compactRecord({
        title: cleanText(message.game.title),
        description: cleanText(message.game.description),
      }),
    });
  }

  if (isRecord(message.invoice)) {
    handledFields.add("invoice");
    attachments.push({
      kind: "invoice",
      summary: [cleanText(message.invoice.title), cleanText(message.invoice.currency)]
        .filter(Boolean)
        .join(", "),
      metadata: compactRecord({
        title: cleanText(message.invoice.title),
        description: cleanText(message.invoice.description),
        currency: cleanText(message.invoice.currency),
        total_amount: getNumber(message.invoice.total_amount),
        start_parameter: cleanText(message.invoice.start_parameter),
      }),
    });
  }

  if (isRecord(message.successful_payment)) {
    handledFields.add("successful_payment");
    attachments.push({
      kind: "payment",
      summary: [cleanText(message.successful_payment.currency), getNumber(message.successful_payment.total_amount)]
        .filter((value) => value !== undefined && value !== "")
        .join(" "),
      metadata: compactRecord({
        currency: cleanText(message.successful_payment.currency),
        total_amount: getNumber(message.successful_payment.total_amount),
        invoice_payload: cleanText(message.successful_payment.invoice_payload),
        telegram_payment_charge_id: cleanText(message.successful_payment.telegram_payment_charge_id),
        provider_payment_charge_id: cleanText(message.successful_payment.provider_payment_charge_id),
      }),
    });
  }

  if (isRecord(message.story)) {
    handledFields.add("story");
    attachments.push({
      kind: "story",
      summary:
        [
          cleanText(asRecord(message.story.chat)?.title),
          getNumber(message.story.id),
        ]
          .filter((value) => value !== undefined && value !== "")
          .join(" #") || undefined,
      metadata: compactRecord({
        id: getNumber(message.story.id),
        chat: normalizeChat(asRecord(message.story.chat)),
      }),
    });
  }
}

function normalizeCallbackQuery(query: TelegramCallbackQuery): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(query.message?.chat, query.from);
  if (chatId === undefined) {
    return null;
  }

  const messageContent = query.message ? normalizeMessageContent(query.message) : undefined;
  const detail = cleanText(query.data) || cleanText(query.game_short_name) || cleanText(query.inline_message_id);

  return {
    chatId,
    date: resolveUnixMs(query.message?.date),
    fromUser: normalizeUser(query.from),
    kind: "callback_query",
    text: buildSummary("callback_query", detail, messageContent?.caption ?? ""),
    updateType: "callback_query",
    telegramMessageId: getNumber(query.message?.message_id),
    mediaFileId: messageContent?.mediaFileId,
    caption: messageContent?.caption,
    attachments: messageContent?.attachments,
    metadata: compactRecord({
      id: cleanText(query.id),
      data: cleanText(query.data),
      game_short_name: cleanText(query.game_short_name),
      chat_instance: cleanText(query.chat_instance),
      inline_message_id: cleanText(query.inline_message_id),
      message: messageContent
        ? compactRecord({
            kind: messageContent.kind,
            text: messageContent.text,
            caption: messageContent.caption,
          })
        : undefined,
    }),
    raw: buildRawFromUnhandled(query, ["id", "from", "message", "data", "chat_instance", "inline_message_id", "game_short_name"]),
  };
}

function normalizeInlineQuery(query: TelegramInlineQuery): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(undefined, query.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: Date.now(),
    fromUser: normalizeUser(query.from),
    kind: "inline_query",
    text: cleanText(query.query) || "[inline_query]",
    updateType: "inline_query",
    metadata: compactRecord({
      id: cleanText(query.id),
      offset: cleanText(query.offset),
      chat_type: cleanText(query.chat_type),
      location: normalizeLocation(query.location),
    }),
    raw: buildRawFromUnhandled(query, ["id", "from", "query", "offset", "chat_type", "location"]),
  };
}

function normalizeChosenInlineResult(
  result: TelegramChosenInlineResult
): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(undefined, result.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: Date.now(),
    fromUser: normalizeUser(result.from),
    kind: "chosen_inline_result",
    text: buildSummary("chosen_inline_result", cleanText(result.query) || cleanText(result.result_id), ""),
    updateType: "chosen_inline_result",
    metadata: compactRecord({
      result_id: cleanText(result.result_id),
      query: cleanText(result.query),
      inline_message_id: cleanText(result.inline_message_id),
      location: normalizeLocation(result.location),
    }),
    raw: buildRawFromUnhandled(result, ["result_id", "from", "query", "inline_message_id", "location"]),
  };
}

function normalizeShippingQuery(query: TelegramShippingQuery): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(undefined, query.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: Date.now(),
    fromUser: normalizeUser(query.from),
    kind: "shipping_query",
    text: buildSummary("shipping_query", cleanText(query.invoice_payload), ""),
    updateType: "shipping_query",
    metadata: compactRecord({
      id: cleanText(query.id),
      invoice_payload: cleanText(query.invoice_payload),
      shipping_address: isRecord(query.shipping_address) ? query.shipping_address : undefined,
    }),
    raw: buildRawFromUnhandled(query, ["id", "from", "invoice_payload", "shipping_address"]),
  };
}

function normalizePreCheckoutQuery(
  query: TelegramPreCheckoutQuery
): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(undefined, query.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: Date.now(),
    fromUser: normalizeUser(query.from),
    kind: "pre_checkout_query",
    text: buildSummary("pre_checkout_query", cleanText(query.invoice_payload), ""),
    updateType: "pre_checkout_query",
    metadata: compactRecord({
      id: cleanText(query.id),
      currency: cleanText(query.currency),
      total_amount: getNumber(query.total_amount),
      invoice_payload: cleanText(query.invoice_payload),
    }),
    raw: buildRawFromUnhandled(query, ["id", "from", "currency", "total_amount", "invoice_payload"]),
  };
}

function normalizePoll(poll: TelegramPoll): NormalizedIncomingUpdate | null {
  const question = cleanText(poll.question);
  if (!question) {
    return null;
  }

  return {
    chatId: 0,
    date: Date.now(),
    fromUser: { id: 0, firstName: "Telegram" },
    kind: "poll",
    text: buildSummary("poll", question, ""),
    updateType: "poll",
    metadata: compactRecord({
      id: cleanText(poll.id),
      question,
      type: cleanText(poll.type),
      total_voter_count: getNumber(poll.total_voter_count),
      allows_multiple_answers: getBoolean(poll.allows_multiple_answers),
      is_anonymous: getBoolean(poll.is_anonymous),
    }),
    raw: buildRawFromUnhandled(poll, ["id", "question", "type", "total_voter_count", "allows_multiple_answers", "is_anonymous", "options"]),
  };
}

function normalizePollAnswer(answer: TelegramPollAnswer): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(undefined, answer.user);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: Date.now(),
    fromUser: normalizeUser(answer.user),
    kind: "poll_answer",
    text: buildSummary("poll_answer", cleanText(answer.poll_id), ""),
    updateType: "poll_answer",
    metadata: compactRecord({
      poll_id: cleanText(answer.poll_id),
      option_ids: Array.isArray(answer.option_ids) ? answer.option_ids : undefined,
    }),
    raw: buildRawFromUnhandled(answer, ["poll_id", "user", "option_ids"]),
  };
}

function normalizeChatMemberUpdate(
  updateType: string,
  update: TelegramChatMemberUpdate
): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(update.chat, update.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: resolveUnixMs(update.date),
    fromUser: normalizeUser(update.from),
    kind: updateType,
    text: buildSummary(updateType, cleanText(update.chat?.title) || cleanText(update.chat?.username), ""),
    updateType,
    metadata: compactRecord({
      chat: normalizeChat(update.chat),
      old_chat_member: isRecord(update.old_chat_member) ? update.old_chat_member : undefined,
      new_chat_member: isRecord(update.new_chat_member) ? update.new_chat_member : undefined,
      invite_link: isRecord(update.invite_link) ? update.invite_link : undefined,
    }),
    raw: buildRawFromUnhandled(update, ["from", "chat", "date", "old_chat_member", "new_chat_member", "invite_link"]),
  };
}

function normalizeChatJoinRequest(update: TelegramChatJoinRequest): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(update.chat, update.from);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: resolveUnixMs(update.date),
    fromUser: normalizeUser(update.from),
    kind: "chat_join_request",
    text: buildSummary("chat_join_request", cleanText(update.bio), ""),
    updateType: "chat_join_request",
    metadata: compactRecord({
      chat: normalizeChat(update.chat),
      bio: cleanText(update.bio),
      invite_link: isRecord(update.invite_link) ? update.invite_link : undefined,
    }),
    raw: buildRawFromUnhandled(update, ["from", "chat", "date", "bio", "invite_link"]),
  };
}

function normalizeMessageReaction(update: TelegramMessageReaction): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(update.chat, update.user, update.actor_chat);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: resolveUnixMs(update.date),
    fromUser: normalizeUser(update.user, update.actor_chat),
    kind: "message_reaction",
    text: buildSummary("message_reaction", cleanText(update.message_id), ""),
    updateType: "message_reaction",
    telegramMessageId: getNumber(update.message_id),
    metadata: compactRecord({
      chat: normalizeChat(update.chat),
      old_reaction: Array.isArray(update.old_reaction) ? update.old_reaction : undefined,
      new_reaction: Array.isArray(update.new_reaction) ? update.new_reaction : undefined,
    }),
    raw: buildRawFromUnhandled(update, ["user", "actor_chat", "chat", "date", "message_id", "old_reaction", "new_reaction"]),
  };
}

function normalizeMessageReactionCount(
  update: TelegramMessageReactionCount
): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(update.chat);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: resolveUnixMs(update.date),
    fromUser: { id: chatId, firstName: cleanText(update.chat?.title) || "Telegram" },
    kind: "message_reaction_count",
    text: buildSummary("message_reaction_count", cleanText(update.message_id), ""),
    updateType: "message_reaction_count",
    telegramMessageId: getNumber(update.message_id),
    metadata: compactRecord({
      chat: normalizeChat(update.chat),
      reactions: Array.isArray(update.reactions) ? update.reactions : undefined,
    }),
    raw: buildRawFromUnhandled(update, ["chat", "date", "message_id", "reactions"]),
  };
}

function normalizeDeletedBusinessMessages(
  update: TelegramDeletedBusinessMessages
): NormalizedIncomingUpdate | null {
  const chatId = resolveChatId(update.chat);
  if (chatId === undefined) {
    return null;
  }

  return {
    chatId,
    date: Date.now(),
    fromUser: { id: chatId, firstName: cleanText(update.chat?.title) || "Telegram" },
    kind: "deleted_business_messages",
    text: buildSummary("deleted_business_messages", "", ""),
    updateType: "deleted_business_messages",
    metadata: compactRecord({
      chat: normalizeChat(update.chat),
      message_ids: Array.isArray(update.message_ids) ? update.message_ids : undefined,
    }),
    raw: buildRawFromUnhandled(update, ["chat", "message_ids"]),
  };
}

function detectServiceFields(message: TelegramMessage): string[] {
  const keys = [
    "new_chat_members",
    "left_chat_member",
    "new_chat_title",
    "new_chat_photo",
    "delete_chat_photo",
    "group_chat_created",
    "supergroup_chat_created",
    "channel_chat_created",
    "message_auto_delete_timer_changed",
    "migrate_to_chat_id",
    "migrate_from_chat_id",
    "pinned_message",
    "forum_topic_created",
    "forum_topic_edited",
    "forum_topic_closed",
    "forum_topic_reopened",
    "general_forum_topic_hidden",
    "general_forum_topic_unhidden",
    "write_access_allowed",
    "users_shared",
    "chat_shared",
    "giveaway",
    "giveaway_winners",
    "gift",
  ];

  return keys.filter((key) => message[key] !== undefined);
}

function normalizeForward(message: TelegramMessage): Record<string, unknown> | undefined {
  return compactRecord({
    origin: isRecord(message.forward_origin) ? message.forward_origin : undefined,
    from: normalizeUserMetadata(message.forward_from),
    from_chat: normalizeChat(message.forward_from_chat),
    sender_name: cleanText(message.forward_sender_name),
    signature: cleanText(message.forward_signature),
    date: formatUnixIso(message.forward_date),
  });
}

function normalizeUser(user?: unknown, fallbackChat?: unknown): UserInfo {
  const userRecord = asRecord(user);
  const fallbackChatRecord = asRecord(fallbackChat);
  return {
    id: getNumber(userRecord?.id) ?? getNumber(fallbackChatRecord?.id) ?? 0,
    firstName:
      cleanText(userRecord?.first_name) ||
      cleanText(fallbackChatRecord?.title) ||
      cleanText(fallbackChatRecord?.first_name) ||
      cleanText(fallbackChatRecord?.username) ||
      "Telegram",
    lastName:
      cleanText(userRecord?.last_name) || cleanText(fallbackChatRecord?.last_name) || undefined,
    username:
      cleanText(userRecord?.username) || cleanText(fallbackChatRecord?.username) || undefined,
  };
}

function normalizeUserMetadata(user?: unknown): Record<string, unknown> | undefined {
  const normalized = normalizeUser(user);
  if (!normalized.firstName && normalized.id === 0) {
    return undefined;
  }
  return compactRecord({
    id: normalized.id,
    first_name: normalized.firstName,
    last_name: normalized.lastName,
    username: normalized.username,
  });
}

function normalizeChat(chat?: unknown): Record<string, unknown> | undefined {
  const chatRecord = asRecord(chat);
  return compactRecord({
    id: getNumber(chatRecord?.id),
    type: cleanText(chatRecord?.type),
    title: cleanText(chatRecord?.title),
    username: cleanText(chatRecord?.username),
    first_name: cleanText(chatRecord?.first_name),
    last_name: cleanText(chatRecord?.last_name),
  });
}

function normalizeLocation(location?: unknown): Record<string, unknown> | undefined {
  const locationRecord = asRecord(location);
  return compactRecord({
    latitude: getNumber(locationRecord?.latitude),
    longitude: getNumber(locationRecord?.longitude),
  });
}

function resolveChatId(...sources: Array<unknown>): number | undefined {
  for (const source of sources) {
    const record = asRecord(source);
    const id = getNumber(record?.id);
    if (id !== undefined) {
      return id;
    }
  }
  return undefined;
}

function buildAttachmentSummary(attachments: StoredAttachment[]): string {
  return attachments
    .map((attachment) => attachment.summary || attachment.kind)
    .filter(Boolean)
    .join(" | ");
}

function formatDuration(value: unknown): string {
  const seconds = getNumber(value);
  if (seconds === undefined || seconds <= 0) {
    return "";
  }
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return rest > 0 ? `${minutes}m ${rest}s` : `${minutes}m`;
}

function formatDimensions(width: unknown, height: unknown): string {
  const normalizedWidth = getNumber(width);
  const normalizedHeight = getNumber(height);
  if (normalizedWidth === undefined || normalizedHeight === undefined) {
    return "";
  }
  return `${normalizedWidth}x${normalizedHeight}`;
}

function formatSquare(length: unknown): string {
  const size = getNumber(length);
  return size === undefined ? "" : `${size}x${size}`;
}

function formatCoordinates(latitude: unknown, longitude: unknown): string {
  const normalizedLatitude = getNumber(latitude);
  const normalizedLongitude = getNumber(longitude);
  if (normalizedLatitude === undefined || normalizedLongitude === undefined) {
    return "";
  }
  return `${normalizedLatitude.toFixed(5)}, ${normalizedLongitude.toFixed(5)}`;
}

function buildSummary(kind: string, detail: string, caption: string): string {
  const parts = [`[${kind}]`];
  if (detail) {
    parts.push(detail);
  }
  if (caption) {
    parts.push(`caption: ${caption}`);
  }
  return parts.join(" ").trim();
}

function cleanText(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function getNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function getBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function resolveUnixMs(value: unknown): number {
  const timestamp = getNumber(value);
  return timestamp !== undefined ? timestamp * 1000 : Date.now();
}

function formatUnixIso(value: unknown): string | undefined {
  const timestamp = getNumber(value);
  return timestamp !== undefined ? new Date(timestamp * 1000).toISOString() : undefined;
}

function compactRecord(
  value: Record<string, unknown>
): Record<string, unknown> | undefined {
  const entries = Object.entries(value).filter(([, entryValue]) => {
    if (entryValue === undefined || entryValue === null) {
      return false;
    }
    if (typeof entryValue === "string") {
      return entryValue.length > 0;
    }
    if (Array.isArray(entryValue)) {
      return entryValue.length > 0;
    }
    if (isRecord(entryValue)) {
      return Object.keys(entryValue).length > 0;
    }
    return true;
  });

  return entries.length > 0 ? Object.fromEntries(entries) : undefined;
}

function buildRawFromUnhandled(
  value: Record<string, unknown>,
  handledFields: string[]
): Record<string, unknown> | undefined {
  const unhandledFields = Object.keys(value).filter((field) => !handledFields.includes(field));
  return unhandledFields.length > 0 ? pickFields(value, unhandledFields) : undefined;
}

function pickFields(
  value: Record<string, unknown>,
  fields: string[]
): Record<string, unknown> | undefined {
  const entries = fields
    .filter((field) => value[field] !== undefined)
    .map((field) => [field, value[field]] as const);
  return entries.length > 0 ? Object.fromEntries(entries) : undefined;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) ? value : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
