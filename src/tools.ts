import { access, mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Bot, InputFile } from "grammy";
import { z } from "zod";
import { MessageStorage } from "./storage.js";
import { StoredMessage } from "./types.js";

const TELEGRAM_UPDATES_BATCH_SIZE = 100;
const SENDABLE_MEDIA_TYPES = [
  "photo",
  "video",
  "audio",
  "document",
  "animation",
  "voice",
  "sticker",
] as const;

type TelegramUpdate = {
  update_id: number;
  [key: string]: unknown;
};

type SendableMediaType = (typeof SENDABLE_MEDIA_TYPES)[number];

export function registerTools(
  server: McpServer,
  bot: Bot,
  storage: MessageStorage,
  defaultChatId?: number
) {
  server.registerTool(
    "list_chats",
    {
      description:
        "List all Telegram chats that have sent messages to the bot. Shows user info, last message, and unread count.",
      inputSchema: {},
    },
    async () => {
      try {
        await syncInbox(bot, storage);
        const chats = await storage.listChats();
        if (chats.length === 0) {
          return textResponse("No active chats.");
        }
        const lines = chats.map((chat) => {
          const name = chat.user.username
            ? `${chat.user.firstName} (@${chat.user.username})`
            : chat.user.firstName;
          const time = new Date(chat.lastMessageDate).toISOString();
          return `[${chat.chatId}] ${name} | unread: ${chat.unreadCount} | last: "${chat.lastMessage}" (${time})`;
        });
        return textResponse(lines.join("\n"));
      } catch (error) {
        return textResponse(`Error reading chats: ${formatError(error)}`);
      }
    }
  );

  server.registerTool(
    "get_new_messages",
    {
      description:
        "Get unread Telegram updates. Returns structured details for text, callbacks, media, replies, forwards, and other Telegram update types. Messages are marked as read after retrieval.",
      inputSchema: {
        chat_id: z
          .number()
          .optional()
          .describe("Filter by specific chat ID. If omitted, returns unread updates from all chats."),
      },
    },
    async ({ chat_id }) => {
      try {
        await syncInbox(bot, storage);
        const messages = await storage.getNewMessages(chat_id);
        if (messages.length === 0) {
          return textResponse("No new messages.");
        }
        const payload = messages.map(formatStoredMessage);
        return textResponse(JSON.stringify(payload, null, 2));
      } catch (error) {
        return textResponse(`Error reading new messages: ${formatError(error)}`);
      }
    }
  );

  server.registerTool(
    "send_message",
    {
      description:
        "Send a text message to a Telegram chat. If chat_id is not provided, the default TELEGRAM_CHAT_ID from config is used.",
      inputSchema: {
        chat_id: z
          .number()
          .optional()
          .describe("Target chat ID. Falls back to TELEGRAM_CHAT_ID env variable if omitted."),
        text: z.string().describe("Message text to send."),
      },
    },
    async ({ chat_id, text }) => {
      const targetChatId = chat_id ?? defaultChatId;
      if (!targetChatId) {
        return noChatConfiguredResponse();
      }
      try {
        await bot.api.sendMessage(targetChatId, text);
        return textResponse(`Message sent to chat ${targetChatId}.`);
      } catch (error) {
        return textResponse(`Error sending message: ${formatError(error)}`);
      }
    }
  );

  server.registerTool(
    "send_media",
    {
      description:
        "Send Telegram media to a chat. Supports photo, video, audio, document, animation, voice, and sticker. Source may be an absolute local path, an HTTP URL, or an existing Telegram file_id.",
      inputSchema: {
        chat_id: z
          .number()
          .optional()
          .describe("Target chat ID. Falls back to TELEGRAM_CHAT_ID env variable if omitted."),
        media_type: z.enum(SENDABLE_MEDIA_TYPES).describe("Telegram media type to send."),
        source: z
          .string()
          .describe("Absolute local path, HTTP(S) URL, or Telegram file_id for the media."),
        caption: z.string().optional().describe("Optional caption for supported media types."),
        file_name: z
          .string()
          .optional()
          .describe("Optional override filename when uploading a local file."),
        title: z.string().optional().describe("Optional title for audio."),
        performer: z.string().optional().describe("Optional performer for audio."),
        duration_seconds: z
          .number()
          .optional()
          .describe("Optional duration for audio, video, animation, or voice."),
        width: z.number().optional().describe("Optional width for video or animation."),
        height: z.number().optional().describe("Optional height for video or animation."),
        supports_streaming: z
          .boolean()
          .optional()
          .describe("Optional flag for streaming video."),
      },
    },
    async ({ chat_id, media_type, source, caption, file_name, title, performer, duration_seconds, width, height, supports_streaming }) => {
      const targetChatId = chat_id ?? defaultChatId;
      if (!targetChatId) {
        return noChatConfiguredResponse();
      }
      try {
        await sendMedia(bot, targetChatId, {
          mediaType: media_type,
          source,
          caption,
          fileName: file_name,
          title,
          performer,
          durationSeconds: duration_seconds,
          width,
          height,
          supportsStreaming: supports_streaming,
        });
        return textResponse(`${media_type} sent to chat ${targetChatId}.`);
      } catch (error) {
        return textResponse(`Error sending ${media_type}: ${formatError(error)}`);
      }
    }
  );

  server.registerTool(
    "send_voice",
    {
      description:
        "Send a voice message to a Telegram chat from a local file, URL, or existing Telegram file_id.",
      inputSchema: {
        chat_id: z
          .number()
          .optional()
          .describe("Target chat ID. Falls back to TELEGRAM_CHAT_ID env variable if omitted."),
        file_path: z
          .string()
          .describe("Absolute local path, HTTP(S) URL, or Telegram file_id for the voice file."),
        caption: z.string().optional().describe("Optional caption for the voice message."),
      },
    },
    async ({ chat_id, file_path, caption }) => {
      const targetChatId = chat_id ?? defaultChatId;
      if (!targetChatId) {
        return noChatConfiguredResponse();
      }
      try {
        await sendMedia(bot, targetChatId, {
          mediaType: "voice",
          source: file_path,
          caption,
        });
        return textResponse(`Voice message sent to chat ${targetChatId}.`);
      } catch (error) {
        return textResponse(`Error sending voice message: ${formatError(error)}`);
      }
    }
  );

  server.registerTool(
    "download_telegram_file",
    {
      description:
        "Download a Telegram file by file_id to a local path. If output_path is omitted, the file is saved to a temp path.",
      inputSchema: {
        file_id: z.string().describe("Telegram file_id to download."),
        output_path: z
          .string()
          .optional()
          .describe("Optional local file path. If omitted, a temp path is chosen automatically."),
      },
    },
    async ({ file_id, output_path }) => {
      try {
        const file = await bot.api.getFile(file_id);
        if (!file.file_path) {
          return textResponse(
            `Error downloading file: Telegram did not return file_path for ${file_id}.`
          );
        }

        const targetPath = await resolveDownloadPath(file.file_path, file_id, output_path);
        await ensureParentDirectory(targetPath);

        const response = await fetch(getTelegramFileUrl(bot.token, file.file_path));
        if (!response.ok) {
          throw new Error(
            `Telegram file download failed with ${response.status} ${response.statusText}`
          );
        }

        const bytes = Buffer.from(await response.arrayBuffer());
        await writeFile(targetPath, bytes);

        const payload = compactObject({
          file_id,
          output_path: targetPath,
          telegram_file_path: file.file_path,
          file_size: typeof file.file_size === "number" ? file.file_size : undefined,
        });

        return textResponse(JSON.stringify(payload, null, 2));
      } catch (error) {
        return textResponse(`Error downloading Telegram file: ${formatError(error)}`);
      }
    }
  );
}

async function syncInbox(bot: Bot, storage: MessageStorage): Promise<void> {
  let offset = storage.getNextUpdateOffset();

  while (true) {
    const updates = (await bot.api.getUpdates({
      offset,
      limit: TELEGRAM_UPDATES_BATCH_SIZE,
      timeout: 0,
      allowed_updates: [],
    })) as TelegramUpdate[];

    await storage.ingestUpdates(updates);

    if (updates.length < TELEGRAM_UPDATES_BATCH_SIZE) {
      return;
    }

    const maxUpdateId = updates.reduce((currentMax, update) => {
      return Math.max(currentMax, update.update_id);
    }, -1);
    offset = maxUpdateId + 1;
  }
}

async function sendMedia(
  bot: Bot,
  chatId: number,
  options: {
    mediaType: SendableMediaType;
    source: string;
    caption?: string;
    fileName?: string;
    title?: string;
    performer?: string;
    durationSeconds?: number;
    width?: number;
    height?: number;
    supportsStreaming?: boolean;
  }
): Promise<void> {
  const api = bot.api as any;
  const media = await resolveTelegramInput(options.source, options.fileName);

  switch (options.mediaType) {
    case "photo":
      await api.sendPhoto(chatId, media, compactObject({ caption: options.caption }));
      return;
    case "video":
      await api.sendVideo(
        chatId,
        media,
        compactObject({
          caption: options.caption,
          duration: options.durationSeconds,
          width: options.width,
          height: options.height,
          supports_streaming: options.supportsStreaming,
        })
      );
      return;
    case "audio":
      await api.sendAudio(
        chatId,
        media,
        compactObject({
          caption: options.caption,
          title: options.title,
          performer: options.performer,
          duration: options.durationSeconds,
        })
      );
      return;
    case "document":
      await api.sendDocument(chatId, media, compactObject({ caption: options.caption }));
      return;
    case "animation":
      await api.sendAnimation(
        chatId,
        media,
        compactObject({
          caption: options.caption,
          duration: options.durationSeconds,
          width: options.width,
          height: options.height,
        })
      );
      return;
    case "voice":
      await api.sendVoice(
        chatId,
        media,
        compactObject({
          caption: options.caption,
          duration: options.durationSeconds,
        })
      );
      return;
    case "sticker":
      await api.sendSticker(chatId, media);
      return;
  }
}

async function resolveTelegramInput(source: string, fileName?: string): Promise<InputFile | string> {
  if (/^https?:\/\//i.test(source)) {
    return source;
  }

  const resolvedPath = path.isAbsolute(source) ? source : path.resolve(source);
  try {
    await access(resolvedPath);
    return new InputFile(resolvedPath, fileName ?? path.basename(resolvedPath));
  } catch {
    return source;
  }
}

function formatStoredMessage(message: StoredMessage): Record<string, unknown> {
  return compactObject({
    chat_id: message.chatId,
    from: compactObject({
      id: message.fromUser.id,
      first_name: message.fromUser.firstName,
      last_name: message.fromUser.lastName,
      username: message.fromUser.username,
    }),
    date: new Date(message.date).toISOString(),
    update_type: message.updateType ?? "message",
    type: message.kind,
    text: message.text,
    caption: message.caption,
    telegram_update_id: message.telegramUpdateId,
    telegram_message_id: message.telegramMessageId,
    media_file_id: message.mediaFileId,
    attachments: message.attachments,
    metadata: message.metadata,
    raw: message.raw,
  });
}

function noChatConfiguredResponse() {
  return textResponse("Error: no chat_id provided and TELEGRAM_CHAT_ID is not configured.");
}

function getTelegramFileUrl(token: string, filePath: string): string {
  return `https://api.telegram.org/file/bot${token}/${filePath}`;
}

async function resolveDownloadPath(
  telegramFilePath: string,
  fileId: string,
  outputPath?: string
): Promise<string> {
  if (outputPath) {
    return path.resolve(outputPath);
  }

  const tempDirectory = path.join(os.tmpdir(), "tg-bot-mcp");
  await mkdir(tempDirectory, { recursive: true });

  const filename = deriveDownloadFilename(telegramFilePath, fileId);
  return path.join(tempDirectory, `${Date.now()}-${filename}`);
}

function deriveDownloadFilename(telegramFilePath: string, fileId: string): string {
  const basename = path.basename(telegramFilePath);
  const fallback = `telegram-file-${sanitizePathSegment(fileId)}`;
  return sanitizePathSegment(basename || fallback) || fallback;
}

function sanitizePathSegment(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]+/g, "_");
}

async function ensureParentDirectory(filePath: string): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
}

function compactObject<T extends Record<string, unknown>>(value: T): T {
  return Object.fromEntries(
    Object.entries(value).filter(([, entryValue]) => entryValue !== undefined)
  ) as T;
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function textResponse(text: string) {
  return {
    content: [{ type: "text" as const, text }],
  };
}
