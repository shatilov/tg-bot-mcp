import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { ChatInfo, StoredMessage, UserInfo } from "./types.js";

const DEFAULT_STORAGE_PATH = path.join(
  os.homedir(),
  ".codex",
  "tg-bot-mcp",
  "storage.json"
);
const MAX_MESSAGES = 500;

type PersistedState = {
  version: 1;
  nextId: number;
  lastUpdateId: number | null;
  messages: StoredMessage[];
};

type TelegramTextUpdate = {
  update_id: number;
  message?: {
    message_id?: number;
    date?: number;
    text?: string;
    chat?: {
      id?: number;
    };
    from?: {
      id?: number;
      first_name?: string;
      last_name?: string;
      username?: string;
    };
  };
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

  async ingestUpdates(updates: TelegramTextUpdate[]): Promise<void> {
    await this.start();

    let changed = false;

    for (const update of updates) {
      if (typeof update.update_id === "number") {
        this.lastUpdateId = Math.max(this.lastUpdateId ?? update.update_id, update.update_id);
        changed = true;
      }

      const text = update.message?.text;
      const chatId = update.message?.chat?.id;
      const from = update.message?.from;
      if (
        typeof text !== "string" ||
        typeof chatId !== "number" ||
        typeof from?.id !== "number" ||
        typeof from.first_name !== "string"
      ) {
        continue;
      }

      const telegramMessageId =
        typeof update.message?.message_id === "number" ? update.message.message_id : undefined;
      const duplicate = this.messages.some((message) => {
        return (
          message.chatId === chatId &&
          telegramMessageId !== undefined &&
          message.telegramMessageId === telegramMessageId
        );
      });
      if (duplicate) {
        continue;
      }

      const user: UserInfo = {
        id: from.id,
        firstName: from.first_name,
        lastName: from.last_name,
        username: from.username,
      };

      this.messages.push({
        id: this.nextId++,
        chatId,
        text,
        fromUser: user,
        date: typeof update.message?.date === "number" ? update.message.date * 1000 : Date.now(),
        read: false,
        telegramUpdateId: update.update_id,
        telegramMessageId,
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
          version: 1,
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
