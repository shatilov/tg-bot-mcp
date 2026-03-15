import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { Bot } from "grammy";
import { z } from "zod";
import { MessageStorage } from "./storage.js";

const TELEGRAM_UPDATES_BATCH_SIZE = 100;

type TelegramTextUpdate = {
  update_id: number;
  message?: {
    text?: string;
  };
};

export function registerTools(
  server: McpServer,
  bot: Bot,
  storage: MessageStorage,
  defaultChatId?: number
) {
  server.registerTool(
    "list_chats",
    {
      description: "List all Telegram chats that have sent messages to the bot. Shows user info, last message, and unread count.",
      inputSchema: {},
    },
    async () => {
      try {
        await syncInbox(bot, storage);
        const chats = await storage.listChats();
        if (chats.length === 0) {
          return { content: [{ type: "text", text: "No active chats." }] };
        }
        const lines = chats.map((chat) => {
          const name = chat.user.username
            ? `${chat.user.firstName} (@${chat.user.username})`
            : chat.user.firstName;
          const time = new Date(chat.lastMessageDate).toISOString();
          return `[${chat.chatId}] ${name} | unread: ${chat.unreadCount} | last: "${chat.lastMessage}" (${time})`;
        });
        return { content: [{ type: "text", text: lines.join("\n") }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text", text: `Error reading chats: ${message}` }] };
      }
    }
  );

  server.registerTool(
    "get_new_messages",
    {
      description:
        "Get unread messages from Telegram users. Optionally filter by chat_id. Messages are marked as read after retrieval.",
      inputSchema: {
        chat_id: z
          .number()
          .optional()
          .describe("Filter by specific chat ID. If omitted, returns unread messages from all chats."),
      },
    },
    async ({ chat_id }) => {
      try {
        await syncInbox(bot, storage);
        const messages = await storage.getNewMessages(chat_id);
        if (messages.length === 0) {
          return { content: [{ type: "text", text: "No new messages." }] };
        }
        const lines = messages.map((message) => {
          const name = message.fromUser.username
            ? `${message.fromUser.firstName} (@${message.fromUser.username})`
            : message.fromUser.firstName;
          const time = new Date(message.date).toISOString();
          return `[${message.chatId}] ${name} (${time}):\n${message.text}`;
        });
        return { content: [{ type: "text", text: lines.join("\n\n") }] };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return { content: [{ type: "text", text: `Error reading new messages: ${message}` }] };
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
        return {
          content: [
            {
              type: "text",
              text: "Error: no chat_id provided and TELEGRAM_CHAT_ID is not configured.",
            },
          ],
        };
      }
      try {
        await bot.api.sendMessage(targetChatId, text);
        return {
          content: [
            { type: "text", text: `Message sent to chat ${targetChatId}.` },
          ],
        };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return {
          content: [
            { type: "text", text: `Error sending message: ${message}` },
          ],
        };
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
      allowed_updates: ["message"],
    })) as TelegramTextUpdate[];

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
