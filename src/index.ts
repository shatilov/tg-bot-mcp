#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { MessageStorage } from "./storage.js";
import { createBot } from "./bot.js";
import { registerTools } from "./tools.js";

const token = process.env.TELEGRAM_BOT_TOKEN;
if (!token) {
  console.error("TELEGRAM_BOT_TOKEN is required");
  process.exit(1);
}

const defaultChatId = process.env.TELEGRAM_CHAT_ID
  ? Number(process.env.TELEGRAM_CHAT_ID)
  : undefined;

const storage = new MessageStorage();
const bot = createBot(token);

const server = new McpServer({
  name: "telegram",
  version: "1.0.0",
});

registerTools(server, bot, storage, defaultChatId);

async function main() {
  await storage.start();

  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("[mcp] server running on stdio");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
