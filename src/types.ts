export interface StoredMessage {
  id: number;
  chatId: number;
  kind: StoredMessageKind;
  text: string;
  fromUser: UserInfo;
  date: number;
  read: boolean;
  telegramUpdateId?: number;
  telegramMessageId?: number;
  updateType?: StoredUpdateType;
  mediaFileId?: string;
  caption?: string;
  attachments?: StoredAttachment[];
  metadata?: Record<string, unknown>;
  raw?: Record<string, unknown>;
}

export type StoredMessageKind = string;
export type StoredUpdateType = string;

export interface StoredAttachment {
  kind: string;
  fileId?: string;
  summary?: string;
  metadata?: Record<string, unknown>;
}

export interface UserInfo {
  id: number;
  firstName: string;
  lastName?: string;
  username?: string;
}

export interface ChatInfo {
  chatId: number;
  user: UserInfo;
  lastMessage: string;
  lastMessageDate: number;
  unreadCount: number;
}
