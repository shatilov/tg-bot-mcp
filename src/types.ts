export interface StoredMessage {
  id: number;
  chatId: number;
  text: string;
  fromUser: UserInfo;
  date: number;
  read: boolean;
  telegramUpdateId?: number;
  telegramMessageId?: number;
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
