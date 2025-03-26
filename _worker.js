// ======================================================
// 配置和全局变量
// ======================================================
// 从环境变量中读取配置
let BOT_TOKEN;
let GROUP_ID;
let MAX_MESSAGES_PER_MINUTE;

// 全局变量，用于控制清理频率和 webhook 初始化
let lastCleanupTime = 0;
const CLEANUP_INTERVAL = 24 * 60 * 60 * 1000; // 24 小时
let isWebhookInitialized = false; // 用于标记 webhook 是否已初始化
let botUsernameCache = null; // 用于缓存机器人用户名

// 用户信息缓存，减少重复请求
const userInfoCache = new Map();
const CACHE_EXPIRY = 3600000; // 1小时缓存过期

// ======================================================
// 入口点和主要流程
// ======================================================
export default {
  async fetch(request, env) {
    // 从环境变量加载配置
    BOT_TOKEN = env.BOT_TOKEN_ENV || null;
    GROUP_ID = env.GROUP_ID_ENV || null;
    MAX_MESSAGES_PER_MINUTE = env.MAX_MESSAGES_PER_MINUTE_ENV ? parseInt(env.MAX_MESSAGES_PER_MINUTE_ENV) : 40;
    
    if (!BOT_TOKEN) {
      console.error('BOT_TOKEN_ENV is not defined');
    }

    if (!GROUP_ID) {
      console.error('GROUP_ID_ENV is not defined');
    }

    if (!env.D1) {
      console.error('D1 database is not bound');
      return new Response('Server configuration error: D1 database is not bound', { status: 500 });
    }

    await checkAndRepairTables(env.D1);

    if (!isWebhookInitialized && BOT_TOKEN) {
      await registerWebhook(request);
      isWebhookInitialized = true;
    }

    await runPeriodicCleanup(env.D1);

    // ======================================================
    // 主请求处理
    // ======================================================
    // 主处理函数
    async function handleRequest(request) {
      if (!BOT_TOKEN || !GROUP_ID) {
        console.error('Missing required environment variables');
        return new Response('Server configuration error: Missing required environment variables', { status: 500 });
      }

      const url = new URL(request.url);
      if (url.pathname === '/webhook') {
        try {
          const update = await request.json();
          await handleUpdate(update);
          return new Response('OK');
        } catch (error) {
          console.error('Error parsing request or handling update:', error);
          return new Response('Bad Request', { status: 400 });
        }
      } else if (url.pathname === '/registerWebhook') {
        return await registerWebhook(request);
      } else if (url.pathname === '/unRegisterWebhook') {
        return await unRegisterWebhook();
      } else if (url.pathname === '/checkTables') {
        await checkAndRepairTables(env.D1);
        return new Response('Database tables checked and repaired', { status: 200 });
      } else if (url.pathname === '/force-nickname-update') {
        return await handleForceNicknameUpdate(request);
      }
      return new Response('Not Found', { status: 404 });
    }

    async function handleForceNicknameUpdate(request) {
      try {
        const chatId = new URL(request.url).searchParams.get('chatId');
        if (!chatId) {
          return new Response('缺少chatId参数', { status: 400 });
        }
        
        const userInfo = await getUserInfo(chatId);
        if (!userInfo) {
          return new Response(`找不到用户 ${chatId} 的信息`, { status: 404 });
        }
        
        const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
        const topicId = await getExistingTopicId(chatId);
        if (!topicId) {
          return new Response(`用户 ${chatId} 没有对应的话题`, { status: 404 });
        }
        
        const userState = await env.D1.prepare(
          'SELECT last_nickname FROM user_states WHERE chat_id = ?'
        ).bind(chatId).first();
        
        const updated = await updateForumTopicName(topicId, nickname);
        if (updated) {
          await env.D1.prepare(
            'UPDATE user_states SET last_nickname = ? WHERE chat_id = ?'
          ).bind(nickname, chatId).run();
          
          return new Response(`成功更新用户 ${chatId} 的话题名称为 ${nickname}`, { status: 200 });
        }
        
        return new Response(`更新用户 ${chatId} 的话题名称失败`, { status: 500 });
      } catch (error) {
        console.error('Error forcing nickname update:', error);
        return new Response(`强制更新话题名称时发生错误: ${error.toString()}`, { status: 500 });
      }
    }

    // ======================================================
    // Webhook 相关功能
    // ======================================================
    // 自动注册 webhook 的函数
    async function autoRegisterWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        // 设置 webhook
        const webhookResponse = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl }),
        }).then(r => r.json());
        
        if (!webhookResponse.ok) {
          console.error('Webhook auto-registration failed:', JSON.stringify(webhookResponse, null, 2));
        }

      } catch (error) {
        console.error('Error during webhook auto-registration:', error);
      }
    }

    // ======================================================
    // 数据库管理
    // ======================================================
    // 检查和修复数据库表结构
    async function checkAndRepairTables(d1) {
      try {
        console.log('Checking and repairing database tables...');

        const expectedTables = {
          user_states: {
            columns: {
              chat_id: 'TEXT PRIMARY KEY',
              is_blocked: 'BOOLEAN DEFAULT FALSE',
              is_verified: 'BOOLEAN DEFAULT FALSE',
              verification_code: 'TEXT',
              last_verification_message_id: 'TEXT',
              is_first_verification: 'BOOLEAN DEFAULT FALSE',
              verification_failures: 'INTEGER DEFAULT 0',
              last_nickname: 'TEXT',
              last_message_time: 'INTEGER'
            }
          },
          message_rates: {
            columns: {
              chat_id: 'TEXT PRIMARY KEY',
              message_count: 'INTEGER DEFAULT 0',
              window_start: 'INTEGER',
              start_count: 'INTEGER DEFAULT 0',
              start_window_start: 'INTEGER'
            }
          },
          chat_topic_mappings: {
            columns: {
              chat_id: 'TEXT PRIMARY KEY',
              topic_id: 'TEXT NOT NULL'
            }
          }
        };

        for (const [tableName, structure] of Object.entries(expectedTables)) {
          try {
            const tableInfo = await d1.prepare(
              `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`
            ).bind(tableName).first();

            if (!tableInfo) {
              await createTable(d1, tableName, structure);
              continue;
            }

            const columnsResult = await d1.prepare(
              `PRAGMA table_info(${tableName})`
            ).all();
            
            const currentColumns = new Map(
              columnsResult.results.map(col => [col.name, {
                type: col.type,
                notnull: col.notnull,
                dflt_value: col.dflt_value
              }])
            );

            for (const [colName, colDef] of Object.entries(structure.columns)) {
              if (!currentColumns.has(colName)) {
                const columnParts = colDef.split(' ');
                const addColumnSQL = `ALTER TABLE ${tableName} ADD COLUMN ${colName} ${columnParts.slice(1).join(' ')}`;
                await d1.exec(addColumnSQL);
              }
            }
          } catch (error) {
            console.error(`Error checking ${tableName}:`, error);
            await d1.exec(`DROP TABLE IF EXISTS ${tableName}`);
            await createTable(d1, tableName, structure);
          }
        }

        console.log('Database tables check and repair completed');
      } catch (error) {
        console.error('Error in checkAndRepairTables:', error);
        throw error;
      }
    }

    // 创建表的辅助函数
    async function createTable(d1, tableName, structure) {
      const columnsDef = Object.entries(structure.columns)
        .map(([name, def]) => `${name} ${def}`)
        .join(', ');
      const createSQL = `CREATE TABLE ${tableName} (${columnsDef})`;
      await d1.exec(createSQL);
    }

    // ======================================================
    // 缓存管理和清理
    // ======================================================
    // 定期清理用户信息缓存
    async function cleanupCache() {
      const now = Date.now();
      for (const [key, value] of userInfoCache.entries()) {
        if (now - value.timestamp > CACHE_EXPIRY) {
          userInfoCache.delete(key);
        }
      }
    }

    // 执行定期清理任务
    async function runPeriodicCleanup(d1) {
      const now = Date.now();
      
      // 仅在超过清理间隔时执行清理
      if (now - lastCleanupTime < CLEANUP_INTERVAL) {
        return;
      }
      
      console.log('Running cleanup tasks...');
      
      try {
        // 清理用户缓存
        await cleanupCache();
        
        lastCleanupTime = now; // 更新最后清理时间
      } catch (error) {
        console.error('Error during cleanup tasks:', error);
      }
    }

    // ======================================================
    // 消息处理
    // ======================================================
    /**
     * 处理Telegram更新（消息或回调查询）
     */
    async function handleUpdate(update) {
      try {
        if (update.message) {
          await onMessage(update.message);
        } else if (update.callback_query) {
          await onCallbackQuery(update.callback_query);
        }
      } catch (error) {
        console.error('Error handling update:', error);
      }
    }

    /**
     * 处理收到的消息
     * 根据消息来源、用户状态和消息类型进行不同处理
     */
    async function onMessage(message) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const messageId = message.message_id;

      if (chatId === GROUP_ID) {
        await handleGroupMessage(message);
        return;
      }

      const userState = await env.D1.prepare(
        'SELECT is_blocked, is_verified, is_first_verification, last_nickname, last_message_time FROM user_states WHERE chat_id = ?'
      ).bind(chatId).first() || { 
        is_blocked: false, 
        is_verified: false, 
        is_first_verification: true,
        last_nickname: null,
        last_message_time: null
      };
      
      if (userState.is_blocked) return;
      
      if (text === '/start') {
        await handleStartCommand(chatId, messageId, userState.is_verified, userState.is_first_verification, userState);
        return;
      }

      if (!userState.is_verified) {
        await sendMessageToUser(chatId, "您尚未完成验证，请使用 /start 命令进行验证！");
        return;
      }

      if (await checkMessageRate(chatId)) {
        await sendMessageToUser(chatId, `消息发送频率过高，此消息未被转发：${text || '非文本消息'}\n\n请稍后再试，每分钟最多可发送${MAX_MESSAGES_PER_MINUTE}条消息。`);
        return;
      }

      try {
        await handleUserMessage(message, userState);
      } catch (error) {
        console.error(`处理消息失败: chatId=${chatId}, error=${error}`);
      }
    }

    async function handleUserMessage(message, userState) {
      const chatId = message.chat.id.toString();
      const text = message.text || '';
      const userInfo = await getUserInfo(chatId);
      const now = Date.now();
      
      const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
      
      // 更新用户状态和检查话题名称更新
      await updateUserStateAndCheckTopicName(chatId, nickname, now, userState);
      
      const topicId = await getOrCreateTopic(chatId, nickname, userInfo);
      if (!topicId) return;

      if (message.forward_date) {
        await forwardMessageToTopic(topicId, message);
      } else if (text) {
        await sendMessageToTopic(topicId, text);
      } else {
        await copyMessageToTopic(topicId, message);
      }
    }

    // 新增通用函数处理用户状态更新和话题名称更新
    async function updateUserStateAndCheckTopicName(chatId, nickname, now, userState) {
      // 检查是否需要更新话题名
      const shouldUpdateName = shouldUpdateNickname(userState, nickname, now);
      
      // 更新用户状态
      await updateUserState(chatId, nickname, now);
      
      // 如果需要更新话题名
      if (shouldUpdateName) {
        const topicId = await getExistingTopicId(chatId);
        if (topicId) {
          await updateForumTopicName(topicId, nickname);
        }
      }
      
      return shouldUpdateName;
    }

    function shouldUpdateNickname(userState, newNickname, now) {
      if (!userState.last_nickname || !userState.last_message_time) return false;
      
      const timeDiff = now - userState.last_message_time;
      const oneDayInMs = 10;

      return timeDiff > oneDayInMs && userState.last_nickname !== newNickname;
    }

    async function updateUserState(chatId, nickname, timestamp) {
      await env.D1.prepare(
        'UPDATE user_states SET last_nickname = ?, last_message_time = ? WHERE chat_id = ?'
      ).bind(nickname, timestamp, chatId).run();
    }

    async function getOrCreateTopic(chatId, nickname, userInfo) {
      let topicId = await getExistingTopicId(chatId);
      if (topicId) return topicId;
      
      const userName = userInfo.username || userInfo.first_name;
      topicId = await createForumTopic(nickname, userName, nickname, userInfo.id);
      
      if (topicId) {
        await saveTopicId(chatId, topicId);
        return topicId;
      }
      
      console.error(`创建话题失败: chatId=${chatId}`);
      return null;
    }

    /**
     * 处理来自群组的消息（客服回复或管理员命令）
     */
    async function handleGroupMessage(message) {
      const topicId = message.message_thread_id;
      if (!topicId) return;

      const privateChatId = await getPrivateChatId(topicId);
      if (!privateChatId) return;

      const text = message.text || '';
      const botUsername = await getBotUsername();
      
      // 检查是否为管理员命令（直接命令或@机器人的命令）
      const isDirectCommand = text.startsWith('/block') || text.startsWith('/unblock') || text.startsWith('/checkblock');
      const isAtCommand = botUsername && (
        text.startsWith(`/block@${botUsername}`) || 
        text.startsWith(`/unblock@${botUsername}`) || 
        text.startsWith(`/checkblock@${botUsername}`)
      );
      
      if (isDirectCommand || isAtCommand) {
        // 去除可能存在的@username部分
        const cleanCommand = text.split('@')[0];
        // 创建一个干净的消息对象副本，确保handleAdminCommand处理的是纯命令
        const cleanMessage = { ...message, text: cleanCommand };
        await handleAdminCommand(cleanMessage, topicId, privateChatId);
        return;
      }
      
      // 获取用户信息和状态，检查昵称是否需要更新
      try {
        // 获取用户状态
        const userState = await env.D1.prepare(
          'SELECT last_nickname, last_message_time FROM user_states WHERE chat_id = ?'
        ).bind(privateChatId).first() || { last_nickname: null, last_message_time: null };
        
        const userInfo = await getUserInfo(privateChatId);
        const now = Date.now();
        const nickname = `${userInfo.first_name} ${userInfo.last_name || ''}`.trim();
        
        // 使用共用函数更新用户状态并检查话题名称更新
        await updateUserStateAndCheckTopicName(privateChatId, nickname, now, userState);
      } catch (error) {
        // 出错时继续执行转发消息的逻辑
      }
      
      // 判断是普通回复还是转发消息
      if (message.forward_date) {
        // 如果是转发消息，保持原样转发
        await forwardMessageToPrivateChat(privateChatId, message);
      } else {
        // 如果是普通消息，使用复制方式
        await copyMessageToPrivateChat(privateChatId, message);
      }
    }

    /**
     * 获取机器人的用户名
     */
    async function getBotUsername() {
      // 使用缓存避免重复请求
      if (botUsernameCache) {
        return botUsernameCache;
      }
      
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getMe`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        });
        
        const data = await response.json();
        if (data.ok && data.result && data.result.username) {
          // 缓存用户名以供后续使用
          botUsernameCache = data.result.username;
          return botUsernameCache;
        }
        
        return null;
      } catch (error) {
        console.error("Error getting bot username:", error);
        return null;
      }
    }

    // ======================================================
    // 用户管理
    // ======================================================
    // 处理 /start 命令
    async function handleStartCommand(chatId, messageId, isVerified, isFirstVerification, userState) {
      // 检查 /start 命令的频率
      if (await checkStartCommandRate(chatId)) {
        await sendMessageToUser(chatId, "您发送 /start 命令过于频繁，请15秒后再试！");
        return;
      }

      // 如果用户尚未有记录，初始化 is_first_verification 为 true
      if (!userState || Object.keys(userState).length === 0) {
        await env.D1.prepare('INSERT INTO user_states (chat_id, is_first_verification) VALUES (?, ?)')
          .bind(chatId, true)
          .run();
      }

      // 如果用户已经验证，直接发送欢迎消息
      if (isVerified) {
        await sendMessageToUser(chatId, "欢迎使用私聊机器人，现在发送信息吧！");
        return;
      }

      // 如果用户未验证，触发验证流程
      await sendMessageToUser(chatId, "欢迎使用私聊机器人，请完成验证以开始使用！");
      await handleVerification(chatId, messageId);
    }

    async function checkStartCommandRate(chatId) {
      const key = chatId;
      const now = Date.now();
      const window = 15 * 1000; // 15 秒窗口
      const maxStartsPerWindow = 1; // 每 15 秒最多允许 1 次 /start 命令

      try {
        const rateData = await env.D1.prepare('SELECT start_count, start_window_start FROM message_rates WHERE chat_id = ?')
          .bind(key)
          .first();
        let data = rateData ? { count: rateData.start_count, start: rateData.start_window_start } : { count: 0, start: now };

        if (now - data.start > window) {
          data.count = 1;
          data.start = now;
        } else {
          data.count += 1;
        }

        await env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, start_count, start_window_start) VALUES (?, ?, ?)')
          .bind(key, data.count, data.start)
          .run();

        return data.count > maxStartsPerWindow;
      } catch (error) {
        console.error("Error checking start command rate:", error);
        // 在错误情况下，保守起见返回false不限制用户
        return false;
      }
    }

    // ======================================================
    // 群组和权限管理
    // ======================================================
    async function handleAdminCommand(message, topicId, privateChatId) {
      const text = message.text;
      const senderId = message.from.id.toString();

      const isAdmin = await checkIfAdmin(senderId);
      if (!isAdmin) {
        await sendMessageToTopic(topicId, '只有管理员可以使用此命令。');
        return;
      }

      try {
        if (text === '/block') {
          await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, is_blocked) VALUES (?, ?)')
            .bind(privateChatId, true)
            .run();
          await sendMessageToTopic(topicId, `用户 [${privateChatId}](tg://user?id=${privateChatId}) 已被拉黑，消息将不再转发。`);
        } else if (text === '/unblock') {
          await env.D1.prepare('UPDATE user_states SET is_blocked = ? WHERE chat_id = ?')
            .bind(false, privateChatId)
            .run();
          await sendMessageToTopic(topicId, `用户 [${privateChatId}](tg://user?id=${privateChatId}) 已解除拉黑，消息将继续转发。`);
        } else if (text === '/checkblock') {
          const userState = await env.D1.prepare('SELECT is_blocked FROM user_states WHERE chat_id = ?')
            .bind(privateChatId)
            .first();
          const isBlocked = userState ? userState.is_blocked : false;
          await sendMessageToTopic(topicId, isBlocked ? `用户 [${privateChatId}](tg://user?id=${privateChatId}) 已在黑名单中` : `用户 [${privateChatId}](tg://user?id=${privateChatId}) 不在黑名单中`);
        }
      } catch (error) {
        console.error("Error executing admin command:", error);
        await sendMessageToTopic(topicId, "执行管理员命令出错，请稍后重试。");
      }
    }

    async function checkIfAdmin(userId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChatMember`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            user_id: userId,
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to check admin status: ${data.description}`);
          return false;
        }
        const status = data.result.status;
        return status === 'administrator' || status === 'creator';
      } catch (error) {
        console.error("Error checking admin status:", error);
        return false;
      }
    }

    // ======================================================
    // 验证系统
    // ======================================================
    async function handleVerification(chatId, messageId) {
      try {
        const lastVerification = await env.D1.prepare('SELECT last_verification_message_id FROM user_states WHERE chat_id = ?')
          .bind(chatId)
          .first();
        const lastVerificationMessageId = lastVerification ? lastVerification.last_verification_message_id : null;
        
        if (lastVerificationMessageId) {
          try {
            await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                chat_id: chatId,
                message_id: lastVerificationMessageId,
              }),
            });
          } catch (error) {
            // 删除消息失败不影响流程
          }
        }
        
        await env.D1.prepare('UPDATE user_states SET verification_code = NULL, last_verification_message_id = NULL WHERE chat_id = ?')
          .bind(chatId)
          .run();
      } catch (error) {
        console.error(`验证准备失败: chatId=${chatId}, error=${error}`);
        await sendMessageToUser(chatId, "验证准备出错，请稍后重试。");
        return;
      }

      await sendVerification(chatId);
    }

    async function sendVerification(chatId) {
      // 生成两个1-20之间的随机数
      const num1 = Math.floor(Math.random() * 20) + 1;
      const num2 = Math.floor(Math.random() * 20) + 1;
      const correctResult = num1 + num2;

      // 生成错误选项（正确答案上下浮动1-3）
      const options = new Set();
      options.add(correctResult);
      while (options.size < 4) {
        let offset = Math.floor(Math.random() * 3) + 1;
        if (Math.random() > 0.5) offset = -offset;
        
        const wrongResult = correctResult + offset;
        if (wrongResult > 0) {
          options.add(wrongResult);
        }
      }
      const optionArray = Array.from(options).sort(() => Math.random() - 0.5);

      const buttons = optionArray.map((option) => ({
        text: `${option}`,
        callback_data: `verify_${chatId}_${option}_${option === correctResult ? 'correct' : 'wrong'}`,
      }));

      const question = `请计算：${num1} + ${num2} = ?（点击下方按钮完成验证）`;
      
      try {
        await env.D1.prepare('INSERT OR REPLACE INTO user_states (chat_id, verification_code, is_verified, is_blocked, is_first_verification, verification_failures) VALUES (?, ?, COALESCE((SELECT is_verified FROM user_states WHERE chat_id = ?), FALSE), COALESCE((SELECT is_blocked FROM user_states WHERE chat_id = ?), FALSE), COALESCE((SELECT is_first_verification FROM user_states WHERE chat_id = ?), TRUE), COALESCE((SELECT verification_failures FROM user_states WHERE chat_id = ?), 0))')
          .bind(chatId, correctResult.toString(), chatId, chatId, chatId, chatId)
          .run();

        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: question,
            reply_markup: {
              inline_keyboard: [buttons],
            },
          }),
        });
        
        const data = await response.json();
        if (!data.ok) {
          console.error(`发送验证消息失败: ${data.description}`);
          return;
        }
        
        await env.D1.prepare('UPDATE user_states SET last_verification_message_id = ? WHERE chat_id = ?')
          .bind(data.result.message_id.toString(), chatId)
          .run();
      } catch (error) {
        console.error(`发送验证消息出错: chatId=${chatId}, error=${error}`);
      }
    }

    // ======================================================
    // 用户信息和频率控制
    // ======================================================
    async function checkMessageRate(chatId) {
      const key = chatId;
      const now = Date.now();
      const window = 60 * 1000; // 1 分钟窗口

      try {
        const rateData = await env.D1.prepare('SELECT message_count, window_start FROM message_rates WHERE chat_id = ?')
          .bind(key)
          .first();
        let data = rateData ? { count: rateData.message_count, start: rateData.window_start } : { count: 0, start: now };

        if (now - data.start > window) {
          data.count = 1;
          data.start = now;
        } else {
          data.count += 1;
        }

        await env.D1.prepare('INSERT OR REPLACE INTO message_rates (chat_id, message_count, window_start, start_count, start_window_start) VALUES (?, ?, ?, COALESCE((SELECT start_count FROM message_rates WHERE chat_id = ?), 0), COALESCE((SELECT start_window_start FROM message_rates WHERE chat_id = ?), ?))')
          .bind(key, data.count, data.start, key, key, now)
          .run();

        return data.count > MAX_MESSAGES_PER_MINUTE;
      } catch (error) {
        console.error("Error checking message rate:", error);
        // 在错误情况下，保守起见返回false不限制用户
        return false;
      }
    }

    async function getUserInfo(chatId) {
      // 检查缓存中是否有用户信息
      const cacheKey = `user:${chatId}`;
      const cachedData = userInfoCache.get(cacheKey);
      const now = Date.now();
      
      if (cachedData && (now - cachedData.timestamp < CACHE_EXPIRY)) {
        return cachedData.data;
      }
      
      // 缓存中没有或已过期，从API获取
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/getChat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId }),
        });
        const data = await response.json();
        if (!data.ok) {
          throw new Error(`Failed to get user info: ${data.description}`);
        }
        
        // 保存到缓存
        userInfoCache.set(cacheKey, {
          data: data.result,
          timestamp: now
        });
        
        return data.result;
      } catch (error) {
        console.error(`Error fetching user info for ${chatId}:`, error);
        // 如果有过期的缓存数据，宁可使用过期数据
        if (cachedData) {
          return cachedData.data;
        }
        throw error;
      }
    }

    // ======================================================
    // 话题管理
    // ======================================================
    async function getExistingTopicId(chatId) {
      const mapping = await env.D1.prepare('SELECT topic_id FROM chat_topic_mappings WHERE chat_id = ?')
        .bind(chatId)
        .first();
      return mapping ? mapping.topic_id : null;
    }

    async function createForumTopic(topicName, userName, nickname, userId) {
      const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/createForumTopic`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: GROUP_ID, name: topicName }),
      });
      const data = await response.json();
      if (!data.ok) {
        throw new Error(`Failed to create forum topic: ${data.description}`);
      }
      const topicId = data.result.message_thread_id;

      // 转换为北京时间
      const now = new Date();
      now.setHours(now.getHours() + 8); // UTC+8
      const formattedTime = now.toISOString()
        .replace('T', ' ')
        .slice(0, 19);

      const pinnedMessage = `昵称: ${nickname}\n用户名: @${userName}\nUserID: [${userId}](tg://user?id=${userId})\n发起时间: ${formattedTime}`;
      const messageResponse = await sendMessageToTopic(topicId, pinnedMessage);
      const messageId = messageResponse.result.message_id;
      await pinMessage(topicId, messageId);

      return topicId;
    }

    async function saveTopicId(chatId, topicId) {
      try {
        await env.D1.prepare('INSERT OR REPLACE INTO chat_topic_mappings (chat_id, topic_id) VALUES (?, ?)')
          .bind(chatId, topicId)
          .run();
      } catch (error) {
        console.error("Error saving topic ID:", error);
        throw error; // 重新抛出错误让调用者处理
      }
    }

    async function getPrivateChatId(topicId) {
      const mapping = await env.D1.prepare('SELECT chat_id FROM chat_topic_mappings WHERE topic_id = ?')
        .bind(topicId)
        .first();
      return mapping ? mapping.chat_id : null;
    }

    // ======================================================
    // 消息发送和转发
    // ======================================================
    /**
     * 发送文本消息到话题
     */
    async function sendMessageToTopic(topicId, text) {
      if (!text.trim()) {
        return;
      }

      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            text: text,
            message_thread_id: topicId,
            parse_mode: 'Markdown',
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`发送消息到话题失败: ${data.description}`);
        }
        return data;
      } catch (error) {
        console.error(`发送消息到话题出错: ${error}`);
      }
    }

    /**
     * 复制消息到话题，不保留原始转发标记
     */
    async function copyMessageToTopic(topicId, message) {
      try {
        const params = {
          chat_id: GROUP_ID,
          from_chat_id: message.chat.id,
          message_id: message.message_id,
          message_thread_id: topicId
        };
        
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(params),
        });
        
        const data = await response.json();
        if (!data.ok) {
          console.error(`复制消息到话题失败: ${data.description}`);
        }
      } catch (error) {
        console.error(`复制消息到话题出错: ${error}`);
      }
    }

    async function pinMessage(topicId, messageId) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/pinChatMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_id: messageId,
            message_thread_id: topicId,
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to pin message: ${data.description}`);
        }
      } catch (error) {
        console.error("Error pinning message:", error);
      }
    }

    /**
     * 更新话题名称
     */
    async function updateForumTopicName(topicId, newName) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/editForumTopic`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            message_thread_id: topicId,
            name: newName
          }),
        });
        
        const data = await response.json();
        if (!data.ok) {
          console.error(`更新话题名称失败: ${data.description}`);
          return false;
        }
        
        return true;
      } catch (error) {
        console.error(`更新话题名称时发生错误: ${error.toString()}`);
        return false;
      }
    }

    /**
     * 转发消息到私人聊天，保留原始转发标记
     */
    async function forwardMessageToPrivateChat(privateChatId, message) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/forwardMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: privateChatId,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to forward message to private chat: ${data.description}`);
          return;
        }
      } catch (error) {
        console.error("Error forwarding message:", error);
      }
    }

    /**
     * 复制消息到私人聊天，不保留原始转发标记
     */
    async function copyMessageToPrivateChat(privateChatId, message) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/copyMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: privateChatId,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to copy message to private chat: ${data.description}`);
          return;
        }
      } catch (error) {
        console.error("Error copying message:", error);
      }
    }

    /**
     * 发送文本消息到用户
     */
    async function sendMessageToUser(chatId, text) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ 
            chat_id: chatId, 
            text: text,
            reply_markup: {
              remove_keyboard: true
            }
          }),
        });
        const data = await response.json();
        if (!data.ok) {
          console.error(`发送消息到用户失败: ${data.description}`);
        }
      } catch (error) {
        console.error(`发送消息到用户出错: ${error}`);
      }
    }

    // ======================================================
    // 网络请求和Webhook管理
    // ======================================================
    async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
      for (let i = 0; i < retries; i++) {
        try {
          const response = await fetch(url, options);
          if (response.ok) return response;
          
          // 只对频率限制和服务器错误进行重试
          if (response.status === 429 || response.status >= 500) {
            const delay = response.status === 429
              ? parseInt(response.headers.get('Retry-After') || '0') * 1000 || backoff
              : backoff * Math.pow(2, i);
            
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
          
          // 其他错误直接抛出
          throw new Error(`Request failed with status ${response.status}: ${response.statusText}`);
        } catch (error) {
          // 最后一次尝试失败，或者是网络错误
          if (i === retries - 1) throw error;
          await new Promise(resolve => setTimeout(resolve, backoff * Math.pow(2, i)));
        }
      }
    }

    async function registerWebhook(request) {
      const webhookUrl = `${new URL(request.url).origin}/webhook`;
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: webhookUrl }),
        });
        
        const data = await response.json();
        if (!data.ok) {
          console.error('Webhook registration failed:', data.description);
          return new Response(`Webhook registration failed: ${data.description}`, { status: 500 });
        }
        
        return new Response('Webhook set successfully');
      } catch (error) {
        console.error('Error during webhook registration:', error);
        return new Response('Webhook registration failed', { status: 500 });
      }
    }

    async function unRegisterWebhook() {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: '' }),
        });
        
        const data = await response.json();
        return new Response(data.ok ? 'Webhook removed successfully' : 'Failed to remove webhook');
      } catch (error) {
        console.error('Error removing webhook:', error);
        return new Response('Failed to remove webhook', { status: 500 });
      }
    }

    /**
     * 转发消息到话题，保留原始转发标记
     */
    async function forwardMessageToTopic(topicId, message) {
      try {
        const response = await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/forwardMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: GROUP_ID,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
            message_thread_id: topicId
          }),
        });
        
        const data = await response.json();
        if (!data.ok) {
          console.error(`Failed to forward message to topic: ${data.description}`);
        }
      } catch (error) {
        console.error("Error forwarding message to topic:", error);
      }
    }

    // ======================================================
    // 回调查询处理
    // ======================================================
    async function onCallbackQuery(callbackQuery) {
      const chatId = callbackQuery.message.chat.id.toString();
      const data = callbackQuery.data;
      const messageId = callbackQuery.message.message_id;

      if (!data.startsWith('verify_')) return;

      const [, userChatId, selectedAnswer, result] = data.split('_');
      
      if (userChatId !== chatId) return;

      const verificationState = await env.D1.prepare('SELECT verification_code, verification_failures FROM user_states WHERE chat_id = ?')
        .bind(chatId)
        .first();
      
      const storedCode = verificationState ? verificationState.verification_code : null;
      const failures = verificationState ? verificationState.verification_failures || 0 : 0;
      
      if (!storedCode) {
        await sendMessageToUser(chatId, '验证码已失效，请重新使用 /start 命令获取新验证码。');
        return;
      }

      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/answerCallbackQuery`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            callback_query_id: callbackQuery.id,
          }),
        });

        if (result === 'correct') {
          await env.D1.prepare('UPDATE user_states SET is_verified = ?, verification_code = NULL, verification_failures = 0, is_first_verification = ? WHERE chat_id = ?')
            .bind(true, false, chatId)
            .run();

          await sendMessageToUser(chatId, '验证成功！现在可以发送消息了。');
        } else {
          const newFailures = failures + 1;
          
          if (newFailures >= 5) {
            await env.D1.prepare('UPDATE user_states SET is_blocked = TRUE, verification_failures = ? WHERE chat_id = ?')
              .bind(newFailures, chatId)
              .run();

            await sendMessageToUser(chatId, '连续验证失败5次，您已被自动加入黑名单。如需解除，请联系管理员。');
          } else {
            await env.D1.prepare('UPDATE user_states SET verification_failures = ? WHERE chat_id = ?')
              .bind(newFailures, chatId)
              .run();

            await sendMessageToUser(chatId, `验证失败，这是第${newFailures}次失败，连续5次失败将自动加入黑名单。请重新尝试。`);
            await handleVerification(chatId, messageId);
          }
        }
      } catch (error) {
        console.error(`验证回调处理失败: chatId=${chatId}, error=${error}`);
        await sendMessageToUser(chatId, "验证处理出错，请稍后使用 /start 命令重试。");
        return;
      }

      try {
        await fetchWithRetry(`https://api.telegram.org/bot${BOT_TOKEN}/deleteMessage`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            message_id: messageId,
          }),
        });
      } catch (error) {
        // 删除消息失败不影响验证流程
      }
    }

    // ======================================================
    // 主入口点结束，返回响应
    // ======================================================
    try {
      return await handleRequest(request);
    } catch (error) {
      console.error('Unhandled error in fetch handler:', error);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};
