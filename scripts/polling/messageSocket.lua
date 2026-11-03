require("/scripts/messageutil.lua")

---@class MessageSocket
MessageSocket = {}
MessageSocket.__index = MessageSocket

---@param socketId string --ID сокета (если nil, будет сгенерирован новый)
---@return MessageSocket
function MessageSocket.new(socketId, onOpen, onError)
    local self = setmetatable({}, MessageSocket)

    self.socketId = socketId or sb.makeUuid()
    self.isOpen = true
    self.messageQueue = {}
    self.actionHandlers = {}
    self.pendingMessages = {}
    self.batchSize = 10
    self.receivePromise = nil

    promises:add(
        world.sendEntityMessage(player.id(), "messageSocket:create", self.socketId),
        function (result)
            if onOpen then
                onOpen(result)
            end
        end,
        function (error)
            self.isOpen = false
            if onError then
                onError(error)
            end
            sb.logError("MessageSocket:new error: %s", error)
        end
    )

    return self
end

---@param action  string
---@param handler function
---@return boolean
function MessageSocket:on(action, handler)
    if type(action) ~= "string" then
        sb.logError("MessageSocket:on - action должен быть строкой")
        return false
    end
    if type(handler) ~= "function" then
        sb.logError("MessageSocket:on - handler должен быть функцией")
        return false
    end

    self.actionHandlers[action] = handler
    return true
end

---@param targetSocketId    string
---@param action            string
---@param createIfNotExists boolean?
---@param args              any?
---@param onSuccess         function?
---@param onError           function?
---@return boolean
function MessageSocket:emit(targetSocketId, action, createIfNotExists, args, onSuccess, onError)
    if not self.isOpen then
        if onError then
            onError("Socket закрыт")
        end
        return false
    end

    if not targetSocketId then
        if onError then
            onError("Неверный targetSocketId")
        end
        return false
    end

    local message = { fromSocket = self.socketId, action = action, args = args or {}, timestamp = os.time() }

    promises:add(
        world.sendEntityMessage(player.id(), "messageSocket:send", targetSocketId, createIfNotExists, message),
        function (result)
            if onSuccess then
                onSuccess(result)
            end
        end,
        function (error)
            if onError then
                onError(error)
            end
            sb.logError("MessageSocket:emit error: %s", error)
        end
    )

    return true
end

---@param targetSocketId string
---@param message        table
---@return RpcPromise
function MessageSocket:_sendMessage(targetSocketId, message)
    return world.sendEntityMessage(player.id(), "messageSocket:send", targetSocketId, message)
end

---@return RpcPromise
function MessageSocket:_receiveMessages()
    return world.sendEntityMessage(player.id(), "messageSocket:receive", self.socketId, self.batchSize)
end

---@private
---@param message table
---@return void
function MessageSocket:_handleMessage(message)
    if not message or not message.action then
        return
    end

    local handler = self.actionHandlers[message.action]
    if handler then
        local success, result = pcall(handler, message.args, message.fromSocket)
        if not success then
            sb.logError("MessageSocket: ошибка в обработчике action '%s': %s", message.action, result)
        end
    else
        sb.logWarn("MessageSocket: нет обработчика для action '%s'", message.action)
    end
end

---@param onSuccess function?
---@param onError   function?
---@return void
function MessageSocket:close(onSuccess, onError)
    if not self.isOpen then
        return
    end

    self.isOpen = false
    self.receivePromise = nil -- Сбросить активный promise

    promises:add(
        world.sendEntityMessage(player.id(), "messageSocket:close", self.socketId),
        function (result)
            self.actionHandlers = {}
            self.messageQueue = {}
            self.pendingMessages = {}
            if onSuccess then
                onSuccess(result)
            end
        end,
        function (error)
            if onError then
                onError(error)
            end
            sb.logError("MessageSocket:close error: %s", error)
        end
    )
end

---@return boolean
function MessageSocket:isConnected()
    return self.isOpen
end

---@return string
function MessageSocket:getId()
    return self.socketId
end

---@param size number - размер batch
---@return void
function MessageSocket:setBatchSize(size)
    if size and size > 0 then
        self.batchSize = size
    end
end

---@return void
function MessageSocket:update()
    if not self.isOpen then
        return
    end

    if self.receivePromise and self.receivePromise:finished() then
        if self.receivePromise:succeeded() then
            local messages = self.receivePromise:result()
            if messages and type(messages) == "table" then
                for _, message in ipairs(messages) do
                    self:_handleMessage(message)
                end
            end
        else
            -- Ошибка получения
            sb.logError("MessageSocket:_receiveMessages error: %s", self.receivePromise:error())
        end

        self.receivePromise = nil
    elseif not self.receivePromise then
        self.receivePromise = self:_receiveMessages()
    end
end

-- Фабричная функция для создания MessageSocket
---@param socketId string
---@return MessageSocket
function createSocket(socketId, onOpen, onError)
    return MessageSocket.new(socketId, onOpen, onError)
end

