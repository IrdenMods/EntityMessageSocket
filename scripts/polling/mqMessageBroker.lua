require("/scripts/messageutil.lua")

---@class MqMessageBroker
---@field socketId          string
---@field subscriberId      string
---@field isOpen            boolean
---@field isReady           boolean
---@field batchSize         number
---@field subscriptions     table<string, function>
---@field receivePromises   table<string, RpcPromise>
---@field pendingOperations function[]
MqMessageBroker = {}
MqMessageBroker.__index = MqMessageBroker

---@class MqBrokerMessage
---@field fromSubscriber string
---@field data           any
---@field timestamp      number

---@alias MqMessageHandler fun(data: any, fromSubscriberId: string, message: MqBrokerMessage)

---@param socketId            string?
---@param subscriberId        string?
---@param maxQueueHistorySize number?
---@param onOpen              function?
---@param onError             function?
---@return MqMessageBroker
function MqMessageBroker.new(socketId, subscriberId, maxQueueHistorySize, onOpen, onError)
    local self = setmetatable({}, MqMessageBroker)

    self.socketId = tostring(socketId or sb.makeUuid())
    self.subscriberId = tostring(subscriberId or sb.makeUuid())
    self.isOpen = true
    self.isReady = false
    self.batchSize = 10
    self.subscriptions = {}
    self.receivePromises = {}
    self.pendingOperations = {}

    promises:add(
        world.sendEntityMessage(player.id(), "mqSocket:create", self.socketId, maxQueueHistorySize),
        function (result)
            if not result then
                self.isOpen = false
                self.pendingOperations = {}

                if onError then
                    onError("Не удалось создать или подключиться к MQ broker")
                end
                return
            end

            self.isReady = true

            local operations = self.pendingOperations
            self.pendingOperations = {}
            for _, operation in ipairs(operations) do
                operation()
            end

            if onOpen then
                onOpen(result)
            end
        end,
        function (error)
            self.isOpen = false
            self.pendingOperations = {}

            if onError then
                onError(error)
            end

            sb.logError("MqMessageBroker:new error: %s", error)
        end
    )

    return self
end

---@private
---@param operation function
---@param onError   function?
---@return boolean
function MqMessageBroker:_whenReady(operation, onError)
    if not self.isOpen then
        if onError then
            onError("MQ broker закрыт")
        end
        return false
    end

    if self.isReady then
        operation()
    else
        table.insert(self.pendingOperations, operation)
    end

    return true
end

---@param queueId   string
---@param onSuccess function?
---@param onError   function?
---@return boolean
function MqMessageBroker:createQueue(queueId, onSuccess, onError)
    if not queueId then
        if onError then
            onError("Неверный queueId")
        end
        return false
    end

    queueId = tostring(queueId)

    return self:_whenReady(function ()
        promises:add(
            world.sendEntityMessage(player.id(), "mqSocket:createQueue", self.socketId, queueId),
            function (result)
                if result then
                    if onSuccess then
                        onSuccess(result)
                    end
                elseif onError then
                    onError("Не удалось создать очередь '" .. queueId .. "'")
                end
            end,
            function (error)
                if onError then
                    onError(error)
                end
                sb.logError("MqMessageBroker:createQueue error: %s", error)
            end
        )
    end, onError)
end

---@param queueId   string
---@param data      any
---@param onSuccess function?
---@param onError   function?
---@return boolean
function MqMessageBroker:publish(queueId, data, onSuccess, onError)
    if not queueId then
        if onError then
            onError("Неверный queueId")
        end
        return false
    end

    queueId = tostring(queueId)

    local brokerMessage = { fromSubscriber = self.subscriberId, data = data, timestamp = os.time() }

    return self:_whenReady(function ()
        promises:add(
            world.sendEntityMessage(player.id(), "mqSocket:publish", self.socketId, queueId, brokerMessage),
            function (result)
                if result then
                    if onSuccess then
                        onSuccess(result)
                    end
                elseif onError then
                    onError(
                        "Не удалось опубликовать сообщение в очередь '" .. queueId
                            .. "'"
                    )
                end
            end,
            function (error)
                if onError then
                    onError(error)
                end
                sb.logError("MqMessageBroker:publish error: %s", error)
            end
        )
    end, onError)
end

---@param queueId   string
---@param handler   MqMessageHandler
---@param onSuccess function?
---@param onError   function?
---@return boolean
function MqMessageBroker:subscribe(queueId, handler, onSuccess, onError)
    if not queueId then
        if onError then
            onError("Неверный queueId")
        end
        return false
    end

    if type(handler) ~= "function" then
        if onError then
            onError("handler должен быть функцией")
        end
        return false
    end

    queueId = tostring(queueId)

    return self:_whenReady(function ()
        promises:add(
            world.sendEntityMessage(player.id(), "mqSocket:subscribe", self.socketId, queueId, self.subscriberId),
            function (result)
                if result then
                    self.subscriptions[queueId] = handler

                    if onSuccess then
                        onSuccess(result)
                    end
                elseif onError then
                    onError("Не удалось подписаться на очередь '" .. queueId .. "'")
                end
            end,
            function (error)
                if onError then
                    onError(error)
                end
                sb.logError("MqMessageBroker:subscribe error: %s", error)
            end
        )
    end, onError)
end

---@param queueId   string
---@param onSuccess function?
---@param onError   function?
---@return boolean
function MqMessageBroker:unsubscribe(queueId, onSuccess, onError)
    if not queueId then
        if onError then
            onError("Неверный queueId")
        end
        return false
    end

    queueId = tostring(queueId)

    return self:_whenReady(function ()
        promises:add(
            world.sendEntityMessage(player.id(), "mqSocket:unsubscribe", self.socketId, queueId, self.subscriberId),
            function (result)
                if result then
                    self.subscriptions[queueId] = nil
                    self.receivePromises[queueId] = nil

                    if onSuccess then
                        onSuccess(result)
                    end
                elseif onError then
                    onError("Не удалось отписаться от очереди '" .. queueId .. "'")
                end
            end,
            function (error)
                if onError then
                    onError(error)
                end
                sb.logError("MqMessageBroker:unsubscribe error: %s", error)
            end
        )
    end, onError)
end

---@private
---@param queueId string
---@return RpcPromise
function MqMessageBroker:_receiveMessages(queueId)
    return world.sendEntityMessage(
        player.id(), "mqSocket:receive", self.socketId, queueId, self.subscriberId, self.batchSize
    )
end

---@private
---@param queueId       string
---@param brokerMessage MqBrokerMessage
function MqMessageBroker:_handleMessage(queueId, brokerMessage)
    local handler = self.subscriptions[queueId]
    if not handler then
        return
    end

    local success, error = pcall(
        handler, brokerMessage and brokerMessage.data, brokerMessage and brokerMessage.fromSubscriber, brokerMessage
    )

    if not success then
        sb.logError("MqMessageBroker: ошибка обработчика очереди '%s': %s", queueId, error)
    end
end

---@param size number
function MqMessageBroker:setBatchSize(size)
    if size and size > 0 then
        self.batchSize = size
    end
end

---@return string
function MqMessageBroker:getId()
    return self.socketId
end

---@return string
function MqMessageBroker:getSubscriberId()
    return self.subscriberId
end

---@return boolean
function MqMessageBroker:isConnected()
    return self.isOpen and self.isReady
end

function MqMessageBroker:update()
    if not self.isOpen or not self.isReady then
        return
    end

    for queueId, _ in pairs(self.subscriptions) do
        local promise = self.receivePromises[queueId]

        if promise and promise:finished() then
            if promise:succeeded() then
                local messages = promise:result()

                if messages and type(messages) == "table" then
                    for _, brokerMessage in ipairs(messages) do
                        self:_handleMessage(queueId, brokerMessage)
                    end
                end
            else
                sb.logError("MqMessageBroker:receive error for queue '%s': %s", queueId, promise:error())
            end

            self.receivePromises[queueId] = nil
        elseif not promise then
            self.receivePromises[queueId] = self:_receiveMessages(queueId)
        end
    end
end

---@param onSuccess function?
---@param onError   function?
function MqMessageBroker:close(onSuccess, onError)
    if not self.isOpen then
        return
    end

    self.isOpen = false
    self.isReady = false
    self.pendingOperations = {}
    self.receivePromises = {}

    promises:add(
        world.sendEntityMessage(player.id(), "mqSocket:unsubscribeAll", self.socketId, self.subscriberId),
        function (result)
            self.subscriptions = {}

            if onSuccess then
                onSuccess(result)
            end
        end,
        function (error)
            self.subscriptions = {}

            if onError then
                onError(error)
            end

            sb.logError("MqMessageBroker:close error: %s", error)
        end
    )
end

---@param onSuccess function?
---@param onError   function?
function MqMessageBroker:destroy(onSuccess, onError)
    if not self.isOpen then
        return
    end

    self.isOpen = false
    self.isReady = false
    self.pendingOperations = {}
    self.receivePromises = {}
    self.subscriptions = {}

    promises:add(
        world.sendEntityMessage(player.id(), "mqSocket:close", self.socketId),
        function (result)
            if onSuccess then
                onSuccess(result)
            end
        end,
        function (error)
            if onError then
                onError(error)
            end

            sb.logError("MqMessageBroker:destroy error: %s", error)
        end
    )
end

---@param socketId            string?
---@param subscriberId        string?
---@param maxQueueHistorySize number?
---@param onOpen              function?
---@param onError             function?
---@return MqMessageBroker
function createMqMessageBroker(socketId, subscriberId, maxQueueHistorySize, onOpen, onError)
    return MqMessageBroker.new(socketId, subscriberId, maxQueueHistorySize, onOpen, onError)
end