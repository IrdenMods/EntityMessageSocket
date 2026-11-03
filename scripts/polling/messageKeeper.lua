require("/scripts/messageutil.lua")

---@class Socket
---@field queue  table
---@field isOpen boolean

---@type table<string, Socket>
local sockets = {}
---@type table<string, MqSocket>
local mqSockets = {}

local function getLastKey(table)
    local lastKey = nil
    for key, _ in pairs(table) do
        lastKey = key
    end
    return lastKey
end

---@class MqSocket
---@field queues              table<string, Queue>
---@field isOpen              boolean
---@field maxQueueHistorySize number
MqSocket = {
    queues = {},
    isOpen = true,
    maxQueueHistorySize = 1000
}
MqSocket.__index = MqSocket

---@param socketId            string
---@param maxQueueHistorySize number
---@return MqSocket
function MqSocket:new(socketId, maxQueueHistorySize)
    local self = setmetatable({}, MqSocket)
    self.socketId = socketId
    self.queues = {}
    self.isOpen = true
    self.maxQueueHistorySize = maxQueueHistorySize or 1000
    return self
end

---@class Queue
---@field id          string?
---@field messages    table<number, any[]>
---@field subscribers table<string, Subscriber>
Queue = {
    id = nil,
    messages = {},
    subscribers = {}
}
Queue.__index = Queue

---@param id string
---@return Queue
function Queue.new(id)
    local self = setmetatable({}, Queue)
    self.id = id
    self.messages = {}
    self.subscribers = {}
    return self
end

---@param queueId string
---@return boolean
function MqSocket:createQueue(queueId)
    if not queueId then
        return false
    end
    queueId = tostring(queueId)
    if self.queues[queueId] then
        return true
    end

    self.queues[queueId] = Queue.new(queueId)
    return true
end

function MqSocket:addMessage(queueId, message)
    if not queueId or not message then
        return false
    end
    queueId = tostring(queueId)
    if not self.queues[queueId] then
        return false
    end

    local queues = self.queues[queueId]
    for subscriberId, _ in pairs(queues.subscribers) do
        if not queues.messages[subscriberId] then
            queues.messages[subscriberId] = {}
        end
        table.insert(queues.messages[subscriberId], deepCopy(message))
        if #queues.messages[subscriberId] > self.maxQueueHistorySize then
            table.remove(queues.messages[subscriberId], 1)
        end
    end
    return true
end

function MqSocket:receiveMessages(queueId, subscriberId, batchSize)
    if not queueId or not subscriberId then
        return {}
    end
    queueId = tostring(queueId)
    subscriberId = tostring(subscriberId)
    if not self.queues[queueId] or not self.queues[queueId].subscribers[subscriberId] then
        return {}
    end

    local queues = self.queues[queueId]
    local messages = queues.messages[subscriberId] or {}
    local batch = {}

    for _ = 1, math.min(batchSize or 10, #messages) do
        table.insert(batch, messages[1])
        table.remove(messages, 1)
    end
    return batch
end

function MqSocket:subscribe(queueId, subscriberId)
    if not queueId or not subscriberId then
        return false
    end
    queueId = tostring(queueId)
    subscriberId = tostring(subscriberId)
    if not self.queues[queueId] then
        return false
    end

    local queues = self.queues[queueId]
    if queues.subscribers[subscriberId] then
        return true -- Идемпотентная подписка
    end

    queues.subscribers[subscriberId] = Subscriber.new(subscriberId)
    queues.messages[subscriberId] = {}
    return true
end

function MqSocket:unsubscribe(queueId, subscriberId)
    if not queueId or not subscriberId then
        return false
    end
    queueId = tostring(queueId)
    subscriberId = tostring(subscriberId)
    if not self.queues[queueId] or not self.queues[queueId].subscribers[subscriberId] then
        return false
    end

    local queues = self.queues[queueId]
    queues.subscribers[subscriberId] = nil
    queues.messages[subscriberId] = nil
    return true
end

function MqSocket:unsubscribeAll(subscriberId)
    if not subscriberId then
        return false
    end

    subscriberId = tostring(subscriberId)
    for _, queue in pairs(self.queues) do
        queue.subscribers[subscriberId] = nil
        queue.messages[subscriberId] = nil
    end
    return true
end

function MqSocket:close()
    self.isOpen = false
    for _, queue in pairs(self.queues) do
        queue.subscribers = {}
        queue.messages = {}
    end
end

---@class Subscriber
---@field id string
Subscriber = {
    id = nil
}
Subscriber.__index = Subscriber

function Subscriber.new(id)
    local self = setmetatable({}, Subscriber)
    self.id = id
    return self
end

function deepCopy(object)
    local lookup_table = {}
    local function _copy(obj)
        if type(obj) ~= "table" then
            return obj
        elseif lookup_table[obj] then
            return lookup_table[obj]
        end
        local new_table = {}
        lookup_table[obj] = new_table
        for index, value in pairs(obj) do
            new_table[_copy(index)] = _copy(value)
        end
        return setmetatable(new_table, getmetatable(obj))
    end
    return _copy(object)
end

function init()
    self.sockets = {}
    mqSockets = {}

    message.setHandler(
        "messageSocket:create",
        localHandler(function (socketId)
            if not socketId then
                return false
            end
            if not self.sockets[socketId] then
                self.sockets[socketId] = { queue = {}, isOpen = true }
            end
            return true
        end)
    )

    -- Отправить сообщение в сокет
    message.setHandler(
        "messageSocket:send",
        localHandler(function (targetSocketId, createIfNotExists, message)
            if not targetSocketId then
                return false
            end
            if not message or type(message) ~= "table" then
                return false
            end

            if not self.sockets[targetSocketId] then
                if not createIfNotExists then
                    return false
                end
                self.sockets[targetSocketId] = { queue = {}, isOpen = true }
            end

            if not self.sockets[targetSocketId].isOpen then
                return false
            end

            table.insert(self.sockets[targetSocketId].queue, deepCopy(message))
            return true
        end)
    )

    message.setHandler(
        "messageSocket:receive",
        localHandler(
            ---@param socketId string
            ---@param batchSize number
            function (socketId, batchSize)
                if not socketId then
                    return {}
                end

                batchSize = batchSize or 10

                if not self.sockets[socketId] or not self.sockets[socketId].queue then
                    return {}
                end

                local messages = {}
                local queue = self.sockets[socketId].queue

                for _ = 1, math.min(batchSize, #queue) do
                    table.insert(messages, queue[1])
                    table.remove(queue, 1)
                end

                if #queue == 0 and not self.sockets[socketId].isOpen then
                    self.sockets[socketId] = nil
                end

                return messages
            end
        )
    )

    -- Закрыть сокет
    message.setHandler(
        "messageSocket:close",
        localHandler(function (socketId)
            if not socketId then
                return false
            end

            if self.sockets[socketId] then
                self.sockets[socketId] = nil
            end

            return true
        end)
    )

    -- Получить статус сокета
    message.setHandler(
        "messageSocket:status",
        localHandler(function (socketId)
            if not socketId then
                return nil
            end

            if not self.sockets[socketId] then
                return { exists = false, isOpen = false, queueSize = 0 }
            end

            return { exists = true, isOpen = self.sockets[socketId].isOpen, queueSize = #self.sockets[socketId].queue }
        end)
    )

    message.setHandler(
        "messageSocket:clearAll",
        localHandler(function ()
            self.sockets = {}
            return true
        end)
    )

    message.setHandler(
        "mqSocket:create",
        localHandler(function (socketId, maxQueueSize)
            if not socketId then
                return false
            end

            socketId = tostring(socketId)
            if not mqSockets[socketId] then
                mqSockets[socketId] = MqSocket:new(socketId, maxQueueSize)
            end

            return true
        end)
    )

    message.setHandler(
        "mqSocket:createQueue",
        localHandler(function (socketId, queueId)
            if not socketId or not queueId then
                return false
            end

            socketId = tostring(socketId)
            queueId = tostring(queueId)

            local socket = mqSockets[socketId]
            if not socket or not socket.isOpen then
                return false
            end

            return socket:createQueue(queueId)
        end)
    )

    message.setHandler(
        "mqSocket:publish",
        localHandler(function (socketId, queueId, brokerMessage)
            if not socketId or not queueId or brokerMessage == nil then
                return false
            end

            socketId = tostring(socketId)
            queueId = tostring(queueId)

            local socket = mqSockets[socketId]
            if not socket or not socket.isOpen then
                return false
            end

            return socket:addMessage(queueId, brokerMessage)
        end)
    )

    message.setHandler(
        "mqSocket:subscribe",
        localHandler(function (socketId, queueId, subscriberId)
            if not socketId or not queueId or not subscriberId then
                return false
            end

            socketId = tostring(socketId)
            queueId = tostring(queueId)
            subscriberId = tostring(subscriberId)

            local socket = mqSockets[socketId]
            if not socket or not socket.isOpen then
                return false
            end

            return socket:subscribe(queueId, subscriberId)
        end)
    )

    message.setHandler(
        "mqSocket:unsubscribe",
        localHandler(function (socketId, queueId, subscriberId)
            if not socketId or not queueId or not subscriberId then
                return false
            end

            socketId = tostring(socketId)
            queueId = tostring(queueId)
            subscriberId = tostring(subscriberId)

            local socket = mqSockets[socketId]
            if not socket or not socket.isOpen then
                return false
            end

            return socket:unsubscribe(queueId, subscriberId)
        end)
    )

    message.setHandler(
        "mqSocket:unsubscribeAll",
        localHandler(function (socketId, subscriberId)
            if not socketId or not subscriberId then
                return false
            end

            socketId = tostring(socketId)
            subscriberId = tostring(subscriberId)

            local socket = mqSockets[socketId]
            if not socket or not socket.isOpen then
                return false
            end

            return socket:unsubscribeAll(subscriberId)
        end)
    )

    message.setHandler(
        "mqSocket:receive",
        localHandler(function (socketId, queueId, subscriberId, batchSize)
            if not socketId or not queueId or not subscriberId then
                return {}
            end

            socketId = tostring(socketId)
            queueId = tostring(queueId)
            subscriberId = tostring(subscriberId)

            local socket = mqSockets[socketId]
            if not socket or not socket.isOpen then
                return {}
            end

            return socket:receiveMessages(queueId, subscriberId, batchSize)
        end)
    )

    -- Уничтожает весь брокер. Обычный клиент при закрытии должен вызывать
    -- mqSocket:unsubscribeAll, а не этот handler.
    message.setHandler(
        "mqSocket:close",
        localHandler(function (socketId)
            if not socketId then
                return false
            end

            socketId = tostring(socketId)
            local socket = mqSockets[socketId]
            if socket then
                socket:close()
                mqSockets[socketId] = nil
            end

            return true
        end)
    )

    message.setHandler(
        "mqSocket:status",
        localHandler(function (socketId)
            if not socketId then
                return nil
            end

            socketId = tostring(socketId)
            local socket = mqSockets[socketId]

            if not socket then
                return { exists = false, isOpen = false, queueCount = 0 }
            end

            local queueCount = 0
            for _, _ in pairs(socket.queues) do
                queueCount = queueCount + 1
            end

            return { exists = true, isOpen = socket.isOpen, queueCount = queueCount }
        end)
    )
end
