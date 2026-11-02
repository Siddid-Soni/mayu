-- KEYS[1]=stream KEYS[2]=result key KEYS[3]=workflow hash
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=store result ('1'/'0') ARGV[4]=record ARGV[5]=result ttl secs
-- ARGV[6]=node index of the acked task ARGV[7]=the acking consumer
-- Acks a workflow node (as ack.lua does, minus link follow-ups) and in the same step marks the node done and
-- XADDs each child that was waiting and has all its parents done. The node's state decides, not XACK's return:
-- a duplicate run or a retried ack finds it done and releases nothing. In a cancelled workflow the node
-- becomes cancelled and nothing is released. Returns the number of children released, or -1 if the
-- hash is gone (expired): the entry is still acked, nothing is released.
-- If ARGV[7] no longer owns the entry (reclaimed by another worker, or already settled), nothing is acked or
-- released and the node is untouched; a result being stored is written only if none exists (SET NX).
-- Returns -2 then.
-- Raises an error, before changing anything, if the node or a ready child is missing from the hash.
-- Hash layout (see submit.lua): n:<i>:{s,q,up,down,h,b}, plus status, done, ttl.
-- Not Redis-Cluster-safe: child stream keys are built here, not passed in KEYS. ack.lua has the same
-- limit (result keys carry no hash tag), so this adds no new restriction.
if #redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[7]) == 0 then
  if ARGV[3] == '1' then
    redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5], 'NX')
  end
  return -2
end
local wf = KEYS[3]
local exists = redis.call('EXISTS', wf) == 1
local state, status, ttl
local ready = {}
if exists then
  state = redis.call('HGET', wf, 'n:' .. ARGV[6] .. ':s')
  status = redis.call('HGET', wf, 'status')
  ttl = tonumber(redis.call('HGET', wf, 'ttl'))
  if not state or not ttl then
    return redis.error_reply('malformed workflow ' .. wf .. ': node ' .. ARGV[6] .. ' or ttl missing')
  end
  if state ~= 'done' and state ~= 'cancelled' and status ~= 'cancelled' then
    local down = redis.call('HGET', wf, 'n:' .. ARGV[6] .. ':down') or ''
    for c in string.gmatch(down, '[^,]+') do
      local p = 'n:' .. c .. ':'
      if redis.call('HGET', wf, p .. 's') == 'waiting' then
        local all = true
        local up = redis.call('HGET', wf, p .. 'up') or ''
        for parent in string.gmatch(up, '[^,]+') do
          if parent ~= ARGV[6] and redis.call('HGET', wf, 'n:' .. parent .. ':s') ~= 'done' then
            all = false
            break
          end
        end
        if all then
          local q, h, b = unpack(redis.call('HMGET', wf, p .. 'q', p .. 'h', p .. 'b'))
          if not q or not h or not b then
            return redis.error_reply('malformed workflow ' .. wf .. ': node ' .. c .. ' has no stored task')
          end
          ready[#ready + 1] = { c, q, h, b }
        end
      end
    end
  end
end

redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[3] == '1' then
  redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5])
end
if not exists then
  return -1
end
if state == 'done' or state == 'cancelled' then
  return 0
end
if status == 'cancelled' then
  redis.call('HSET', wf, 'n:' .. ARGV[6] .. ':s', 'cancelled')
  redis.call('EXPIRE', wf, ttl)
  return 0
end
redis.call('HSET', wf, 'n:' .. ARGV[6] .. ':s', 'done')
redis.call('HINCRBY', wf, 'done', 1)
for _, r in ipairs(ready) do
  local p = 'n:' .. r[1] .. ':'
  redis.call('HSET', wf, p .. 's', 'queued')
  redis.call('HDEL', wf, p .. 'h', p .. 'b')
  redis.call('XADD', 'mayu:{' .. r[2] .. '}:stream', '*', 'h', r[3], 'p', r[4])
end
redis.call('EXPIRE', wf, ttl)
return #ready
