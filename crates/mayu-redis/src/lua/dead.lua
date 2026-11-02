-- KEYS[1]=stream KEYS[2]=dead stream KEYS[3]=result key KEYS[4]=workflow hash (optional)
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=header ARGV[4]=payload ARGV[5]=error
-- ARGV[6]=store result ('1'/'0') ARGV[7]=record ARGV[8]=ttl secs ARGV[9]=node index (with KEYS[4])
-- The last ARGV is the consumer: if it no longer owns the entry nothing changes and 0 is returned (1 when applied).
-- A workflow node that exists in the hash and isn't done is marked dead, which keeps its descendants waiting.
local consumer = ARGV[#ARGV]
if #redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, consumer) == 0 then
  return 0
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[2], '*', 'h', ARGV[3], 'p', ARGV[4], 'e', ARGV[5])
if ARGV[6] == '1' then
  redis.call('SET', KEYS[3], ARGV[7], 'EX', ARGV[8])
end
if KEYS[4] and ARGV[9] then
  local field = 'n:' .. ARGV[9] .. ':s'
  local state = redis.call('HGET', KEYS[4], field)
  if state and state ~= 'done' then
    redis.call('HSET', KEYS[4], field, 'dead')
  end
end
return 1
