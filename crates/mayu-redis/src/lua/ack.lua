-- KEYS[1]=stream KEYS[2]=result key KEYS[3..]=follow-up streams
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=store result ('1'/'0') ARGV[4]=record ARGV[5]=ttl secs
-- ARGV[6..]=follow-up (header, payload) pairs, one per KEYS[3..], then the last ARGV = the acking consumer
-- Follow-ups (link callbacks) are added only by the call that acks the entry.
-- If that consumer no longer owns the entry (reclaimed by another worker, or already settled) nothing is acked or
-- added; a result being stored is written only if none exists (SET NX). Returns 0 then, 1 when applied.
local consumer = ARGV[#ARGV]
if #redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, consumer) == 0 then
  if ARGV[3] == '1' then
    redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5], 'NX')
  end
  return 0
end
local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
if ARGV[3] == '1' then
  redis.call('SET', KEYS[2], ARGV[4], 'EX', ARGV[5])
end
if acked == 1 then
  for i = 3, #KEYS do
    local a = 6 + (i - 3) * 2
    redis.call('XADD', KEYS[i], '*', 'h', ARGV[a], 'p', ARGV[a + 1])
  end
end
return 1
