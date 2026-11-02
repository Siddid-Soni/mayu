-- KEYS[1]=stream KEYS[2]=delayed
-- ARGV[1]=group ARGV[2]=entry id ARGV[3]=eta ms ARGV[4]=delayed member ARGV[5]=consumer
-- Does nothing and returns 0 if the consumer no longer owns the entry; returns 1 when applied.
if #redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1, ARGV[5]) == 0 then
  return 0
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
