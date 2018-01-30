module.exports = {
  memory: require('./in-memory-channel')
, redis: require('./redis-channel')
, custom: require('./custom-channel')
}
