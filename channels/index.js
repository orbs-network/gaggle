module.exports = {
  memory: require('./in-memory-channel')
, 'socket.io': require('./socket-io-channel')
, redis: require('./redis-channel')
, custom: require('./custom-channel')
}
