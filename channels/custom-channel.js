var ChannelInterface = require('./channel-interface')
  , Joi = require('joi')
  , prettifyJoiError = require('../helpers/prettify-joi-error')
  , util = require('util')
  , _ = require('lodash')

function CustomChannel(opts) {
  var validatedOptions = Joi.validate(opts || {}, Joi.object().keys({
    channelOptions: Joi.object().keys({
      connector: Joi.object()
    })
    , logFunction: Joi.func()
    , id: Joi.string()
  }).requiredKeys('channelOptions', 'channelOptions.connector'), {
      convert: false
    })

  ChannelInterface.apply(this, Array.prototype.slice.call(arguments))

  if (validatedOptions.error != null) {
    throw new Error(prettifyJoiError(validatedOptions.error))
  }

  this._connector = _.get(validatedOptions, 'value.channelOptions.connector')
}

util.inherits(CustomChannel, ChannelInterface)

CustomChannel.prototype._connect = function _connect() {
  const self = this;

  this._connector.connect();
  this._connected()

  this._connector.on('received', function received(originNodeId, message) {
    self._recieved(originNodeId, message);
  });
}

CustomChannel.prototype._disconnect = function _disconnect() {
  this._connector.disconnect();
  this._disconnected()
}

CustomChannel.prototype._broadcast = function _broadcast(data) {
  this._connector.broadcast({
    from: this.id, data: data
  });
}

CustomChannel.prototype._send = function _send(nodeId, data) {
  this._connector.send(nodeId, {
    from: this.id,
    to: nodeId,
    data: data
  });
}

module.exports = CustomChannel;
