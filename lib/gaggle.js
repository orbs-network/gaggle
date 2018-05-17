var Joi = require('joi')
  , util = require('util')
  , Promise = require('bluebird')
  , _ = require('lodash')
  , EventEmitter = require('events').EventEmitter
  , uuid = require('uuid')
  , once = require('once')
  , prettifyJoiError = require('../helpers/prettify-joi-error')
  , STATES = {
      CANDIDATE: 'CANDIDATE'
    , LEADER: 'LEADER'
    , FOLLOWER: 'FOLLOWER'
    }
  , RPC_TYPE = {
      REQUEST_VOTE: 'REQUEST_VOTE'
    , REQUEST_VOTE_REPLY: 'REQUEST_VOTE_REPLY'
    , APPEND_ENTRIES: 'APPEND_ENTRIES'
    , APPEND_ENTRIES_REPLY: 'APPEND_ENTRIES_REPLY'
    , APPEND_ENTRY: 'APPEND_ENTRY'
    , DISPATCH: 'DISPATCH'
    , DISPATCH_SUCCESS: 'DISPATCH_SUCCESS'
    , DISPATCH_ERROR: 'DISPATCH_ERROR'
    }

Promise.config({
  warnings: {wForgottenReturn: false}
})

function Gaggle (opts) {
  var self = this
    , validatedOptions = Joi.validate(opts, Joi.object().keys({
        clusterSize: Joi.number().min(1)
      , channel: Joi.object()
      , id: Joi.string()
      , electionTimeout: Joi.object().keys({
          min: Joi.number().min(0)
        , max: Joi.number().min(Joi.ref('min'))
        }).default({min: 300, max: 500})
      , msgLimit: Joi.number().default(4000000)  // this will determine the number of entries sent at once
      , heartbeatInterval: Joi.number().min(0).default(50)
      , accelerateHeartbeats: Joi.boolean().default(false)
      , rpc: Joi.object().default()
      }).requiredKeys('id', 'channel', 'clusterSize'), {
        convert: false
      })
    , electMin
    , electMax
    , heartbeatInterval

  if (validatedOptions.error != null) {
    throw new Error(prettifyJoiError(validatedOptions.error))
  }

  // For convenience
  electMin = validatedOptions.value.electionTimeout.min
  electMax = validatedOptions.value.electionTimeout.max
  this._clusterSize = validatedOptions.value.clusterSize
  this._unlockTimeout = validatedOptions.value.unlockTimeout
  this._rpc = validatedOptions.value.rpc
  this._heartbeatInterval = validatedOptions.value.heartbeatInterval

  this.id = validatedOptions.value.id
  this._closed = false

  opts.channel.connect()
  this._channel = opts.channel

  // Used for internal communication, such as when an entry is committed
  this._emitter = new EventEmitter()
  this._emitter.setMaxListeners(100)

  // "When servers start up, they begin as followers"
  this._peers = {}
  this._state = STATES.FOLLOWER
  this._leader = null 

  this._currentTerm = 0
  this._votedFor = null
  this._log = [] // [{term: 1, data: {}}, ...]
  this._commitIndex = -1
  this._lastApplied = -1
  // Volatile state on candidates
  this._votes = {}
  // Volatile state on leaders
  this._nextIndex = {}
  this._matchIndex = {}


  // TODO: finish baching
  this._msgLimit = validatedOptions.value.msgLimit

  // Proxy these internal events to the outside world
  this._emitter.on('appended', function () {
    self.emit.apply(self, ['appended'].concat(Array.prototype.slice.call(arguments)))
  })

  this._emitter.on('committed', function () {
    self.emit.apply(self, ['committed'].concat(Array.prototype.slice.call(arguments)))
  })

  this._emitter.on('leaderElected', function () {
    self.emit.apply(self, ['leaderElected'].concat(Array.prototype.slice.call(arguments)))
  })


  // "If a follower recieves no communication over a period of time called the election timeout
  // then it assumes there is no viable leader and begins an election to choose a new leader"
  this._lastCommunicationTimestamp = Date.now()

  this._generateRandomElectionTimeout = function _generateRandomElectionTimeout () {
    return _.random(electMin, electMax, false) // no floating points
  }

  // Gad: new leader flow
  this._becomeLeader = function _becomeLeader () {
      if (self._electionTimeout != null) { // Gad: Leader should not trigger new election..
        clearTimeout(self._electionTimeout)
      }   
      self._state = STATES.LEADER
      // Volatile state on leaders
      self._nextIndex = {} 
      self._matchIndex = {}
      self._beginHeartbeat()
      self._emitter.emit('leaderElected')
  }



  this._sendHeartbeat = function _sendHeartbeat () {
      self._sendAppendEntries()
  }

  // Gad: Leader sends this message as hearbeat - maintain leadership
  // Message might contain entries to sync
  this._sendAppendEntries = function () {
    if (self._state === STATES.LEADER) {
        _.each(self._peers, function (v, nodeId) {
          var entriesToSend = []
            , prevLogIndex = -1
            , prevLogTerm = -1
            , totalBytesToSend = 0 // approximation

          // Initialize to leader last log index + 1 if empty
          // (Reset after each election)
          if (self._nextIndex[nodeId] == null) {
            self._nextIndex[nodeId] = self._log.length
          }

          // support batching - updating a node with batchSize * block at a time - depend on rpc limit

          for (var i=self._nextIndex[nodeId], ii= Math.min(self._log.length, self._nextIndex[nodeId] + 2); i<ii; ++i) {
            totalBytesToSend += JSON.stringify(self._log[i]).length
            if (totalBytesToSend > self._msgLimit)
              break;
            entriesToSend.push(_.extend({index: i}, self._log[i]))
          }
    
          if (entriesToSend.length > 0) {
            prevLogIndex = entriesToSend[0].index - 1
    
            if (prevLogIndex > -1) {
              prevLogTerm = self._log[prevLogIndex].term
            }
          }
    
          console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'sending a heartbeat to', nodeId);
    
          self._channel.send(nodeId, {
            type: RPC_TYPE.APPEND_ENTRIES
          , term: self._currentTerm
          , leaderId: self.id
          , prevLogIndex: prevLogIndex
          , prevLogTerm: prevLogTerm
          , entries: entriesToSend
          , leaderCommit: self._commitIndex
          })
        })
    }

  }

  // Gad: altered for readability
  this._beginHeartbeat = function _beginHeartbeat () {

    if (self._leaderHeartbeatInterval )
        clearInterval(self._leaderHeartbeatInterval)

    self._sendHeartbeat()

    self._leaderHeartbeatInterval = setInterval(self._sendHeartbeat, self._heartbeatInterval)

    if (validatedOptions.value.accelerateHeartbeats) {
      self._forceHeartbeat = function _forceHeartbeat () {
        clearInterval(self._leaderHeartbeatInterval)
        self._sendHeartbeat()
        self._leaderHeartbeatInterval = setInterval(self._sendHeartbeat, self._heartbeatInterval)
      }
    }
  }

  this._onMessageRecieved = _.bind(this._onMessageRecieved, this)
  this._channel.on('recieved', this._onMessageRecieved)

  this._emitter.on('dirty', function () {
    if (typeof self._forceHeartbeat === 'function') {
      self._forceHeartbeat()
    }
  })

  this._resetElectionTimeout()
}

util.inherits(Gaggle, EventEmitter)


// leader steps down - for the network to maintain progress
Gaggle.prototype.stepDown = function () {
  if (self._state === STATES.LEADER) {
      clearInterval(self._leaderHeartbeatInterval)
      self.logger.log(' Leader Step Down: ' + self.id); 
      self._state = STATES.FOLLOWER
      self._leader = null
      self._resetElectionTimeout()
  }
}

Gaggle.prototype.getLog = function () {
  return this._log
}

Gaggle.prototype.getCommitIndex = function () {
  return this._commitIndex
}

Gaggle.prototype.isLeader = function isLeader () { 
  return this._state === STATES.LEADER
}

Gaggle.prototype._onMessageRecieved = function _onMessageRecieved (originNodeId, data) {
  var self = this
    , i
    , ii

  // self._resetElectionTimeout() // TODO: Seems wrong - when sent not from leader .. only on heartbeat ..

  self._handleMessage(originNodeId, data)

  // If we are the leader, must try to increase our commitIndex here, so that
  // followers will find out about it and increment their own commitIndex
  if (self._state === STATES.LEADER) {
    // After handling any message, check to see if we can increment our commitIndex
    var highestPossibleCommitIndex = -1

    console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'old commitIndex', self._commitIndex);

    console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'current matchIndex', JSON.stringify(self._matchIndex));

    for (i=self._commitIndex + 1, ii=self._log.length; i<ii; ++i) {
      if (self._log[i].term === self._currentTerm) {
        console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'log entry #', i, JSON.stringify(_.omit(self._log[i], 'data')));
      }
      
      // Log entries must be from the current term
      if (self._log[i].term === self._currentTerm &&
          // And there must be a majority of matchIndexes >= i
          (_.filter(self._matchIndex, function (matchIndex) {
            return matchIndex >= i
          }).length >= Math.floor(self._clusterSize/2))) { // including one self
        highestPossibleCommitIndex = i
      }
    }

    if (highestPossibleCommitIndex > self._commitIndex) {
      self._commitIndex = highestPossibleCommitIndex
      self._emitter.emit('dirty')
    }
  }

  console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'current commitIndex', self._commitIndex);

  // All nodes should commit entries between lastApplied and commitIndex
  for (i=self._lastApplied + 1, ii=self._commitIndex; i<=ii; ++i) {
    console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'emitting committed');
    self._emitter.emit('committed', JSON.parse(JSON.stringify(self._log[i])), i)
    self._lastApplied = i
  }
}

Gaggle.prototype._handleMessage = function _handleMessage (originNodeId, data) {

  var self = this
    , conflictedAt = -1
    , matchIndex = -1
    , result = false
    , entry

  // data always has the following keys:
  // {
  //   type: the RPC method call or response
  //   term: some integer
  // }

  if (originNodeId !== self.id) {
    this._peers[originNodeId] = true;
  }

  if (data.term > self._currentTerm) {
    clearInterval(self._leaderHeartbeatInterval)
    self._currentTerm = data.term
    self._leader = null
    self._votedFor = null
    self._state = STATES.FOLLOWER
  }

  switch (data.type) {
    
     // Gad: ok
    case RPC_TYPE.REQUEST_VOTE: 
    self.logger.log("REQUEST_VOTE")
    voteGranted = false
    // Gad: Leader completness: We only elect a leader which has up to date log, based on term and index 
    // check candidate is up to date
    if ((data.term == self._currentTerm) && (self._votedFor === null || self._votedFor == data.candidateId)) { 
      var lastLogEntry = _.last(self._log)
      , lastLogTerm = lastLogEntry != null ? lastLogEntry.term : -1
      , candidateIsAtLeastAsUpToDate = data.lastLogTerm > lastLogTerm || // Its either in a later term...
                                      // or same term, and at least at the same index
                                      data.lastLogTerm == lastLogTerm && data.lastLogIndex >= self._log.length - 1
      if (candidateIsAtLeastAsUpToDate) {     // candidate is up to date
          self._votedFor = data.candidateId
          voteGranted = true
          self._resetElectionTimeout()
      }
    }

    self._channel.send(originNodeId, {
      type: RPC_TYPE.REQUEST_VOTE_REPLY
    , term: self._currentTerm
    , voteGranted: voteGranted
    })

    break

    // Gad: ok
    case RPC_TYPE.REQUEST_VOTE_REPLY:

    if (self._state === STATES.CANDIDATE && 
        data.term === self._currentTerm) {

      self._votes[originNodeId] = data.voteGranted
      elected = _.filter(self._votes, function(v) { return v}).length >= Math.floor(self._clusterSize/2) // including one self
      if (elected) { // Wait for a majority - including itself -> decrease one 
        //changed for clarity
        self._becomeLeader()
      }
    }
    break


    case RPC_TYPE.APPEND_ENTRIES:
      
      self.logger.log('received new data entries: ' +  _.size(data.entries) + ' from ' +  data.leaderId + ' term '+ data.term + ' log size is' + _.size(self._log)); 

      // candidate steps down
      if (data.term == self._currentTerm) { // && self._state !== STATES.LEADER) {
        self.logger.log(' This is how you lose an election to: ' + originNodeId); 
        self._state = STATES.FOLLOWER
        self._leader = originNodeId
        self._emitter.emit('leaderElected')
        self._resetElectionTimeout()

        // 2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (section 3.5)
        if ((data.prevLogIndex === -1) || 
            (self._log[data.prevLogIndex] && self._log[data.prevLogIndex].term == data.prevLogTerm )) {
            result = true;
            matchIndex =  data.prevLogIndex;
            // 3. If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it. (section 3.5)
            _.each(data.entries, function (entry) {
              // entry is:
              // {index: 0, term: 0, data: {foo: bar}}
              var idx = entry.index
              if (self._log[idx] != null && self._log[idx].term !== entry.term) {
                conflictedAt = conflictedAt > -1 ? Math.min(conflictedAt, idx): idx;
              }
            })

            if (conflictedAt > -1) {
              self._log = self._log.slice(0, conflictedAt)  
            }
        
            // 4. Append any new entries not already in the log
            _.each(data.entries, function (entry) {
              var idx = entry.index
              if (self._log[idx] == null) {
                self._log[idx] = {
                  term: entry.term
                , data: entry.data
                , id: entry.id
                }
              }
            })

            if (data.entries.length > 0)
                matchIndex = self._log.length - 1
        
            console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'log after merging new entries', _.size(self._log));
        
            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (data.leaderCommit > self._commitIndex) {
              self._commitIndex = Math.min(data.leaderCommit, self._log.length - 1)
            }
            console.log('Gaggle:', self.id, 'term', self._currentTerm, '(current leader:', self._leader, ')', 'new commit index is', _.size(self._commit));
        }
      }

      self._channel.send(originNodeId, {
        type: RPC_TYPE.APPEND_ENTRIES_REPLY
      , term: self._currentTerm
      , success: result
      , matchIndex: matchIndex  
      })
        
    break

    //  
    case RPC_TYPE.APPEND_ENTRIES_REPLY:
      if (self._state === STATES.LEADER && self._currentTerm === data.term) {
        if (data.success === true && data.matchIndex > -1) {
          self._nextIndex[originNodeId] = data.matchIndex + 1
          self._matchIndex[originNodeId] = data.matchIndex
        }
        else {
          self._nextIndex[originNodeId] = Math.max(self._nextIndex[originNodeId] - 1, 0)
        }
      }
    break




    case RPC_TYPE.APPEND_ENTRY:
      if (self._state === STATES.LEADER) {
        console.log('Gaggle:', self.id, 'term', self._currentTerm, '$$$$$$$$$$$$$$$$$$$    RPC_TYPE.APPEND_ENTRY)');
        entry = {
          term: self._currentTerm
        , id: data.id
        , data: data.data
        }

        self._log.push(entry)
        self._emitter.emit('appended', entry, self._log.length - 1)
        self._emitter.emit('dirty')
      }
    break



    case RPC_TYPE.DISPATCH:
    if (self._state === STATES.LEADER && self._currentTerm === data.term) {
      self._dispatchOnLeader(data.id, data.methodName, data.args).catch(_.noop)
    }
    break

    case RPC_TYPE.DISPATCH_ERROR:
      var err = new Error(data.message)
      err.stack = data.stack
      self._emitter.emit.apply(self._emitter, ['rpcError', data.id, err])
    break

    case RPC_TYPE.DISPATCH_SUCCESS:
      self._emitter.emit.apply(self._emitter, ['rpcSuccess', data.id, data.returnValue])
    break
  }
}




// Gad: ok
Gaggle.prototype._resetElectionTimeout = function _resetElectionTimeout () {
  var self = this
    , timeout = self._generateRandomElectionTimeout()

  self._timeout = timeout;

  if (self._electionTimeout != null) {
    clearTimeout(self._electionTimeout)
  }

  self._electionTimeout = setTimeout(function () {
    self._resetElectionTimeout()
    self._beginElection()
  }, timeout)
}

// Gad: ok
Gaggle.prototype._beginElection = function _beginElection () {
  var self = this
  if (self._state === STATES.LEADER) 
      return

  // To begin an election, a follower increments its current term and transitions to
  // candidate state. It then votes for itself and issues RequestVote RPCs in parallel
  // to each of the other servers in the cluster.
  var lastLogIndex

  self._currentTerm = self._currentTerm + 1
  self._state = STATES.CANDIDATE
  self._leader = null
  self._votedFor = self.id
  self._votes = {}

  lastLogIndex = self._log.length - 1

  self._channel.broadcast({
    type: RPC_TYPE.REQUEST_VOTE
  , term: self._currentTerm
  , candidateId: self.id
  , lastLogIndex: lastLogIndex
  , lastLogTerm: (lastLogIndex > -1 &&  lastLogIndex < self._log.length) ? self._log[lastLogIndex].term : -1 

  })
}


// Gad: ok
Gaggle.prototype.close = function close (cb) {
  var self = this
    , p

  this._channel.removeListener('recieved', this._onMessageRecieved)
  clearTimeout(this._electionTimeout)
  clearInterval(this._leaderHeartbeatInterval)

  this._emitter.removeAllListeners()

  p = new Promise(function (resolve, reject) {
    self._channel.once('disconnected', function () {
      resolve()
    })
    self._channel.disconnect()
  })

  if (cb != null) {
    p.then(_.bind(cb, null, null)).catch(cb)
  }
  else {
    return p
  }
}


Gaggle.prototype.logger = {}
Gaggle.prototype.logger.log = function log(msg) {
  var self = this
  var logMsg = 'Gaggle-node-' +  self.id + ':' + (msg? '$ msg: ' +  msg +  '$ ': '' )  + ' $ internal state: ' + self._internalState +  ' $'
  console.log(logMsg);
}

Gaggle.prototype.append = function append (data, timeout, cb) {
  var self = this
    , msgId = self.id + '_' + uuid.v4()
    , performRequest
    , p

  if (typeof timeout === 'function') {
    cb = timeout
    timeout = -1
  }

  timeout = typeof timeout === 'number' ? timeout : -1

  /**
  * This odd pattern is because there is a possibility that
  * we were elected the leader after the leaderElected event
  * fires. So we wait until its time to perform the request
  * to decide if we need to delegate to the leader, or perform
  * the logic ourselves
  */
  performRequest = once(function performRequest () {
    var entry

    if (self._state === STATES.LEADER) {
      entry = {
        term: self._currentTerm
      , data: data
      , id: msgId
      }

      self._log.push(entry)
      console.log('$$$%$%$ appended', entry.data)
      self._emitter.emit('appended', entry, self._log.length - 1)
    }
    else {
      self._channel.send(self._leader, {
        type: RPC_TYPE.APPEND_ENTRY
      , data: data
      , id: msgId
      })
    }
  })

  if (self._state === STATES.LEADER) {
    performRequest()
  }
  else if (self._state === STATES.FOLLOWER && self._leader != null) {
    performRequest()
  }
  else {
    self._emitter.once('leaderElected', performRequest)
  }

  p = new Promise(function (resolve, reject) {
    var resolveOnCommitted = function _resolveOnCommitted (entry) {
          if (entry.id === msgId) {
            resolve()
            cleanup()
          }
        }
      , cleanup = function _cleanup () {
          self._emitter.removeListener('leaderElected', performRequest)
          self._emitter.removeListener('committed', resolveOnCommitted)
          clearTimeout(timeoutHandle)
        }
      , timeoutHandle

    // And wait for acknowledgement...
    self._emitter.on('committed', resolveOnCommitted)

    // Or if we time out before the message is committed...
    if (timeout > -1) {
      timeoutHandle = setTimeout(function _failOnTimeout () {
        reject(new Error('Timed out before the entry was committed'))
        cleanup()
      }, timeout)
    }
  })

  if (cb != null) {
    p.then(_.bind(cb, null, null)).catch(cb)
  }
  else {
    return p
  }
}


Gaggle.prototype.hasUncommittedEntriesInPreviousTerms = function hasUncommittedEntriesInPreviousTerms () {
  var self = this

  return _.find(self._log, function (entry, idx) {
    return entry.term < self._currentTerm && idx > self._commitIndex
  }) != null
}


Gaggle.prototype.dispatchOnLeader = function dispatchOnLeader () {
  var args = Array.prototype.slice.call(arguments)
  return this._dispatchOnLeader.apply(this, [uuid.v4()].concat(args))
}

Gaggle.prototype._dispatchOnLeader = function _dispatchOnLeader (rpcId, methodName, args, timeout, cb) {
  var self = this
    , performRequest
    , p

  if (typeof timeout === 'function') {
    cb = timeout
    timeout = -1
  }

  timeout = typeof timeout === 'number' ? timeout : -1

  /**
  * This odd pattern is because there is a possibility that
  * we were elected the leader after the leaderElected event
  * fires. So we wait until its time to perform the request
  * to decide if we need to delegate to the leader, or perform
  * the logic ourselves
  */
  performRequest = once(function performRequest () {
    if (self._state === STATES.LEADER) {

      if (self._rpc[methodName] == null) {
        self._channel.broadcast({
          type: RPC_TYPE.DISPATCH_ERROR
        , term: self._currentTerm
        , id: rpcId
        , message: 'The RPC method ' + methodName + ' does not exist'
        , stack: 'Gaggle.prototype._dispatchOnLeader'
        })
      }
      else {
        var usedDeprecatedAPI = false
          , result = self._rpc[methodName].apply(self, args.concat([function () {
              usedDeprecatedAPI = true
            }]))

        if (usedDeprecatedAPI) {
          result = new Error('As of version 3, RPC calls are synchronous, and should return a value rather than use this callback. This warning will be removed in a future version.')
        }

        if (result instanceof Error) {
          self._channel.broadcast({
              type: RPC_TYPE.DISPATCH_ERROR
            , term: self._currentTerm
            , id: rpcId
            , message: result.message
            , stack: result.stack
          })
        }
        else {
          self._channel.broadcast({
            type: RPC_TYPE.DISPATCH_SUCCESS
          , term: self._currentTerm
          , id: rpcId
          , returnValue: result
          })
        }
      }
    }
    else {
      self._channel.send(self._leader, {
        type: RPC_TYPE.DISPATCH
      , term: self._currentTerm
      , id: rpcId
      , methodName: methodName
      , args: args
      })
    }
  })

  if (self._state === STATES.LEADER) {
    performRequest()
  }
  else if (self._state === STATES.FOLLOWER && self._leader != null) {
    performRequest()
  }
  else {
    self._emitter.once('leaderElected', performRequest)
  }

  p = new Promise(function (resolve, reject) {
    var resolveOnAck = function _resolveOnAck (id, ret) {
          if (id === rpcId) {
            resolve(ret)
            cleanup()
          }
        }
      , rejectOnError = function _rejectOnError(id, err) {
          if (id === rpcId) {
            reject(err)
            cleanup()
          }
        }
      , cleanup = function _cleanup () {
          self._emitter.removeListener('leaderElected', performRequest)
          self._emitter.removeListener('rpcSuccess', resolveOnAck)
          self._emitter.removeListener('rpcError', rejectOnError)
          clearTimeout(timeoutHandle)
        }
      , timeoutHandle

    // And wait for acknowledgement...
    self._emitter.on('rpcSuccess', resolveOnAck)
    self._emitter.on('rpcError', rejectOnError)

    // Or if we time out before the message is committed...
    if (timeout > -1) {
      timeoutHandle = setTimeout(function _failOnTimeout () {
        reject(new Error('Timed out before the rpc method returned'))
        cleanup()
      }, timeout)
    }
  })

  if (cb != null) {
    p.then(function (args) {
      cb.apply(null, [null].concat(args))
    }).catch(cb)
  }
  else {
    return p
  }
}

module.exports = Gaggle
module.exports._STATES = _.cloneDeep(STATES)
module.exports._RPC_TYPE = _.cloneDeep(RPC_TYPE)

