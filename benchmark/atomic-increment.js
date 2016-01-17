var Benchmark = require('benchmark')
  , bars = require('jstrace-bars')
  , atomicIncrement = require('../lib/atomic-increment')
  , uuid = require('uuid')
  , assert = require('assert')
  , _ = require('lodash')
  , suite = new Benchmark.Suite()
  , CLUSTER_SIZE = 5
  , INCREMENTS_PER_PROCESS = 20

// add tests
suite
.add('Redis', {
  defer: true
, fn: function (deferred) {
    var Strategy = require('../strategies/redis-strategy')

    atomicIncrement(function () {
      // Gives us coverage for both default and explicit init
      return new Strategy({
        strategyOptions: {
          redisConnectionString: 'redis://127.0.0.1'
        }
      , id: uuid.v4()
      })
    }
    , CLUSTER_SIZE, INCREMENTS_PER_PROCESS, function (err) {
      assert.ifError(err, 'There should be no error')

      deferred.resolve()
    })
  }
})
.add('Raft (Accelerated)', {
  defer: true
, minSamples: 10
, fn: function (deferred) {
    var Strategy = require('../strategies/raft-strategy')
      , Channel = require('../channels/in-memory-channel')

    atomicIncrement(function () {
      var id = uuid.v4()
        , chan = new Channel({
            id: id
          })
        , strat = new Strategy({
            id: id
          , channel: chan
          , strategyOptions: {
              clusterSize: CLUSTER_SIZE
            , forceHeartbeat: true
            }
          })

      return strat
    }
    , CLUSTER_SIZE, INCREMENTS_PER_PROCESS, function (err) {
      assert.ifError(err, 'There should be no error')
      deferred.resolve()
    })
  }
})
.add('Raft (Vanilla)', {
  defer: true
, fn: function (deferred) {
    var Strategy = require('../strategies/raft-strategy')
      , Channel = require('../channels/in-memory-channel')

    atomicIncrement(function () {
      var id = uuid.v4()
        , chan = new Channel({
            id: id
          })
        , strat = new Strategy({
            id: id
          , channel: chan
          , strategyOptions: {
              clusterSize: CLUSTER_SIZE
            , forceHeartbeat: false
            }
          })

      return strat
    }
    , CLUSTER_SIZE, INCREMENTS_PER_PROCESS, function (err) {
      assert.ifError(err, 'There should be no error')
      deferred.resolve()
    })
  }
})
// add listeners
.on('cycle', function (event) {
  console.log(String(event.target))
})
.on('complete', function () {
  var totalIncrements = CLUSTER_SIZE * INCREMENTS_PER_PROCESS
    , data = {}

  this.each(function (bench) {
    data[bench.name] = _.round(1000 / bench.hz / totalIncrements, 2)
  })

  console.log('\n\n')

  console.log(bars(data, {
    sort: true
  }))
})
// run async
.run({ 'async': true })
