'use strict'

let Promise = require('bluebird')
let Azure = require('azure')
let AzureStorage = require('azure-storage')
let Bunyan = require('bunyan')

let log = Bunyan.createLogger({
  name: 'auth-service',
  level: 'info'
})

let retryOps = new Azure.ExponentialRetryPolicyFilter()
let serviceBusService = Azure.createServiceBusService(
  process.env.AZURE_SERVICEBUS_ACCESSS_KEY
).withFilter(retryOps)

//let queueService = AzureStorage.createQueueService(
//  process.env.AZURE_STORAGE_ACCOUNT,
//  process.env.AZURE_STORAGE_ACCESS_KEY,
//  process.env.AZURE_STORAGE_CONNECTION_STRING
//).withFilter(retryOps)

let handleMsg = inLetter => {
  try {
    let msg = JSON.parse(inLetter.body)
    log.info('Got msg %j', msg)
    return true
  }
  catch (err) {
    log.error(err, 'handleMsgError: %s', err.message)
    return false
  }
}

let pollMessages = () => {
  serviceBusService.receiveSubscriptionMessage('auth', 'AllMessages', (inError, inLetter) => {
    if (inError) {
      log.error(inError, 'receiveSubMessagesError: %s', inError.message)
      //Promise.delay(500).done( () => {
      //  console.log('polling...')
      //  //pollMessages()
      //})
      pollMessages()
      return
    }
    console.dir(inLetter)

    let replyTo = inLetter.customProperties.msguuid

    let reply = resp => {
			serviceBusService.sendQueueMessage(replyTo, JSON.stringify(resp), inError => {
        if (inError) {
          log.error(inError, 'sendQueueReplyErorr: %s', inError.message)
        }
      })
    }

    let res = handleMsg(inLetter)
    if (res instanceof Promise) {
      res.then( resp => {
        return reply(resp)
      }).finally( () => {
        pollMessages()
      })
    }
    else {
      reply(res)
      pollMessages()
    }
  })
}

let createSub = () => {
  serviceBusService.createSubscription('auth', 'AllMessages', inError => {
    if (inError) {
      if (inError.message.indexOf('already exists') !== -1) {
        log.info('Topic already exists')
      }
      else {
        log.error(inError, 'createSubError: %s', inError.message)
        throw inError
      }
    }

    pollMessages()
  })
}

let createTopic = () => {
  serviceBusService.createTopicIfNotExists('auth', inError => {
    if (inError) {
      log.error(inError, 'createTopicError: %s', inError.message)
      throw inError
    }

    createSub()
  })
}

createTopic()
