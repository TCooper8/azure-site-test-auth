'use strict'

let Promise = require('bluebird')
let Azure = require('azure')
let Bunyan = require('bunyan')
let _ = require('lodash')

let log = Bunyan.createLogger({
  name: 'auth-service',
  level: 'info'
})

let retryOps = new Azure.ExponentialRetryPolicyFilter()
let queueService = Azure.createQueueService(
  process.env.AZURE_STORAGE_ACCOUNT,
  process.env.AZURE_STORAGE_ACCESS_KEY,
  process.env.AZURE_STORAGE_CONNECTION_STRING
).withFilter(retryOps)

let handleLetter = inLetter => {
  try {
    let msg = inLetter
    log.info('Got msg %j', msg)
    return true
  }
  catch (err) {
    log.error(err, 'handleMsgError: %s', err.message)
    return false
  }
}

let queueName = 'auth'

let deleteLetter = letter => {
  queueService.deleteMessage(queueName, letter.messageid, letter.popreceipt, (inError, inResp) => {
    if (inError) {
      log.error(inError, 'subDeleteMessageError: %s', inError.message)
    }
  })
}

let pollMessages = () => {
  queueService.getMessages(queueName, (inError, inRes, inResp) => {
    if (inError) {
      log.error(inError, 'queueService::getMessagesError: %s', inError.message)
      return
    }

    console.log('Got %j', inRes)

    _.each(inRes, inLetter => {
      let inMsg = JSON.parse(inLetter.messagetext)
      console.dir(inMsg)
      let res = handleLetter(inMsg.body)
      let outLetter = {
        body: JSON.stringify(res),
        customProperties: {
          msguuid: inMsg.customProperties.msguuid
        }
      }

      let replyto = inMsg.customProperties.replyto
      queueService.createMessage(replyto, JSON.stringify(outLetter), (error, result, response) => {
        if (error) {
          log.error(error, 'createMessageError: %s', error.message)
        }
        deleteLetter(inLetter)
      })
    })

    Promise.delay(1000)
    .done( pollMessages )
  })
}

let createSub = () => {
  queueService.createQueueIfNotExists('auth', (inError, inRes, inResp) => {
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

createSub()
