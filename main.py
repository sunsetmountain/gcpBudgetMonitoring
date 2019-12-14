import base64
import logging
import json
from calendar import monthrange
import datetime
from httplib2 import Http
from json import dumps

def handle_notification(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    logging.info('Budget information: {}'.format(pubsub_message))
    jsonPayload = json.loads(pubsub_message)
    costAmount = jsonPayload['costAmount']
    budgetAmount = jsonPayload['budgetAmount']
    percentOfBudget = round((costAmount/budgetAmount) * 100,2)
    budgetDisplayName = jsonPayload['budgetDisplayName']
    costIntervalStart = jsonPayload['costIntervalStart']
    percentOfMonth = calcMonthPercent(costIntervalStart)
    trendingPercent = round(percentOfBudget - percentOfMonth,2)
    #logging.info('costAmount: {}'.format(costAmount))
    #logging.info('budgetAmount: {}'.format(budgetAmount))
    #logging.info('percentOfBudget: {}'.format(percentOfBudget))
    #logging.info('budgetDisplayName: {}'.format(budgetDisplayName))

    if trendingPercent >= 1:
        message_text = "{}".format(budgetDisplayName) + ": {}".format(trendingPercent) + "% higher than last month (${:.2f}".format(costAmount) + "/${:.2f}".format(budgetAmount) + ")"
    elif trendingPercent < 1 and trendingPercent > -1:
        message_text = "{}".format(budgetDisplayName) + ": On target (+/- 1%) (${:.2f}".format(costAmount) + "/${:.2f}".format(budgetAmount) + ")"
    else:
        message_text = "{}".format(budgetDisplayName) + ": {}".format(trendingPercent) + "% lower than last month (${:.2f}".format(costAmount) + "/${:.2f}".format(budgetAmount) + ")"
    
    logging.info('message_text: {}'.format(message_text))
    timeToSend = chatLimiter(percentOfBudget, percentOfMonth)
    if timeToSend == True:
        sendChatMessage(message_text)
    
    
def calcMonthPercent(costIntervalStart):
    #Convert the interval timestamp to a DateTime object
    intervalStart = datetime.datetime.strptime(costIntervalStart,"%Y-%m-%dT%H:%M:%SZ")
    
    #Get a DateTime object for the date and time right now
    timeNow = datetime.datetime.now()
    
    #Calculate the difference between the start of the billing period and now
    toNowCalc = timeNow - intervalStart
    toNowDifference  = toNowCalc.days * 86400 + toNowCalc.seconds
    #logging.info('toNow: {}'.format(toNowDifference))
    
    #Get a DateTime object for the end of the billing period
    intervalMonth = intervalStart.month
    intervalYear = intervalStart.year
    daysInIntervalMonth = monthrange(intervalYear, intervalMonth)[1]
    intervalEndTimestamp = str(intervalYear) + "-" + str(intervalMonth) + "-" + str(daysInIntervalMonth) + " 23:59:59"
    intervalEndTime = datetime.datetime.strptime(intervalEndTimestamp, "%Y-%m-%d %H:%M:%S")
    
    #Calculate the difference between the start and end of the billing period
    toMonthEndCalc = intervalEndTime - intervalStart
    toMonthEndDifference  = toMonthEndCalc.days * 86400 + toMonthEndCalc.seconds
    #logging.info('toMonthEnd: {}'.format(toMonthEndDifference))
    
    #Calculate position in the billing period expressed as a percent
    intervalPercent = round(toNowDifference/toMonthEndDifference * 100, 2)
    #logging.info('intervalPercent: {}'.format(intervalPercent))
    return intervalPercent

def chatLimiter(budgetPercent, intervalPercent):
    #Get a DateTime object for the date and time right now
    timeNow = datetime.datetime.now()
    logging.info('timeNow: {}'.format(timeNow))
    dayNow = timeNow.day
    hourNow = timeNow.hour
    minuteNow = timeNow.minute
    overUnder = budgetPercent - intervalPercent
    
    if overUnder > 1: #if over budget by more than 1%
        if minuteNow >= 0 and minuteNow < 30: #PubSub notifications should arrive every 20-30 minutes
            return True
        else:
            return False
    else: #if not over budget by more than 1%
        if minuteNow >= 0 and minuteNow < 30 and hourNow == 14: #send notifications for the 7AM Mountain time hour only (offset for GMT)
            return True
        else:
            return False

def sendChatMessage(message_text):
    
    url = 'https://chat.googleapis.com/v1/spaces/...' #insert your Google Chat Webhook URL here
    bot_message = {'text' : '{}'.format(message_text)}

    message_headers = { 'Content-Type': 'application/json; charset=UTF-8'}

    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )
    logging.info('Message sent')
    logging.info('Response: {}'.format(response))    
