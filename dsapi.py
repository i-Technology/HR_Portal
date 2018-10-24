#
# Data Stream API QT Fork
#
# A Programmer's Interface that provides objects needed to implement an I-Tech Data Stream Application
#
# COPYRIGHT 2016 - 2017 I-Technology Inc
# all rights reserved
#
# Notes:
# Qt affects the SubscriberThread in that it will be set up as a QThread and will use Qt's signals and slots
# event notification system as opposed to an inter-task queue. When using Qt the interTaskQueue is to be set
# to None.
# When not using Qt, the interTaskQueue needs to be provided.
# e.g.
# from threading import Thread
# from queue import Queue
#
#  q = Queue(maxsize = 0)
#
# Publish and Subscribe parameters are to be passed in as parameters. These parameters can be set in a global file
# "broker.py" which is imported into the main application
#
# e.g. in broker.py:
#
#     archivePath = '../archivenew.txt'
#     firstData = 13          # Offset past metadata in message
#
#     exchange = 'TRUE_DS_DIRECT'
#     librarianExchange = 'LIBRARIAN_RPC'
#     userName = 'The broker user name'
#     password = 'The broker password'
#
#     brokerIP = 'The IP of the broker. For LANs with a DNS server we can use "itechpi"'
#
# Database identifier. This identifier is prepended to DS messages so that when a subscriber receives
# the message it can determine if it is a message that was sent by an application that modifies the local
# database (archive) that its application is using. If so the data will already be in the database and the Subscriber
# does'nt need to put it in the database (archive).
#
#    myDBID = uuid.uuid4()
#


# import settings as s
# import broker as brkr

import pika
import uuid
import datetime
from enum import Enum
import ast
#from simpleflock import SimpleFlock
import getpass
import sys
import itertools
import operator
from uuid import getnode as get_mac
import yaml
from queue import Queue
import ssl

from threading import Thread

import os
import csv
import json
import time
import threading


import logging

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

#
# Parameters object to be instatiated once and passed as a parameter to DSAPI objects and methods
# 19 Parameters
#

class DS_Parameters(object):
    def __init__(self, exchange, brokerUserName, brokerPassword, brokerIP, dbID, interTaskQueue, routingKeys,
                 publications, deviceId, deviceName, location, applicationId, applicationName, tenant, archivePath,
                 encrypt, firstData, pathToCertificate, pathToKey, qt):
        self.exchange = exchange
        self.brokerUserName = brokerUserName
        self.brokerPassword = brokerPassword
        self.brokerIP = brokerIP
        self.myDBID = dbID
        self.pathToCertificate = pathToCertificate
        self.pathToKey = pathToKey
        self.interTaskQueue = interTaskQueue
        self.archivePath = archivePath
        self.deviceId = deviceId
        self.deviceName = deviceName
        self.location = location
        self.applicationId = applicationId
        self.applicationName = applicationName
        self.tenant = tenant
        self.routingKeys = self.subscriptions = routingKeys
        self.publications = publications
        self.encrypt = encrypt
        self.firstData =firstData
        self.qt = qt


#
# The DS_Metadata Enum provides an index into the fields of metadata that pre-pend a DS record
#

class DS_MetaData(Enum):
    recordType = 0
    action = 1
    recordId = 2
    link = 3
    tenant = 4
    userId = 5
    publishDateTime = 6
    applicationId = 7
    versionLink= 8
    versioned = 9
    userMetadata1 = 10
    userMetadata2 = 11
    userMetadata3 = 12

#
# The Publisher provides a mechanism for publishing data to the Broker.
#

class Publisher(object):
    def __init__(self,dsParam):
        self.exchange = dsParam.exchange
        self.brokerUserName = dsParam.brokerUserName
        self.brokerPassword = dsParam.brokerPassword
        self.brokerIP = dsParam.brokerIP
        self.myDBID = dsParam.myDBID
        self.encrypt = dsParam.encrypt
        self.applicationId = dsParam.applicationId
        self.tenant = dsParam.tenant

    #
    # The publish method formats a message, adding metadata, and sends it to the AMQP broker
    #

    def publish(self, recordType, action, link, userId, umd1, umd2, umd3, umd4, umd5, dataTuple):

        # Set up Broker connection

        if not self.encrypt:
            params = pika.ConnectionParameters(host = self.brokerIP,
                                               port = 5672,
                                               credentials = (pika.PlainCredentials(self.brokerUserName ,self.brokerPassword)),
                                               heartbeat_interval=30,
                                               virtual_host="/",
                                               ssl=False,
                                               socket_timeout=10,
                                               blocked_connection_timeout=10,
                                               retry_delay=3,
                                               connection_attempts=5)
        else:
            context = ssl.create_default_context(cafile="~/certs/ca_certificate.pem")
            context.load_cert_chain("~/certs/client_certificate.pem","~/certs/client_key.pem")
            ssloptions = pika.SSLOptions(context, self.brokerIP)
            params = pika.ConnectionParameters(host = self.brokerIP,
                                               port = 5671,
                                               heartbeat_interval=30,
                                               virtual_host="/",
                                               ssl=True,
                                               ssl_options=ssloptions,
                                               socket_timeout=10,
                                               blocked_connection_timeout=10,
                                               retry_delay=3,
                                               connection_attempts=5)



        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        recordTypeSz = '{:12.2f}'.format(float(recordType)).strip()

        # Prep data here

        recordId = str(uuid.uuid4())        # Give it a new Id
        if link == 0:
            link = '00000000-0000-0000-0000-000000000000'
        if self.tenant == 0:
            self.tenant = '00000000-0000-0000-0000-000000000000'
        if self.applicationId == 0:
            self.applicationId = '00000000-0000-0000-0000-000000000000'

        data = (str(self.myDBID), recordTypeSz, action, recordId, link, self.tenant, userId,
                datetime.datetime.utcnow().isoformat(sep='T'), self.applicationId, umd1, umd2, umd3, umd4, umd5) + dataTuple

        # Publish it

        rk = "{0:12.2f}".format(float(recordType)).strip()
        channel.basic_publish(exchange=self.exchange, routing_key=rk,
                              properties=pika.BasicProperties(content_type="text/plain", delivery_mode=2, ),
                              body=str(data))
        connection.close()

        return str(data)

    #
    # Publish a tuple that already has the metadata
    #

    def rawPublish(self, recordType, dataTuple):

        # Set up Broker connection

        credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
        parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        # Prep data here

        # recordId = str(uuid.uuid4())  # Give it a new Id
        # if link == 0:
        #     link = '00000000-0000-0000-0000-000000000000'
        # if tenant == 0:
        #     tenant = '00000000-0000-0000-0000-000000000000'
        # if applicationId == 0:
        #     applicationId = '00000000-0000-0000-0000-000000000000'

        data = dataTuple

        # print('%s' % (data,))

        # Publish it

        rk = "{0:10.2f}".format(recordType).strip()

        if '.00' in rk:
            rk = "{0:10.1f}".format(recordType).strip()

        channel.basic_publish(exchange=self.exchange, routing_key=rk,
                              properties=pika.BasicProperties(content_type="text/plain", delivery_mode=2, ),
                              body=str(data))
        connection.close()

#
# The AssuredPublisher provides a mechanism for publishing data to the Broker that is assured (Under development).
#

class AssuredPublisher(object):
    def __init__(self, dsParam):

        self.exchange = dsParam.exchange
        self.brokerUserName = dsParam.brokerUserName
        self.brokerPassword = dsParam.brokerPassword
        self.brokerIP = dsParam.brokerIP
        self.myDBID = dsParam.myDBID
        self.encrypt = dsParam.encrypt
        self.applicationId = dsParam.applicationId
        self.tenant = dsParam.tenant


        # Set up Broker connection

        credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
        parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
        self.channel.confirm_delivery(self.delivery_confirmation_callback)
        self.result = self.channel.queue_declare(exclusive=True)
        self.queue_name = self.result.method.queue

    def delivery_confirmation_callback(self, method_frame):

        confirmation_type = method_frame.method.NAME.split('.')[1].lower()

        LOGGER.info('Received %s for delivery tag: %i', confirmation_type, method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)

        LOGGER.info('Published %i messages, %i have yet to be confirmed, %i were acked and %i were nacked',
            self._message_number, len(self._deliveries), self._acked, self._nacked)

    def publish(self, recordType, action, link, userId, applicationId, umd1, umd2, umd3, umd4, umd5,
                dataTuple):


        # Prep data here

        recordId = str(uuid.uuid4())  # Give it a new Id
        if link == 0:
            link = '00000000-0000-0000-0000-000000000000'
        if self.tenant == 0:
            self.tenant = '00000000-0000-0000-0000-000000000000'
        if applicationId == 0:
            applicationId = '00000000-0000-0000-0000-000000000000'

        data = (str(self.myDBID), recordType, action, recordId, link, self.tenant, userId,
                datetime.datetime.utcnow().isoformat(sep='T'), applicationId, umd1, umd2, umd3, umd4,
                umd5) + dataTuple

        # Publish it

        rk = "{0:10.2f}".format(recordType).strip()

        if '.00' in rk:
            rk = "{0:10.1f}".format(recordType).strip()

        self.channel.basic_publish(exchange=self.exchange, routing_key=rk,
                              properties=pika.BasicProperties(content_type="text/plain", delivery_mode=2, ),
                              body=str(data))
        # self.connection.close()


#
# Subscriber Thread - A thread watching for messages from a broker
# copyright 2017 I-Technology Inc.
#
# parameter notes:
#     routingKeys is the list of recordTypes that we need to subscribe to
# Usage:
#     from Source.subscriberThread import subscriberThread
#
#     self.subscriberThread = SubscriberThread()    # Create the thread
#     self.subscriberThread.start()  # Start watching for messages we are subscribing to
#     self.subscriberThread.stop()  # Stop thread when application closes
#
class Gui(object):
    def __init__(self, qt):

        self.qt = qt


    def makeSubscriber(self, dsParam, applicationUser, archiver):

        if self.qt:
            from PyQt5 import QtCore
            from PyQt5.QtCore import Qt, pyqtSignal


            # Make a Qt Subscriber Thread

            class SubscriberThread(QtCore.QThread):

                pubIn = pyqtSignal(str, str)

                def __init__(self, dsParam, applicationUser, archiver):

                    QtCore.QThread.__init__(self)

                    self.exchange = dsParam.exchange
                    self.brokerUserName = dsParam.brokerUserName
                    self.brokerPassword = dsParam.brokerPassword
                    self.brokerIP = dsParam.brokerIP
                    self.myDBID = dsParam.myDBID
                    self.encrypt = dsParam.encrypt
                    self.applicationId = dsParam.applicationId
                    self.applicationName = dsParam.applicationName
                    self.applicationUser = applicationUser
                    self.tenant = dsParam.tenant
                    self.firstData = dsParam.firstData
                    self.archivePath = dsParam.archivePath
                    self.deviceId = dsParam.deviceId
                    self.deviceName = dsParam.deviceName
                    self.location = dsParam.location
                    self.routingKeys = dsParam.routingKeys
                    self.systemRoutingKeys = ['9000010.00']
                    self.archiver = archiver

                    # Get a Publisher

                    self.aPublisher = Publisher(dsParam)

                    # Set up RabbitMQ connection

                    credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
                    parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)
                    self.connection = pika.BlockingConnection(parameters)
                    self.channel = self.connection.channel()
                    self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
                    result = self.channel.queue_declare(exclusive=False, queue=str(self.myDBID))
                    self.queue_name = result.method.queue

                    workingRoutingKeys = self.routingKeys + self.systemRoutingKeys

                    for routingKey in workingRoutingKeys:
                        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=routingKey)

                    LOGGER.info("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)

                def run(self):

                    LOGGER.info("Running Subscriber Thread.")

                    # Monitor RMQ and write incoming data to local archive if we have one

                    # Create the local archive if none and we want one
                    if len(self.archivePath) > 0 and os.path.isfile(self.archivePath) == 0:
                        # if os.path.isfile(self.archivePath) == 0:
                        file = open(self.archivePath, "a", newline='')
                        file.close()

                    print("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)
                    LOGGER.info("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)

                    # Get next request
                    self.channel.basic_consume(self.callback, queue=self.queue_name, no_ack=True)
                    self.channel.start_consuming()
                    LOGGER.error("Should never get here !!!")
                #
                # Close connections to RabbitMQ and stop the thread
                #   Launched when thread stopped in pcbMrp when closeEvent() is fired
                #

                def stop(self):
                    LOGGER.info('Subscriber Thread Terminating')
                    self.channel.stop_consuming()
                    self.channel.queue_delete(queue=self.queue_name)
                    LOGGER.error('Subscriber Thread Terminating1')
                    self.connection.close()
                    LOGGER.error('Subscriber Thread Terminating2')
                    #self.stop()

                #
                # Bind the queue to a record type (routing key)
                #
                def bind(self, recordType):

                    rk = "{0:10.2f}".format(recordType).strip()

                    if '.00' in rk:
                        rk = "{0:10.1f}".format(recordType).strip()

                    self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=rk)

                #
                # Pass the message on to the main application
                #

                def alertMainApp(self, bodyTuple):

                    # Write the Local Archive if we are subscribing to this record type

                    recordType = rt = bodyTuple[0]

                    if rt in self.routingKeys or '#' in self.routingKeys:

                        self.archiver.archive(bodyTuple)

                        # Uncomment this code if we are not archiving through the subscriber

                        # if len(self.archivePath) > 0:       # Put it in the local archive if there is one
                        #     with open(self.archivePath, "a", newline='') as f:
                        #         writer = csv.writer(f, delimiter='\t')
                        #         writer.writerow(bodyTuple)


                        # Alert the main loop

                        recordType = bodyTuple[0]
                        btz = '{0}'.format(bodyTuple)
                        self.pubIn.emit(recordType, btz)
                        LOGGER.info("Emitted Alert for: %s", str(recordType))

                #
                # Execute callback when request received
                #

                def callback(self, ch, method, properties, body):

                    print("DS message is %s", body.decode('utf_8'))
                    LOGGER.info("DS message is %s", body.decode('utf_8'))

                    bodyStr = body.decode('utf_8')
                    i = bodyStr.find(',')  # see who sent this
                    sender = bodyStr[2:i - 1]

                    # j = bodyStr.find(',', i+1)  # see if this was generated by a Versioner (second field 0 = False, 1 = True)
                    # versionSz = bodyStr[i+1:(j)]
                    # aVersion = int(versionSz)

                    LOGGER.info("External DS message received: %s", bodyStr)
                    bodyStr = bodyStr[i + 1:]  # Make this a tuple without dbid
                    # bodyStr = bodyStr[j + 1:]  # Make this a tuple
                    bodyStr = '(' + bodyStr
                    bodyTuple = eval(bodyStr)
                    recordType = bodyTuple[0]
                    LOGGER.info("Record Type: %s", str(recordType))
                    rt = int(float(recordType) * 100)
                    myTime = datetime.datetime.utcnow().isoformat(sep='T')

                    LOGGER.info("bodyTuple[firstData]:%s", str(bodyTuple[self.firstData]))
                    LOGGER.info("rt: %i", rt)

                    if sender != str(self.myDBID):  # Ignore if it was sent by me

                        # Handle System Messages

                        if int(rt) in range(900000000, 910000099):
                            LOGGER.info("IN RANGE")
                            if float(recordType) == 9000010.00:  # Ping Request
                                LOGGER.info("Got Ping Request")
                                if bodyTuple[self.firstData] == '0':  # All ?
                                    LOGGER.info("Ping All Received.")
                                    pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                                  self.applicationName, myTime)


                                    self.aPublisher.publish(9000011.00, 0, '00000000-0000-0000-0000-000000000000',
                                                            self.applicationUser, '', '', '', '', '', pingRecord)
                                elif bodyTuple[self.firstData] == '1':  # Device Ping ?
                                    deviceWanted = bodyTuple[self.firstData+1].replace('\'', '').strip()
                                    LOGGER.info("Device Ping Received:[%s] Me:[%s]", deviceWanted, self.deviceId )
                                    if deviceWanted == self.deviceId:
                                        LOGGER.info("It's for Me!!")
                                        pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                                      self.applicationName, myTime)
                                        self.aPublisher.publish(9000011.00, 0, '00000000-0000-0000-0000-000000000000',
                                                                self.applicationUser, '', '', '', '', '', pingRecord)

                                elif bodyTuple[self.firstData] == '2':  # Application Ping
                                    appWanted = bodyTuple[self.firstData+2].replace('\'', '').strip()
                                    LOGGER.info("Application Ping Received:[%s] Me:[%s]",appWanted,self.applicationId )

                                    if appWanted == self.applicationId:
                                        LOGGER.info("It's for Me!!")
                                        pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                                      self.applicationName, myTime)
                                    self.aPublisher.publish(9000011.00, 0, '00000000-0000-0000-0000-000000000000',
                                                            self.applicationUser, '', '', '', '', '', pingRecord)
                                else:
                                    LOGGER.info('Invalid Ping Type:' + bodyTuple[self.firstData])

                            # Pass unhandled messages to the main thread

                            else:
                                self.alertMainApp(bodyTuple)

                        else:
                            self.alertMainApp(bodyTuple)
                    else:
                        # Here I sent this message
                        self.alertMainApp(bodyTuple)

                    def createSubscriberTask(self):
                        return self.SubscriberThread(self)
        else:
            # Make a normal Subscriber Thread

            class SubscriberThread(Thread):

                def __init__(self, dsParam, applicationUser, archiver):

                    super(SubscriberThread, self).__init__()

                    self.interTaskQueue = dsParam.interTaskQueue
                    self.exchange = dsParam.exchange
                    self.brokerUserName = dsParam.brokerUserName
                    self.brokerPassword = dsParam.brokerPassword
                    self.brokerIP = dsParam.brokerIP
                    self.myDBID = dsParam.myDBID
                    self.encrypt = dsParam.encrypt
                    self.applicationId = dsParam.applicationId
                    self.applicationName = dsParam.applicationName
                    self.applicationUser = applicationUser
                    self.tenant = dsParam.tenant
                    self.firstData = dsParam.firstData
                    self.archivePath = dsParam.archivePath
                    self.deviceId = dsParam.deviceId
                    self.deviceName = dsParam.deviceName
                    self.location = dsParam.location
                    self.routingKeys = dsParam.routingKeys
                    self.systemRoutingKeys = ['9000010.00']
                    self.archiver = archiver

                    self.aPublisher = Publisher(dsParam)

                    # Set up RabbitMQ connection

                    credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
                    parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)
                    self.connection = pika.BlockingConnection(parameters)
                    self.channel = self.connection.channel()
                    self.channel.exchange_declare(exchange=self.exchange, exchange_type="topic", durable=True)
                    result = self.channel.queue_declare(exclusive=False, queue=str(self.myDBID))
                    self.queue_name = result.method.queue

                    workingRoutingKeys = self.routingKeys + self.systemRoutingKeys

                    for routingKey in workingRoutingKeys:
                        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=routingKey)

                    LOGGER.info("Subscriber Thread: Connection established and queue (%s) bound.", self.queue_name)

                def run(self):

                    LOGGER.info("Running Subscriber Thread.")

                    # Monitor RMQ and write incoming data to local archive if we have one

                    # Create the local archive if none and we want one
                    if len(self.archivePath) > 0 and os.path.isfile(self.archivePath) == 0:
                        # if os.path.isfile(self.archivePath) == 0:
                        file = open(self.archivePath, "a", newline='')
                        file.close()

                    print("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)
                    LOGGER.info("Waiting for RabbitMQ DS messages from queue %s", self.queue_name)

                    # Get next request
                    self.channel.basic_consume(self.callback, queue=self.queue_name, no_ack=True)
                    self.channel.start_consuming()
                    LOGGER.error("Should never get here !!!")

                #
                # Close connections to RabbitMQ and stop the thread
                #   Launched when thread stopped in pcbMrp when closeEvent() is fired
                #

                def stop(self):
                    LOGGER.info('Subscriber Thread Terminating')
                    self.channel.stop_consuming()
                    self.channel.queue_delete(queue=self.queue_name)
                    self.connection.close()
                    self.stop()

                #
                # Bind the queue to a record type (routing key)
                #
                def bind(self, recordType):

                    rk = "{0:10.2f}".format(recordType).strip()

                    if '.00' in rk:
                        rk = "{0:10.1f}".format(recordType).strip()

                    self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=rk)

                #
                # Pass the message on to the main application
                #

                def alertMainApp(self, bodyTuple):

                    # Write the Local Archive if we are subscribing to this record type

                    recordType = rt = bodyTuple[0]

                    if rt in self.routingKeys or '#' in self.routingKeys:

                        self.archiver.archive(bodyTuple)

                        # Uncomment code below if not archiving through the subscriber task
                        # if len(self.archivePath) > 0:       # Put it in the local archive if there is one
                        #     with open(self.archivePath, "a", newline='') as f:
                        #         writer = csv.writer(f, delimiter='\t')
                        #         writer.writerow(bodyTuple)

                        # Alert the main loop

                        if self.interTaskQueue != None:
                            self.interTaskQueue.put(bodyTuple)  # Pass it to the main thread
                            LOGGER.info("Queued Alert for: %s", recordType)
                        else:
                            LOGGER.error("No Inter-task data transfer method available to the subscriber task!")

                #
                # Execute callback when request received
                #

                def callback(self, ch, method, properties, body):

                    print("DS message is %s", body.decode('utf_8'))
                    LOGGER.info("DS message is %s", body.decode('utf_8'))

                    bodyStr = body.decode('utf_8')
                    i = bodyStr.find(',')  # see who sent this
                    sender = bodyStr[2:i - 1]

                    # j = bodyStr.find(',', i+1)  # see if this was generated by a Versioner (second field 0 = False, 1 = True)
                    # versionSz = bodyStr[i+1:(j)]
                    # aVersion = int(versionSz)

                    LOGGER.info("External DS message received: %s", bodyStr)
                    bodyStr = bodyStr[i + 1:]  # Make this a tuple without dbid
                    # bodyStr = bodyStr[j + 1:]  # Make this a tuple
                    bodyStr = '(' + bodyStr
                    bodyTuple = eval(bodyStr)
                    recordType = bodyTuple[0]
                    LOGGER.info("Record Type: %s", str(recordType))
                    rt = int(float(recordType) * 100)
                    myTime = datetime.datetime.utcnow().isoformat(sep='T')

                    LOGGER.info("bodyTuple[firstData]:%s", str(bodyTuple[self.firstData]))
                    LOGGER.info("rt: %i", rt)

                    if sender != str(self.myDBID):  # Ignore if it was sent by me

                        # Handle System Messages

                        if int(rt) in range(900000000, 910000099):
                            LOGGER.info("IN RANGE")
                            if float(recordType) == 9000010.00:  # Ping Request
                                LOGGER.info("Got Ping Request")
                                if bodyTuple[self.firstData] == '0':  # All ?
                                    LOGGER.info("Ping All Received.")
                                    pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                                  self.applicationName, myTime)

                                    self.aPublisher.publish(9000011.00, 0, '00000000-0000-0000-0000-000000000000',
                                                            self.applicationUser, '', '', '', '', '', pingRecord)
                                elif bodyTuple[self.firstData] == '1':  # Device Ping ?
                                    deviceWanted = bodyTuple[self.firstData + 1].replace('\'', '').strip()
                                    LOGGER.info("Device Ping Received:[%s] Me:[%s]", deviceWanted, self.deviceId)
                                    if deviceWanted == self.deviceId:
                                        LOGGER.info("It's for Me!!")
                                        pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                                      self.applicationName, myTime)
                                        self.aPublisher.publish(9000011.00, 0, '00000000-0000-0000-0000-000000000000',
                                                                self.applicationUser, '', '', '', '', '', pingRecord)

                                elif bodyTuple[self.firstData] == '2':  # Application Ping
                                    appWanted = bodyTuple[self.firstData + 2].replace('\'', '').strip()
                                    LOGGER.info("Application Ping Received:[%s] Me:[%s]", appWanted, self.applicationId)

                                    if appWanted == self.applicationId:
                                        LOGGER.info("It's for Me!!")
                                        pingRecord = (self.deviceId, self.deviceName, self.location, self.applicationId,
                                                      self.applicationName, myTime)
                                    self.aPublisher.publish(9000011.00, 0, '00000000-0000-0000-0000-000000000000',
                                                            self.applicationUser, '', '', '', '', '', pingRecord)
                                else:
                                    LOGGER.info('Invalid Ping Type:' + bodyTuple[self.firstData])

                            # Pass unhandled messages to the main thread

                            else:
                                self.alertMainApp(bodyTuple)

                        else:
                            self.alertMainApp(bodyTuple)
                    else:
                        # Here I sent this message
                        self.alertMainApp(bodyTuple)

                    def createSubscriberTask(self):
                        return self.SubscriberThread(self)

        return SubscriberThread(dsParam, applicationUser, archiver)

#
# Classes for enabling queries of the Archive through the Librarian
#

class QueryTerm(dict):
    def __init__(self, fieldName, operator, value):
        self.fieldName = fieldName
        self.operator = operator
        self.value = value

class dsQuery(dict):
    def __init__(self, limit, user, tenant, startDate, endDate, queryTerms):
        self.limit = limit
        self.user = user
        self.tenant = str(tenant)
        self.startDate = startDate
        self.endDate = endDate
        self.queryTerms = queryTerms        # A List of QueryTerm

#
# The Librarian Client will send a query to the Librarian through the AMQP broker
#

class LibrarianClient(object):
    # def __init__(self, brokerIP, brokerUserName, brokerPassword, logger):
    def __init__(self, dsParam, logger):

        # self.brokerIP = brokerIP
        # self.brokerUserName = brokerUserName
        # self.brokerPassword = brokerPassword
        # self.logger = logger

        self.brokerIP = dsParam.brokerIP
        self.brokerUserName = dsParam.brokerUserName
        self.brokerPassword = dsParam.brokerPassword
        self.logger = logger

        self.timeout = False

        # credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
        # parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)
        #
        # self.connection = pika.BlockingConnection(parameters)
        # self.channel = self.connection.channel()
        #
        # result = self.channel.queue_declare(exclusive=True)
        # self.callback_queue = result.method.queue
        #
        # self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)


    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, userName, tenant, startDate, endDate, limit, queries):

        credentials = pika.PlainCredentials(self.brokerUserName, self.brokerPassword)
        parameters = pika.ConnectionParameters(self.brokerIP, 5672, '/', credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

        qdList = []
        t = threading.Timer(60, self.timerExpired) # Query timeout
        for q in queries:
            qdList.append(q.__dict__)
        query = dsQuery(limit, userName, tenant, startDate, endDate, qdList)
        queryDict = query.__dict__
        querySz = json.dumps(queryDict, cls = to_json)
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',routing_key='rpc_queue',
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                   correlation_id=self.corr_id ),body=querySz)
        t.start()  # Start the Timer. If it times out the method timerExpired() will execute
        LOGGER.info("Launching a query %s", qdList)
        while self.response is None and self.timeout==False:
            self.connection.process_data_events()
        if self.timeout == True:
            LOGGER.warning('Librarian not responding')
            print(self, 'Librarian not responding')
            t.cancel()
            self.logger.log('dsapi', 0, 0, 0, 'Librarian not responding')
            return None

        # def log(self, userID, errorType, errorLevel, errorAction, errorText):

        t.cancel()
        return ast.literal_eval(self.response.decode("utf_8"))

    #
    # Timer expiry handler
    #
    def timerExpired(self):
        self.timeout = True


#
# Converts a python object to JSON
#

class to_json(json.JSONEncoder):
    def default(self, python_object):
        if isinstance(python_object, time.struct_time):
            return {'__class__': 'time.asctime',
                    '__value__': time.asctime(python_object)}
        if isinstance(python_object, bytes):
            return {'__class__': 'bytes',
                    '__value__': list(python_object)}
        if isinstance(python_object, complex):
            return [python_object.real, python_object.imag]
        return json.JSONEncoder.default(self, python_object)
        # raise TypeError(repr(python_object) + ' is not JSON serializable')

#
# DS_Init allows the loading of parameters from a .yaml file
#

class DS_Init(object):
    def __init__(self, applicationId, applicationName):
        self.applicationId = applicationId
        self.applicationName = applicationName

    def getParams(self, configurationfile, routingKeys, publications):
        LOGGER.info('Reading configuration File')
        with open (configurationfile, 'r') as f:
            condata = yaml.load(f)
        if condata.get('deviceId') == '':
            condata['deviceId'] = ':'.join((itertools.starmap(operator.add, zip(*([iter("%012X" % get_mac())] * 2)))))
        if condata.get('myDBID') == '':
            condata['myDBID'] = str(uuid.uuid4())
        if condata['qt']:
            # Get a queue
            interTaskQueue = None
            # from PyQt5 import QtCore
            # from PyQt5.QtCore import *

        else:
            interTaskQueue = Queue()

        with open(configurationfile, 'w') as yaml_file:
            yaml_file.write( yaml.dump(condata, default_flow_style=False))

        dsParam = DS_Parameters( condata.get('brokerExchange'), condata.get('brokerUser'), condata.get('password'),
                            condata.get('brokerIP'), condata.get('myDBID'), interTaskQueue, routingKeys, publications,
                            condata.get('deviceId'), condata.get('deviceName'), condata.get('location'),
                            self.applicationId, self.applicationName, condata.get('tenant'),
                            condata.get('localArchivePath'), condata.get('encrypt'), condata.get('firstData'),
                            condata.get('pathToCertificate'), condata.get('pathToKey'), condata.get('qt'))

        return (dsParam)


#
# DS_Utility contains helper methods to assist in implementing am I-Tech DataStream application
#

class DS_Utility(object):
    def __init__(self, logger, librarianClient, dsParam, userId, qt):
        self.logger = logger
        self.librarianClient = librarianClient
        self.dsParam = dsParam
        self.userId = userId        # This is the application user
        self.qt = qt

    #
    # Query the Librarian for all records and put them in the local archive.
    #   Used to initialize the local repository on startup or reset
    #

    def refreshArchive(self, loggedInUser, localArchivePath, tenant, subscriptions=None):

        LOGGER.info('Sending query and awaiting a response.')
        if (subscriptions != None and not('#' in subscriptions)):
            with open(localArchivePath, "a", encoding='utf_8', newline='') as localArch:
                # Delete local archive contents
                localArch.seek(0)
                localArch.truncate()
                writer = csv.writer(localArch, delimiter='\t')
                recordCount = 0

                queries = []
                queryTerm = QueryTerm("recordType", "EQ", subscriptions)
                queries.append(queryTerm)
                result = self.librarianClient.call(loggedInUser, tenant, '', '', 0, queries)  # Get all the records
                if result:
                    # Fetch records from the master archive and write them into the local archive
                    for r in result:
                       writer.writerow(r)  # Here at end of record. Write it and clear the string
                       recordCount += 1
            LOGGER.info('Data Transferred from Archive. Wrote %s records.', recordCount)


        else:
            queries = []
            queryTerm = QueryTerm("recordType", "EQ", "*")
            queries.append(queryTerm)
            # query = dsQuery(0, loggedInUser, tenant, '', '', queryTerm)
            result = self.librarianClient.call(loggedInUser, tenant, '', '', 0, queries)  # Get all the records
            LOGGER.info('Result retrieved.')
            if result:
                with open(localArchivePath, "a", encoding='utf_8', newline='') as localArch:

                    # Delete local archive contents
                    localArch.seek(0)
                    localArch.truncate()

                    # Fetch records from the master archive and write them into the local archive
                    writer = csv.writer(localArch, delimiter='\t')
                    recordCount = 0
                    for r in result:
                        writer.writerow(r)  # Here at end of record. Write it and clear the string
                        recordCount += 1

                    LOGGER.info('Data Transferred from Archive. Wrote %s records.', recordCount)


    #
    # update either a list or localarchive. To removed all records that have
    # been updated or delected. will also remove and records that are not yours by comparing
    # the tenent records.

    def updateArchive(self, loggedInUser, tenant, localList=None):

        updateList =[]
        if (localList != None ):
            rdr = localList
        else:
            f = open(self.dsParam.archivePath, encoding='utf_8', newline='')
            rdr = csv.reader(f, delimiter="\t")

        for row in rdr:
            action = row[1]
            recNo = row[2]
            linkNo = row[3]
            tenantNo = row[4]

            if (action == '0' and tenantNo == tenant) :  # Insert New Record
                updateList.append(row)
            elif (tenantNo == tenant):
                found = -1
                index = 0
                for item in updateList:
                    if (item[2] == linkNo and tenantNo == tenant):
                        found = index
                        break
                    index += 1
                if found >= 0:
                    if action == '1':  # Update existing record
                        updateList.pop(index)
                        updateList.append(row)
                    elif action == '2':
                        # Here we are deleting a record
                        updateList.pop(index)
                    else:
                        LOGGER.error('Invalid Record Action. Record action must be 0,1,2')
                else:
                    LOGGER.warning("Updating or Deleting a record not found: ", linkNo)

        if (localList == None ):
            f.close()

        return (updateList)

    #
    # A function to append a new DS RECORD to the local Archive.
    #

    def archive(self, record, pathToArchive):

        # if self.lockURL != '':
        #     with SimpleFlock(self.lockURL):
        #         with open (pathToArchive, "a", newline='') as arch:
        #             writer = csv.writer(arch, delimiter='\t')
        #             writer.writerow(record)
        # else:
        #     with open (pathToArchive, "a", newline='') as arch:
        #         writer = csv.writer(arch, delimiter='\t')
        #         writer.writerow(record)

        with open(pathToArchive, "a", newline='') as arch:
            writer = csv.writer(arch, delimiter='\t')
            writer.writerow(record)

        return

    #
    # Find a record in a table where the data in 'column' matches the 'target'. Return the row # or -1 if no match found.
    #   The table is a QT QTableWidget
    #

    def match(self, table, column, target):
        """
        Find row in table that has a value in column that equals target
        :rtype: int
        """


        # This is QT specific
        #LOGGER.info('Rows: %i',table.rowCount())
        if self.qt:
            from PyQt5 import QtCore
            from PyQt5.QtCore import Qt, pyqtSignal

            index = 0
            for row in range(0,table.rowCount(), 1):
                item = table.item(row, column)
                #LOGGER.info('Item: %s' %item.data(Qt.DisplayRole))
                if item.data(Qt.DisplayRole) == target:
                    # return table.indexFromItem(item).row()
                    return index
                index += 1
            return -1
        else:
            LOGGER.error('match in DS_Utility is for Qt projects only')

    #
    # Find a record in a list where the data in recordId matches the 'target'. Return the row # or -1 if no match found.
    #

    def matchListItem(aList, target):
        """
        Find row in table that has a value in column that equals target
        :rtype: int
        """
        # LOGGER.info('Rows: %i',aList.rowCount())
        index = 0
        for row in aList:
            item = row[2]
            # LOGGER.info('Item: %s' %item.data(Qt.DisplayRole))
            if item == target:
                # return table.indexFromItem(item).row()
                return index
            index += 1
        return -1

    #
    # Returns the day of the year for a date
    #

    def dayOfYear(self, aDate):
        fmt = '%Y-%m-%d'
        dt = datetime.datetime.strptime(aDate, fmt)
        tt = dt.timetuple()
        return tt.tm_yday

    #
    # If the OS (Linux) messes up the date format then fix it:
    # Want yyyy-mm-dd from d/m/yy
    #


    def cleanDate(self, aDateString):
        if len(aDateString) == 10:
            return aDateString
        else:
            slash1 = aDateString.find('/', 0)
            slash2 = aDateString.find('/', slash1+1)
            y = aDateString[slash2+1:]
            d = aDateString[slash1+1:slash2]
            if len(d) == 1:
                d = '0' + d
            m = aDateString[0:slash1]
            if len(m) == 1:
                m = '0' + m
            newDateString = '20' + y + '-' + m + '-' + d
            return newDateString

    #
    # Called when a application starts. Generates and publishes a system message.
    #

    def startApplication(self, aPublisher, userId):

        newUUID = uuid.uuid4()
        myTime = datetime.datetime.utcnow().isoformat(sep='T')

        # g.users.items())[g.currentUser]

        if self.dsParam.archivePath != "":
            self.refreshArchive( userId, self.dsParam.archivePath, self.dsParam.tenant, self.dsParam.routingKeys)

        startRecord = (self.dsParam.deviceId, self.dsParam.deviceName, self.dsParam.location,
                       self.dsParam.applicationId, self.dsParam.applicationName, myTime, self.dsParam.subscriptions,
                       self.dsParam.publications)
        aPublisher.publish(9000000.00, 0, '00000000-0000-0000-0000-000000000000', self.userId,
                           '', '', '', '', '', startRecord)

    #
    # Called when a application stops. Generates and publishes a system message.
    #

    def stopApplication(self, aPublisher):

        newUUID = uuid.uuid4()
        myTime = datetime.datetime.utcnow().isoformat(sep='T')

        stopRecord = (self.dsParam.deviceId, self.dsParam.deviceName, self.dsParam.location,
                       self.dsParam.applicationId, self.dsParam.applicationName, myTime)
        aPublisher.publish(9000001.00, 0, '00000000-0000-0000-0000-000000000000', self.userId,
                           '', '', '', '', '', stopRecord)

#
# If the OS (Linux) messes up the date format then fix it:
# Want yyyy-mm-dd from d/m/yy strings
#


    def cleanDate(self,aDateString):
        if len(aDateString) == 10:
            return aDateString
        else:
            slash1 = aDateString.find('/', 0)
            slash2 = aDateString.find('/', slash1+1)
            y = aDateString[slash2+1:]
            d = aDateString[slash1+1:slash2]
            if len(d) == 1:
                d = '0' + d
            m = aDateString[0:slash1]
            if len(m) == 1:
                m = '0' + m
            newDateString = '20' + y + '-' + m + '-' + d
            return newDateString

#
# A DS_Logger packages an error message in a system message and publishes it. Acts like a regular log message
# but it is subscribed to by a system monitor.
#
# Error Type: Error Code defined by Developer
# Error Level: Error Level for Verbosity filtering
# Error Action code:
# 0 – Display
# 1 – Email Alert Level 1
# 2 – Email Alert Level 2
# 3 – Email Alert Level 3
# 4 – Page Alert
# 5 – Syslog Alert
#

class DS_Logger(object):
    def __init__(self, dsParam):

        self.exchange = dsParam.exchange
        self.brokerUserName = dsParam.brokerUserName
        self.brokerPassword = dsParam.brokerPassword
        self.brokerIP = dsParam.brokerIP
        self.myDBID = dsParam.myDBID
        self.encrypt = dsParam.encrypt
        self.deviceId =  dsParam.deviceId
        self.deviceName =  dsParam.deviceName
        self.location = dsParam.location
        self.applicationId = dsParam.applicationId
        self.applicationName =  dsParam.applicationName
        self.tenant = dsParam.tenant


        self.dsParam = dsParam


    #
    # log method when we don't have a Publisher
    #


    def log(self, userID, errorType, errorLevel, errorAction, errorText):

        logPublisher = Publisher(self.dsParam)
        # logPublisher = Publisher(self.exchange, self.brokerUserName, self.brokerPassword, self.brokerIP, self.dbID)

        logTuple = (self.deviceId, self.deviceName, self.location, self.applicationId, self.applicationName, errorType,
                    errorLevel, errorAction, errorText)

        logPublisher.publish(9000020.00, 0, '00000000-0000-0000-0000-000000000000', userID, '', '', '', '', '', logTuple)

    #
    # log method when we have a Publisher
    #

    def logP(self, logPublisher, userID, errorType, errorLevel, errorAction, errorText):

        logTuple = (self.deviceID, self.deviceName, self.location, self.applicationID, self.applicationName, errorType, errorLevel, errorAction,
                    errorText)

        logPublisher.publish(9000020.00, 0, '00000000-0000-0000-0000-000000000000', userID, '', '', '', '', '', logTuple)

#
# A State Machine allows an application to recognize state
#
# It uses a state table imported from a tab delimited .csv file generated from a spreadsheet
# The state machine moves from an initail state to other states based on the events it receives
# as arguments to the processEvent() method.
#
# The state table is converted fron .csv format by the SMU class method translateTable()
#
# A transitions dictionary maps the alpha transition method name to a defined method in the application.
# See the tstStateMachine.py example.
#

class StateMachine(object):
    # def __init__(self, states, stateTable, eventTable, transitions):
    def __init__(self, states, transitions, stateTable):
        self.states = states
        self.transitions = transitions
        self.stateTable = stateTable
        self.currentState = 0

    #
    # processEvent is called when an event occurs in the main loop or when one is generated by a method
    #

    def processEvent(self, event):

        found = False
        for ev in self.stateTable:      # Find the event entry in the state table
            l1 = ev[0]
            l2 = ev[1]
            print('Event{0} {1}'.format(l1[0][1], l1[1][1]))
            if l1[1][1] == event:
                # Here we found the event. Get the action method
                nextState = l1[self.currentState + 2][1]
                action = l2[self.currentState + 2][1]
                print('Action: ' + action)
                found = True
                break
        if found:
            self.currentState = int(nextState)
            fntocall = self.transitions[action]  # look up the transition function
            fntocall()  # go and execute the required transition logic!
        else:
            print("Sorry, event " + str(event) + " was not found. ")


        # action = self.eventTable[event]
        # print(action)

#
# The state table is converted fron .csv format by the SMU class method translateTable()
#

class SMU(object):
    def __init__(self):
        pass

    #
    # Translate Table From csv
    #
    def translateTable(self, pathToTable):
        rowNo = 0
        states = []
        stateTable = []
        even = True
        with open(pathToTable, "r") as f:
            rdr = csv.reader(f, delimiter="\t")
            for row in rdr:
                # Process a row
                # print('Row #: ' + str(rowNo))
                if rowNo == 0:
                    # Get the States
                    states = list(enumerate(row, start=-2))
                    del states[0:2]

                if rowNo >= 2:
                    # Process an event
                    if even:
                        # event = row[0]
                        # eventDesc = row[1]
                        nextStates = list(enumerate(row, start=-2))
                        even = False
                    else:
                        transitionMethods = list(enumerate(row, start=-2))
                        eventTuple = ( nextStates, transitionMethods)
                        stateTable.append(eventTuple)
                        even = True
                rowNo += 1


        return(states, stateTable)
