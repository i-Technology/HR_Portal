'''H.R. Portal
Provides access to H.R. activities and data.'''

# Copyright 2018 I-Technology Inc.
#
# Main code executed at the beginning
#

import sys
import yaml
import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from flatArchiver import  Archiver
import atexit
import time
from ast import literal_eval as makeTuple

from PyQt5 import QtCore
from PyQt5.QtCore import pyqtSlot, QMutexLocker, QMutex
from PyQt5 import uic
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from OpenToHire import OpenToHire
from Publication import Publication


'''
    This microservice [Describe what this programme does]
'''

class HR_Portal(QMainWindow):
    # Object constructor
    def __init__(self, logger, archiver, librarianClient, publisher, dsParam, appParams, utilities):

        self.dsParam = dsParam
        self.appParams = appParams
        self.publisher = publisher

        # uiPath = appParams.uiPath
        # openToHireDialogPath = appParams.openToHireDialogPath
        # publishedDialogPath = appParams.publishedDialofPath

        # User Interfaces

        self.ui = uic.loadUi(appParams.uiPath)
        self.openToHireDialogUi = uic.loadUi(appParams.openToHireDialogPath)
        self.publishedDialogUi = uic.loadUi(appParams.publishedDialogPath)
        self.offerOutcomeDialogUi = uic.loadUi(appParams.offerOutcomeDialogPath)
        self.applicationsDialogUi = uic.loadUi(appParams.applicationsDialogPath)
        self.applicationOutcomeDialogUi = uic.loadUi(appParams.applicationOutcomeDialogPath)
        self.applicantReplyDailogUi = uic.loadUi(appParams.applicantReplyDialogPath)

        # Data Objects openToHireDialogUi, firstData, publisher, librarianClient, dsParam, utilities

        self.oth = OpenToHire(self.openToHireDialogUi, self.publisher, librarianClient, dsParam, utilities)
        self.pub = Publication(self.publishedDialogUi, self.publisher, librarianClient, dsParam, utilities)
        # self.off = self.OfferOutcome(self.offerOutcomeDialogUi, self.publisher, librarianClient, dsParam, utilities)
        # self.app = self.Applications(self.applicationsDialogUi, self.publisher, librarianClient, dsParam, utilities)
        # self.appOutcome = self.ApplicationOutcome(self.applicationOutcomeDialogUi, self.publisher, librarianClient, dsParam, utilities)
        # self.appReply = self.ApplicationReply(self.applicantReplyDailogUi, self.publisher, librarianClient, dsParam, utilities)

        # Map Buttons to Methods

        # Open To Hire Table
        self.ui.newOTHPushButton.clicked.connect(self.oth.othNewPBClicked)
        self.ui.editOTHPushButton.clicked.connect(self.oth.othEditPBClicked)
        self.ui.deleteOTHPushButton.clicked.connect(self.oth.othDeletePBClicked)

        # Published Table
        self.ui.newPubPushButton.clicked.connect(self.pub.pubNewPBClicked)
        self.ui.editPubPushButton.clicked.connect(self.pub.pubEditPBClicked)
        self.ui.deletePubPushButton.clicked.connect(self.pub.pubDeletePBClicked)

        # # Applications Table
        # self.ui.newAppPushButton.clicked.connect(self.app.appNewPBClicked)
        # self.ui.editAppPushButton.clicked.connect(self.app.appEditPBClicked)
        # self.ui.deleteAppPushButton.clicked.connect(self.app.appDeletePBClicked)
        #
        # # Application Outcome Table
        # self.ui.newAppOutcomePushButton.clicked.connect(self.appOutcome.appOutcomeNewPBClicked)
        # self.ui.editAppOutcomePushButton.clicked.connect(self.appOutcome.appOutcomeEditPBClicked)
        # self.ui.deleteAppOutcomePushButton.clicked.connect(self.appOutcome.appOutcomeDeletePBClicked)
        #
        # # Offer Outcome Table
        # self.ui.newOfferOutcomePushButton.clicked.connect(self.off.offerOutcomeNewPBClicked)
        # self.ui.editOfferOutcomePushButton.clicked.connect(self.off.offerOutcomeEditPBClicked)
        # self.ui.deleteOfferOutcomePushButton_3.clicked.connect(self.off.offerOutcomeDeletePBClicked)
        #
        # # Applicant Reply Table
        # self.ui.newApplicantReplyPushButton.clicked.connect(self.appReply.applicantReplyNewPBClicked)
        # self.ui.editApplicantReplyPushButton.clicked.connect(self.appReply.applicantReplyEditPBClicked)
        # self.ui.deleteApplicantReplyPushButton.clicked.connect(self.appReply.applicantReplyDeletePBClicked)

        '''The Gui object uses the qt parameter to determin how messages are passed to this thread from the subscriber 
        thread. If qt is true the subscriber thread is a QThread and passes messages through the pubin. 
        Otherwise we use a Python Thread and an InterTaskQueue which we poll in this thread '''
        
        gui = Gui(dsParam.qt)
        self.subscriber = gui.makeSubscriber(self.dsParam, 'Phoenix', archiver)  # Create a Subscriber Thread

        # self.subscriber = gui.SubscriberThread(self.dsParam, userTuple[5], archiver, dsParam.qt)
        # self.subscriber = SubscriberThread(self.dsParam, applicationUser, archiver, dsParam.qt, gui.SubscriberThreadParent)

        self.subscriber.start()  # Start watching for messages we are subscribing to

        # Here if we are using QT signals and slots
        self.subscriber.pubIn.connect(self.processMessage)    # Using QT Signals & Slots

        # Here if we are polling the interTaskQueue
        # while True:
        #     if self.dsParam.interTaskQueue.empty() == False:
        #         self.processMessage(self.dsParam.interTaskQueue.get())
        #     else:
        #         time.sleep(0.10)    # 1/10 th of a second

        # self.subscriber.pubIn.connect(self.processMessage)    # Using QT Signals & Slots


    # This routine receives all the messages the application is subscribing to, and deals with them
    # @pyqtSlot(str, str)
    def processMessage(self, recordType, message):
        # ToDo Handle the messages
        record = makeTuple(message)
        parsedRecord = str(record[0])
        for index, field in enumerate(record):
            if index >= fd:
                parsedRecord += ", " + str(record[index])
        if recordType == '16100.00':
            self.oth.refreshOpenToHireTable()

    '''
        DS Records
    '''

        # ToDo Carry on




class APP_Parameters(object):
    def __init__(self, uiPath, openToHireDialogPath, publishedDialogPath, offerOutcomeDialogPath,
                 applicationsDialogPath, applicationOutcomeDialogPath, applicantReplyDialogPath):         # Add parameters specific to your application
        self.uiPath = uiPath
        self.openToHireDialogPath = openToHireDialogPath
        self.publishedDialogPath = publishedDialogPath
        self.offerOutcomeDialogPath = offerOutcomeDialogPath
        self.applicationsDialogPath = applicationsDialogPath
        self.applicationOutcomeDialogPath = applicationOutcomeDialogPath
        self.applicantReplyDialogPath = applicantReplyDialogPath


# ------------------------------------------------------------------------------------------------------
''' This executes if the code is run directly and not when an app imports these objects
'''
if __name__ == "__main__":

    applicationId =  'fdfeb13b-a54f-4fd8-8773-f03be5310b77'     # UUID New
    applicationName = 'H.R. Portal  v1.0 v1.0'

    uiPath = 'hrPortal.ui'

    routingKeys = ['16100.00']       # Record Types subscribed to
    publications = ['16100.00']      # Record Types that this application publishes

    # Initialize DS ( Get dsParams from settings.yaml)
    dsInit = DS_Init(applicationId, applicationName)    # Create the object
    dsParam = dsInit.getParams('settings.yaml', routingKeys, publications)  # Get the parameters

    fd = dsParam.firstData  # Offset to application data in DS records (after metadata)

    # Get the application specific parameters

    # LOGGER.info('Reading configuration File')
    with open('settings.yaml', 'r') as f:
        condata = yaml.load(f)
    # yearStartMonth = condata.get('yearStartMonth')
    # payCycle = condata.get('payCycle')
    # seedPayDay = condata.get('seedPayDay')
    openToHireDialogPath = condata.get('openToHireDialog')
    publishedDialogPath = condata.get('publishedDialog')
    offerOutcomeDialogPath = condata.get('offeroutcomeDialog')
    applicationsDialogPath = condata.get('applicationsDialog')
    applicationOutcomeDialogPath = condata.get('publishedDialog')
    applicantReplyDialogPath = condata.get('applicantReplyDialog')



    appParams = APP_Parameters ( uiPath, openToHireDialogPath, publishedDialogPath, offerOutcomeDialogPath,
                                 applicationsDialogPath, applicationOutcomeDialogPath, applicantReplyDialogPath)
    # appParams = APP_Parameters ( dsParam.yearStartMonth'), dsParam.get('payCycle'), dsParam.get('seedPayDay'))

    app = QApplication(sys.argv)

    # Create a DS_Logger object to enable DS Logging which sends log messages to SystemMonitor
    logger = DS_Logger(dsParam)

    # Create a Librarian Client object to enable sending of queries to the Librarian
    librarianClient = LibrarianClient(dsParam, logger)

    # Create a DS_Utility object to allow access to DS specific utility methods
    utilities = DS_Utility( logger, librarianClient, dsParam, 'I-Tech', dsParam.qt)

    # Create a Publisher object to enable publishing DS messages
    aPublisher = Publisher(dsParam)

    # Let's get going

    # Issue a Start System Message
    userId = 'User Id for the system'
    utilities.startApplication(aPublisher, userId)

    # Instantiate a Subscriber Task
    archiver = Archiver(dsParam.archivePath)

    hrPortal = HR_Portal(logger, archiver, librarianClient, aPublisher,dsParam, appParams, utilities)
    utilities.startApplication(aPublisher, userId)
    # hrPortal.refreshTables(hrPortal, dsParam.archivePath)
    hrPortal.ui.show()

    sys.exit(app.exec_())
