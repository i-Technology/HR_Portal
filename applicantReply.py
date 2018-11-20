import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class ApplicantReply(object):
    def __init__(self, applicantReplyUi, publisher, librarianClient, dsParam, utilities):
        # def __init__(self, openToHireDialogUi, othId, contract, classification, location, criteriaPassFail,
        #              shift, seasonal, driversPermit, ownTransportation, startDate):
        self.publisher = publisher
        self.userId = 'I-Tech H.R. Portal'
        self.LibrarianClient = librarianClient
        self.dsParam = dsParam
        self.utilities = utilities

        self.recordType = '16100.00'
        self.applicantReplyUi = applicantReplyUi
        self.firstData = dsParam.firstData
        self.appId = ''
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.HeardBackFrom = ''
        self.dateHeardBackFrom = datetime.strftime(datetime.now() , '%Y-%m-%d')
        self.applicantAccepted = ''

    def getApplicantReplyfromDialog(self):
        self.appId = self.applicantReplyUi.appReplyOutAppLabel.text()
        self.othId = self.applicantReplyUi.appReplyOutOthLabel.text()
        self.contract = self.applicantReplyUi.applicantReplyContractComboBox.currentText()
        self.classification = self.applicantReplyUi.appClassificationLineEdit.text()
        self.location = self.applicantReplyUi.applicantReplyLocationLineEdit.text()
        if self.heardBackFrom == self.applicantReplyUi.applicantReplyHeardBackYesradioButton.isDown():
            self.heardBackFrom = 1 # True
        else:
            self.heardBackFrom = 0 # False
        self.dateHeardBackFrom = self.applicantReplyUi.offerOutcomeDeadlinedateEdit.text()
        self.methodApplicantInformed = self.applicantReplyUi.offerOutcomeMethodApplicantInformedComboBox.currentText()
        if self.applicantAccepted == self.applicantReplyUi.applicantReplyApplicantAcceptedYesradioButton.isDown():
            self.applicantAccepted = 1 # True
        else:
            self.applicantAccepted = 0 # False

    def getapplicantReplyfromSelection(self, selectedItem):
        self.appId = selectedItem(0).text()
        self.othId = selectedItem[1].text()
        self.contract = selectedItem[2].text()
        self.classification = selectedItem[3].text()
        self.location = selectedItem[4].text()
        if selectedItem[5].text() == '1':
          self.heardBackFrom = '1'  # True
        else:
           self.heardBackFrom = 0  # False
        self.dateHeardBackFrom = selectedItem[6].text()
        if selectedItem[7].text() == '1':
            self.dateHeardBackFrom = '1'  # True
        else:
            self.dateHeardBackFrom = 0  # False


    def setDialogFromapplicantReply(self):
        self.applicantReplyUi.outOutAppLabel.setText(self.appId)
        self.applicantReplyUi.outOutOthLabel.setText(self.othId)
        applicantReplyContractComboBoxIndex = self.applicantReplyUi.applicantReplyContractComboBox.findText(self.contract)
        self.applicantReplyUi.applicantReplyContractComboBox.setCurrentIndex(applicantReplyContractComboBoxIndex)
        self.applicantReplyUi.applicantReplyClassificationLineEdit.setText(self.classification)
        self.applicantReplyUi.applicantReplyeClassificationLineEdit.setText(self.location)
        if self.heardBackFrom == 1:
            self.applicationOutcomeUi.applicantReplyHeardBackYesRadioButton.down(True)
        else:
            self.applicationOutcomeUi.applicantReplyHeardBackYesRadioButton.down(False)
        self.applicantReplyUi.applicantReplydateHeardBackFrom.setText(self.dateHeardBackFrom)
        if self.applicantAccepted == 1:
            self.applicantReplyUi.applicantReplyApplicantAcceptedYesradioButton.down(True)
        else:
            self.applicantReplyUi.applicantReplyApplicantAcceptedNoradioButton.down(False)

        '''These methods handle CRUD for this record type'''

    def applicantReplyNewPBClicked(self):
        #        self.applicantReplyUi.applicantReplyStartDateDateEdit.setDate(QDate.currentDate())
        result = self.applicantReplyUi.exec()
        if result != 0:
            self.getApplicantReplyfromDialog()
            self.applicationId = str(uuid.uuid4())  # Give it a new ID
            applicantReplyTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                 self.heardBackFrom, self.dateHeardBackFrom, self.applicantAccepted)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', applicantReplyTuple)

    def applicantReplyEditPBClicked(self):
        selectedapplicantReply = self.applicantReplyUi.applicantReplyTableWidget.selectedItems()
        if selectedapplicantReply:
            self.getapplicantReplyfromSelection(selectedapplicantReply)
            self.setDialogFromapplicantReply()
            result = self.applicantReplyUi.exec()
            if result != 0:
                applicantReplyTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                     self.heardBackFrom, self.dateHeardBackFrom, self.applicantAccepted)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                       '00000000-0000-0000-0000-000000000000', self.userId,
                                                       '', '', '', '', '', applicantReplyTuple)

    def applicantReplyDeletePBClicked(self):
        selectedapplicantReply = self.applicantReplyUi.applicantReplyTableWidget.selectedItems()
        if selectedapplicantReply:
            self.getapplicantReplyfromSelection(selectedapplicantReply)
            self.setDialogFromapplicantReply()
            result = self.applicantReplyUi.exec()
            if result != 0:
                applicantReplyTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                     self.heardBackFrom, self.dateHeardBackFrom, self.applicantAccepted)

                dataPublished = self.publisher.publish(self.recordType, 2,'00000000-0000-0000-0000-000000000000',
                                                       self.userId, '', '', '', '', '', applicantReplyTuple)

    def getRecords(self, recType, userName):
        queries = []
        queryTerm = QueryTerm("recordType", "EQ", recType)
        queries.append(queryTerm)
        queryResults = self.librarianClient.call(userName, self.dsParam.tenant, '', '', 0, queries)  # Get all the records
        queryResult = self.utilities.updateArchive(userName, self.dsParam.tenant, queryResults)
        return queryResult

    def refreshapplicantReplyTable(self):
        self.applicantReplyUi.applicantReplyTableWidget.clearContents()
        self.applicantReplyUi.applicantReplyTableWidget.setRowCount(0)
        dataList = self.getRecords(self.recordType, 'I-Tech H.R. Portal')
        for record in dataList:
            self.applicantReplyeId = record[self.firstData]
            self.othId = record[self.firstData + 1]
            self.contract = record[self.firstData + 2]
            self.classification = record[self.firstData + 3]
            self.location = record[self.firstData + 4]
            self.dateOffered = record[self.firstData + 5]
            self.deadline = record[self.firstData + 6]
            self.methodApplicantInformed = record[self.firstData + 7]
            rowPosition = self.offerOutcomeUi.offerOutcomeTableWidget.rowCount()
            self.applicantReplyeUi.applicantReplyTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.applicantReplyId))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.othId))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.contract))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.classification))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 4, QTableWidgetItem(self.location))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.heardBackFrom))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.dateHeardBackFrom))
            self.applicantReplyUi.applicantReplyTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.applicantAccepted))
            # Done - clean up thetable
            self.applicantReplyUi.applicantReplyTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.ui.dwbEmployeesTable.setColumnWidth(0, 0)    # Hide the ID