import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class OfferOutcome(object):
    def __init__(self, offerOutcomeUi, publisher, librarianClient, dsParam, utilities):
        # def __init__(self, openToHireDialogUi, othId, contract, classification, location, criteriaPassFail,
        #              shift, seasonal, driversPermit, ownTransportation, startDate):
        self.publisher = publisher
        self.userId = 'I-Tech H.R. Portal'
        self.LibrarianClient = librarianClient
        self.dsParam = dsParam
        self.utilities = utilities

        self.recordType = '16100.00'
        self.offerOutcomeUi = offerOutcomeUi
        self.firstData = dsParam.firstData
        self.appId = ''
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.dateOffered = datetime.strftime(datetime.now() , '%Y-%m-%d')
        self.deadline = datetime.strftime(datetime.now() , '%Y-%m-%d')
        self.methodApplicantInformed = ''

    def getofferOutcomefromDialog(self):
        self.appId = self.offerOutcomeUi.offerOutAppLabel.text()
        self.othId = self.offerOutcomeUi.offerOutOthLabel.text()
        self.contract = self.offerOutcomeUi.offerOutcomeContractComboBox.currentText()
        self.classification = self.offerOutcomeUi.appClassificationLineEdit.text()
        self.location = self.offerOutcomeUi.offerOutcomeLocationLineEdit.text()
        self.dateOffered = self.offerOutomeUi.offerOutcomeDateOffereddateEdit.text()
        self.deadline = self.offerOutcomeUi.offerOutcomeDeadlinedateEdit.text()
        self.methodApplicantInformed = self.offerOutcomeUi.offerOutcomeMethodApplicantInformedComboBox.currentText()

    def getofferOutcomefromSelection(self, selectedItem):
        self.appId = selectedItem(0).text()
        self.othId = selectedItem[1].text()
        self.contract = selectedItem[2].text()
        self.classification = selectedItem[3].text()
        self.location = selectedItem[4].text()
        self.dateOffered = selectedItem[5].text()
        self.deadline = selectedItem[6].text()
        self.methodApplicantInformed = selectedItem[7].text()

    def setDialogFromofferOutcome(self):
        self.offerOutcomeUi.outOutAppLabel.setText(self.appId)
        self.offerOutcomeUi.outOutOthLabel.setText(self.othId)
        offerOutcomeContractComboBoxIndex = self.offerOutcomeUi.offerOutcomeContractComboBox.findText(self.contract)
        self.offerOutcomeUi.offerOutcomeContractComboBox.setCurrentIndex(offerOutcomeContractComboBoxIndex)
        self.offerOutcomeUi.offerOutcomeClassificationLineEdit.setText(self.classification)
        self.offerOutcomeUi.offerOutcomeClassificationLineEdit.setText(self.location)
        self.offerOutcomeUi.offerOutcomedateOffered.setText(self.dateOffered)
        self.offerOutcomeUi.offerOutcomedeadline.setText(self.deadline)
        self.offerOutcomeUi.offerOutcomeMethodApplicantInformedComboBox.setText()

        '''These methods handle CRUD for this record type'''

    def offerOutcomeNewPBClicked(self):
        #        self.offerOutcomeUi.offerOutcomeStartDateDateEdit.setDate(QDate.currentDate())
        result = self.offerOutcomeUi.exec()
        if result != 0:
            self.getofferOutcomeDialog()
            self.applicationId = str(uuid.uuid4())  # Give it a new ID
            offerOutcomeTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
            self.dateOffered, self.deadline, self.methodApplicantInformed)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', offerOutcomeTuple)

    def offerOutcomeEditPBClicked(self):
        selectedofferOutcome = self.offerOutcomeUi.offerOutcomeTableWidget.selectedItems()
        if selectedofferOutcome:
            self.getofferOutcomefromSelection(selectedofferOutcome)
            self.setDialogFromofferOutcome()
            result = self.offerOutcomeUi.exec()
            if result != 0:
                offerOutcomeTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                   self.dateOffered, self.deadline, self.methodApplicantInformed)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                       '00000000-0000-0000-0000-000000000000', self.userId,
                                                       '', '', '', '', '', offerOutcomeTuple)

    def offerOutcomeDeletePBClicked(self):
        selectedofferOutcome = self.offerOutcomeUi.offerOutcomeTableWidget.selectedItems()
        if selectedofferOutcome:
            self.getofferOutcomefromSelection(selectedofferOutcome)
            self.setDialogFromofferOutcome()
            result = self.offerOutcomeUi.exec()
            if result != 0:
                offerOutcomeTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                     self.dateOffered, self.deadline, self.methodApplicantInformed)

                dataPublished = self.publisher.publish(self.recordType, 2,'00000000-0000-0000-0000-000000000000',
                                                       self.userId, '', '', '', '', '', offerOutcomeTuple)

    def getRecords(self, recType, userName):
        queries = []
        queryTerm = QueryTerm("recordType", "EQ", recType)
        queries.append(queryTerm)
        queryResults = self.librarianClient.call(userName, self.dsParam.tenant, '', '', 0, queries)  # Get all the records
        queryResult = self.utilities.updateArchive(userName, self.dsParam.tenant, queryResults)
        return queryResult

    def refreshofferOutcomeTable(self):
        self.offerOutcomeUi.offerOutcomeTableWidget.clearContents()
        self.offerOutcomeUi.offerOutcomeTableWidget.setRowCount(0)
        dataList = self.getRecords(self.recordType, 'I-Tech H.R. Portal')
        for record in dataList:
            self.offerOutcomecomeId = record[self.firstData]
            self.othId = record[self.firstData + 1]
            self.contract = record[self.firstData + 2]
            self.classification = record[self.firstData + 3]
            self.location = record[self.firstData + 4]
            self.dateOffered = record[self.firstData + 5]
            self.deadline = record[self.firstData + 6]
            self.methodApplicantInformed = record[self.firstData + 7]
            rowPosition = self.offerOutcomeUi.offerOutcomeTableWidget.rowCount()
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.offerOutcomeId))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.othId))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.contract))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.classification))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 4, QTableWidgetItem(self.location))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.dateOffered))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.deadline))
            self.offerOutcomeUi.offerOutcomeTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.methodApplicantInformed))
            # Done - clean up thetable
            self.offerOutcomeUi.offerOutcomeTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.ui.dwbEmployeesTable.setColumnWidth(0, 0)    # Hide the ID