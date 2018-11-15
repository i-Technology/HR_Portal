import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class Application(object):
    def __init__(self, applicationUi, publisher, librarianClient, dsParam, utilities):
        # def __init__(self, openToHireDialogUi, othId, contract, classification, location, numberOfPositions, ftpt,
        #              shift, seasonal, driversPermit, ownTransportation, startDate):
        self.publisher = publisher
        self.userId = 'I-Tech H.R. Portal'
        self.LibrarianClient = librarianClient
        self.dsParam = dsParam
        self.utilities = utilities

        self.recordType = '16110.00'
        self.applicationUi = applicationUi
        self.firstData = dsParam.firstData

        self.applicationId = ''
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.dateReceived = ''
        self.applicantFirstName = ''
        self.applicantMiddleName = ''
        self.applicantSurname = ''
        self.currentClassification = ''
        self.criminalRecord = 0

    def getPublicationFromDialog(self):
        self.applicationId = self.applicationUi.applicaionIDLabel.text()
        self.othId = self.applicationUi.appOthLabel.text()
        self.contract = self.applicationUi.appContractComboBox.currentText()
        self.classification = self.applicationUi.appClassificationLineEdit.text()
        self.location = self.applicationUi.appLocationLineEdit.text()
        self.dateReceived = self.applicationUi.appDateReceivedEdit.text()
        self.applicantFirstName = self.applicationUi.appFirstNameLineEdit.text()
        self.applicantMiddleName = self.applicationUi.appMiddleNameLineEdit.text()
        self.applicantSurname = self.applicationUi.appSurnameLineEdit.text()
        self.currentClassification = self.applicationUi.appCurrentClassificationLineEdit.text()

        if self.applicationUi.pubBulletinCheckBox.isChecked():
            self.criminalRecord = 1
        else:
            self.criminalRecord = 0


    def getApplicationFromSelection(self, selectedItem):
        self.applicationId = selectedItem[0].text()
        self.othId = selectedItem[1].text()
        self.contract = selectedItem[2].text()
        self.classification = selectedItem[3].text()
        self.location = selectedItem[4].text()
        self.dateReceived = selectedItem[5].text()
        self.applicantFirstName = selectedItem[6].text()
        self.applicantMiddleName = selectedItem[7].text()
        self.applicantSurname = selectedItem[8].text()
        self.currentClassification = selectedItem[9].text()
        if self.selectedItem[10].text() == '1':
            self.criminalRecord = 1
        else:
            self.criminalRecord = 0

    def setDialogFromApplication(self):
        self.applicationUi.applicaionIDLabel.setText(self.publicationId)
        self.applicationUi.appOthLabel.setText(self.othId)

        appContractIndex = self.applicationUi.appContractComboBox.findText(self.contract)
        self.applicationUi.appContractComboBox.setIndex(appContractIndex)
        self.applicationUi.appClassificationLineEdit.setText(self.classification)
        self.applicationUi.appLocationLineEdit.text(self.location)

        self.applicationUi.appDateReceivedEdit.setText(self.dateReceived)
        self.applicationUi.appFirstNameLineEdit.setText(self.applicantFirstName)
        self.applicationUi.appMiddleNameLineEdit.setText(self.applicantMiddleName)
        self.applicationUi.appSurnameLineEdit.setText(self.applicantSurname)
        self.applicationUi.appCurrentClassificationLineEdit.settext(self.currentClassification)
        if self.criminalRecord == 1:
            self.applicationUi.appCriminalRecordCheckBox.setChecked(True)
        else:
            self.applicationUi.appCriminalRecordCheckBox.setChecked(False)


        '''These methods handle CRUD for this record type'''

    def appNewPBClicked(self):
        self.applicationUi.appDateReceivedEdit.setDate(QDate.currentDate())
        result = self.applicationUi.exec()
        if result != 0:
            self.getApplicationFromDialog()
            self.appID = str(uuid.uuid4())  # Give it a new ID
            othTuple = (self.appID, self.othId, self.contract, self.classification, self.location,
                        self.dateReceived, self.applicantFirstName, self.applicantMiddleName, self.applicantSurname,
                        self.currentClassification, self.criminalRecord,)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', othTuple)

    def appEditPBClicked(self):
        selectedAPP = self.applicationUi.applicationsTableWidget.selectedItems()
        if selectedAPP:
            self.getApplicationFromSelection(selectedAPP)
            self.setDialogFromApplication()
            result = self.applicationUi.exec()
            if result != 0:
                othTuple = (self.appID, self.othId, self.contract, self.classification, self.location,
                            self.dateReceived, self.applicantFirstName, self.applicantMiddleName, self.applicantSurname,
                            self.currentClassification, self.criminalRecord,)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                       '00000000-0000-0000-0000-000000000000', self.userId,
                                                       '', '', '', '', '', othTuple)

    def appDeletePBClicked(self):
        selectedAPP = self.applicationUi.applicationsTableWidget.selectedItems()
        if selectedAPP:
            self.getApplicationFromSelection(selectedAPP)
            self.setDialogFromApplication()
            result = self.applicationUi.exec()
            if result != 0:
                othTuple = (self.appID, self.othId, self.contract, self.classification, self.location,
                            self.dateReceived, self.applicantFirstName, self.applicantMiddleName, self.applicantSurname,
                            self.currentClassification, self.criminalRecord,)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                       '00000000-0000-0000-0000-000000000000', self.userId,
                                                       '', '', '', '', '', othTuple)

    def getRecords(self, recType, userName):
        queries = []
        queryTerm = QueryTerm("recordType", "EQ", recType)
        queries.append(queryTerm)
        queryResults = self.librarianClient.call(userName, self.dsParam.tenant, '', '', 0, queries)  # Get all the records
        queryResult = self.utilities.updateArchive(userName, self.dsParam.tenant, queryResults)
        return queryResult

    def refreshPublishedTable(self):
        self.applicationUi.applicationsTableWidget.clearContents()
        self.applicationUi.applicationsTableWidget.setRowCount(0)
        dataList = self.getRecords(self.recordType, 'I-Tech H.R. Portal')
        for record in dataList:
            self.appID = record[self.firstData]
            self.othId = record[self.firstData + 1]
            self.contract = record[self.firstData + 2]
            self.classification = record[self.firstData + 3]
            self.location = record[self.firstData + 4]
            self.dateReceived = record[self.firstData + 5]
            self.applicantFirstName = record[self.firstData + 6]
            self.applicantMiddleName = record[self.firstData + 7]
            self.applicantSurname = record[self.firstData + 7]
            self.currentClassification = record[self.firstData + 8]
            self.criminalRecord = record[self.firstData + 9]

            rowPosition = self.applicationUi.applicationsTableWidget.rowCount()
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.appID))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.othId))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.contract))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.classification))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 4, QTableWidgetItem(self.location))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.self.dateReceived))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.applicantFirstName))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.applicantMiddleName))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 8, QTableWidgetItem(self.applicantSurname))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 9, QTableWidgetItem(self.currentClassification))
            self.applicationUi.publishedTableWidget.setItem(rowPosition, 10, QTableWidgetItem(self.criminalRecord))

            # Done - clean up thetable
            self.applicationUi.applicationsTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.applicationUi.applicationsTableWidget.setColumnWidth(0, 0)    # Hide the ID
           # self.applicationUi.applicationsTableWidget.setColumnWidth(0, 0)    # Hide the ID

