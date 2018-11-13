import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class Publication(object):
    def __init__(self, publishedUi, publisher, librarianClient, dsParam, utilities):
        # def __init__(self, openToHireDialogUi, othId, contract, classification, location, numberOfPositions, ftpt,
        #              shift, seasonal, driversPermit, ownTransportation, startDate):
        self.publisher = publisher
        self.userId = 'I-Tech H.R. Portal'
        self.LibrarianClient = librarianClient
        self.dsParam = dsParam
        self.utilities = utilities

        self.recordType = '16110.00'
        self.publishedUi = publishedUi
        self.firstData = dsParam.firstData

        self.publicationId = ''
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.bulletinBoard = ''
        self.newspaper = ''
        self.internetBoard = ''
        self.approvalDate = ''
        self.postingDate = 0
        self.cutoffDate = False
        self.startDate = datetime.strftime(datetime.now() , '%Y-%m-%d')

    def getPublicationFromDialog(self):
        self.publicationId = self.publishedDialogUi.pubIDLabel.text()
        self.othId = self.publishedDialogUi.othPubIDLabel.text()
        self.contract = self.publishedDialogUi.pubContractComboBox.currentText()
        self.classification = self.publishedDialogUi.pubClassificationLineEdit.text()
        self.location = self.publishedDialogUi.pubClassificationLineEdit.text()
        if self.publishedDialogUi.pubBulletinCheckBox.isChecked():
            self.bulletinBoard = 1
        else:
            self.bulletinBoard = 0
        if self.publishedDialogUi.pubNewsCheckBox.isChecked:
            self.newspaper = 1
        else:
            self.newspaper = 0
        if self.publishedDialogUi.pubInternetCheckBox.isChecked:
            self.internetBoard = 1
        else:
            self.internetBoard = 1
        self.approvalDate = self.self.publishedDialogUi.pubApprovalDateEdit.text()
        self.postingDate = self.self.publishedDialogUi.pubPostingDateEdit.text()
        self.cutoffDate = self.self.publishedDialogUi.pubCutoffDateEdit.text


    def getPublicationFromSelection(self, selectedItem):
        self.publicationId = selectedItem[0].text()
        self.othId = selectedItem[1].text()
        self.contract = selectedItem[2].text()
        self.classification = selectedItem[3].text()
        self.location = selectedItem[4].text()
        if self.selectedItem[5].text() == 1:
            self.bulletinBoard = 1
        else:
            self.bulletinBoard = 0
        if selectedItem[6].text() == 1:
            self.newspaper = 1
        else:
            self.newspaper = 0
        if selectedItem[7].text() == 1:
            self.internetBoard = 1
        else:
            self.internetBoard = 1
        self.approvalDate = selectedItem[8].text()
        self.postingDate = selectedItem[9].text()
        self.cutoffDate = selectedItem[10].text()

    def setDialogFromPublication(self):
        self.publishedDialogUi.pubIDLabel.setText(self.publicationId)
        self.publishedDialogUi.othPubIDLabel.setText(self.othId)
        pubContractIndex = self.publishedDialogUi.pubContractComboBox.findText(self.contract)
        self.publishedDialogUi.pubContractComboBox.setIndex(pubContractIndex)
        self.publishedDialogUi.pubClassificationLineEdit.setText(self.classification)
        self.publishedDialogUi.pubClassificationLineEdit.setText(self.location)
        if self.bulletinBoard == 1:
            self.publishedDialogUi.pubBulletinCheckBox.setChecked(True)
        else:
            self.publishedDialogUi.pubBulletinCheckBox.setChecked(False)
        if self.newspaper == 1:
            self.publishedDialogUi.pubNewsCheckBox.setChecked(True)
        else:
            self.publishedDialogUi.pubNewsCheckBox.setChecked(False)
        if self.internetBoard == 1:
            self.publishedDialogUi.pubInternetCheckBox.setChecked(True)
        else:
            self.publishedDialogUi.pubInternetCheckBox.setChecked(False)
        self.self.publishedDialogUi.pubApprovalDateEdit.setText(self.approvalDate)
        self.self.publishedDialogUi.pubPostingDateEdit.setText(self.postingDate)
        self.self.publishedDialogUi.pubCutoffDateEdit.setText(self.cutoffDate)


        '''These methods handle CRUD for this record type'''

    def pubNewPBClicked(self):
        self.publishedUi.pubApprovalDateEdit.setDate(QDate.currentDate())
        self.publishedUi.pubPostingDateEdit.setDate(QDate.currentDate())
        self.publishedUi.pubCutoffDateEdit.setDate(QDate.currentDate())
        result = self.publishedUi.exec()
        if result != 0:
            self.getpubFromDialog()
            self.pubId = str(uuid.uuid4())  # Give it a new ID
            othTuple = ( self.publicationId, self.othId, self.contract, self.classification, self.location,
                         self.bulletinBoard, self.newspaper, self.internetBoard, self.approvalDate, self.postingDate,
                         self.cutoffDate, self.startDate)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', othTuple)

    def pubEditPBClicked(self):
        selectedPUB = self.publishedUi.publishedTableWidget.selectedItems()
        if selectedPUB:
            self.getpubFromSelection(selectedPUB)
            self.setDialogFromPublication()
            result = self.publishedUi.exec()
            if result != 0:
                othTuple = (self.publicationId, self.othId, self.contract, self.classification, self.location,
                            self.bulletinBoard, self.newspaper, self.internetBoard, self.approvalDate, self.postingDate,
                            self.cutoffDate, self.startDate)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                       '00000000-0000-0000-0000-000000000000', self.userId,
                                                       '', '', '', '', '', othTuple)

    def pubDeletePBClicked(self):
        selectedOTH = self.publishedUi.openToHireTableWidget.selectedItems()
        if selectedOTH:
            self.getpubFromSelection(selectedOTH)
            self.setDialogFromPublication()
            result = self.publishedUi.exec()
            if result != 0:
                othTuple = (self.publicationId, self.othId, self.contract, self.classification, self.location,
                            self.bulletinBoard, self.newspaper, self.internetBoard, self.approvalDate, self.postingDate,
                            self.cutoffDate, self.startDate)

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
        self.publishedUi.publishedTableWidget.clearContents()
        self.publishedUi.publishedTableWidget.setRowCount(0)
        dataList = self.getRecords(self.recordType, 'I-Tech H.R. Portal')
        for record in dataList:
            self.publicationId = record[self.firstData]
            self.othId = record[self.firstData + 1]
            self.contract = record[self.firstData + 2]
            self.classification = record[self.firstData + 3]
            self.location = record[self.firstData + 4]
            if self.record[self.firstData + 5] == 1:
                self.bulletinBoard = 1
            else:
                self.bulletinBoard = 0
            if record[self.firstData + 6] == 1:
                self.newspaper = 1
            else:
                self.newspaper = 0
            if record[self.firstData + 7] == 1:
                self.internetBoard = 1
            else:
                self.internetBoard = 1
            self.approvalDate = record[self.firstData + 8]
            self.postingDate = record[self.firstData + 9]
            self.cutoffDate = record[self.firstData + 10]

            rowPosition = self.publishedUi.publishedTableWidget.rowCount()
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.publicationId))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.othId))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.contract))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.classification))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 4, QTableWidgetItem(self.location))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.self.bulletinBoard))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.newspaper))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.internetBoard))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 8, QTableWidgetItem(self.approvalDate))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 9, QTableWidgetItem(self.postingDate))
            self.publishedUi.publishedTableWidget.setItem(rowPosition, 10, QTableWidgetItem(self.cutoffDate))

            # Done - clean up thetable
            self.publishedUi.publishedTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.publishedUi.publishedTableWidget.setColumnWidth(0, 0)    # Hide the ID
           # self.publishedUi.publishedTableWidget.setColumnWidth(0, 0)    # Hide the ID

