import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class OpenToHire(object):
    def __init__(self, openToHireDialogUi, publisher, librarianClient, dsParam, utilities):
        # def __init__(self, openToHireDialogUi, othId, contract, classification, location, numberOfPositions, ftpt,
        #              shift, seasonal, driversPermit, ownTransportation, startDate):
        self.publisher = publisher
        self.userId = 'I-Tech H.R. Portal'
        self.LibrarianClient = librarianClient
        self.dsParam = dsParam
        self.utilities = utilities

        self.recordType = '16100.00'
        self.openToHireDialogUi = openToHireDialogUi
        self.firstData = dsParam.firstData
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.numberOfPositions = 0
        self.ftpt = ''
        self.shift = 0
        self.seasonal = False
        self.driversPermit = 0
        self.ownTransportation = 0
        self.startDate = datetime.strftime(datetime.now() , '%Y-%m-%d')

    def getOpenToHirefromDialog(self):
        self.othId = self.openToHireDialogUi.othIdLabel.text()
        self.contract = self.openToHireDialogUi.othContractComboBox.currentText()
        self.classification = self.openToHireDialogUi.othClassificationLineEdit.text()
        self.location = self.openToHireDialogUi.othLocationLineEdit.text()
        self.numberOfPositions = self.openToHireDialogUi.othQtyLineEdit.text()
        self.ftpt = self.openToHireDialogUi.othFTPTComboBox.currentText()
        self.shift = self.openToHireDialogUi.othShiftSpinBox.value()
        if self.openToHireDialogUi.othSeasonalCheckBox.isChecked():
            self.seasonal = 1  # True
        else:
            self.seasonal = 0  # False
        if self.openToHireDialogUi.othDriversPermitCheckBox.isChecked():
            self.driversPermit = 1  # True
        else:
            self.driversPermit = 0  # False
        if self.openToHireDialogUi.othOwnTransportationCheckBox.isChecked():
            self.ownTransportation = 1  # True
        else:
            self.ownTransportation = 0  # False
        self.startDate = self.openToHireDialogUi.othStartDateDateEdit.text()

    def getOpenToHirefromSelection(self, selectedItem):
        self.othId = selectedItem[0].text()
        self.contract = selectedItem[1].text()
        self.classification = selectedItem[2].text()
        self.location = selectedItem[3].text()
        self.numberOfPositions = int(selectedItem[4].text())
        self.ftpt = selectedItem[5].text()
        self.shift = int(selectedItem[6].text())
        if selectedItem[7].text() == '1':
            self.seasonal = '1'  # True
        else:
            self.seasonal = 0  # False
        if selectedItem[8].text() == '1':
            self.driversPermit = 1  # True
        else:
            self.driversPermit = 0  # False
        if selectedItem[9].text() == '1':
            self.ownTransportation = 1  # True
        else:
            self.ownTransportation = 0  # False
        self.startDate = selectedItem[10].text()

    def setDialogFromOpenToHire(self):
        self.openToHireDialogUi.othIdLabel.setText(self.othId)
        othContractComboBoxIndex = self.openToHireDialogUi.othContractComboBox.findText(self.contract)
        self.openToHireDialogUi.othContractComboBox.setCurrentIndex(othContractComboBoxIndex)
        self.openToHireDialogUi.othClassificationLineEdit.setText(self.classification)
        self.openToHireDialogUi.othClassificationLineEdit.setText(self.location)
        self.openToHireDialogUi.othQtyLineEdit.setText(str(self.numberOfPositions))
        othFTPTIndex = self.openToHireDialogUi.othFTPTComboBox.findText(self.ftpt)
        self.openToHireDialogUi.othFTPTComboBox.setIndex(othFTPTIndex)
        self.openToHireDialogUi.othShiftSpinBox.setValue(self.shift)
        if self.seasonal == 1:
            self.openToHireDialogUi.othSeasonalCheckBox.setChecked(True)
        else:
            self.openToHireDialogUi.othSeasonalCheckBox.setChecked(False)

        if self.driversPermit == 1:
            self.openToHireDialogUi.othDriversPermitCheckBox.setChecked(True)
        else:
            self.openToHireDialogUi.othDriversPermitCheckBox.setChecked(False)
        if self.ownTransportation == 1:
            self.openToHireDialogUi.othOwnTransportationCheckBox.setChecked(True)
        else:
            self.openToHireDialogUi.othOwnTransportationCheckBox.setChecked(False)
        self.openToHireDialogUi.othStartDateDateEdit.setText(self.startDate)

        '''These methods handle CRUD for this record type'''

    def othNewPBClicked(self):
        self.openToHireDialogUi.othStartDateDateEdit.setDate(QDate.currentDate())
        result = self.openToHireDialogUi.exec()
        if result != 0:
            self.getOpenToHirefromDialog()
            self.othId = str(uuid.uuid4())  # Give it a new ID
            othTuple = (self.othId, self.contract, self.classification, self.location,
                        self.numberOfPositions, self.ftpt, self.shift, self.seasonal, self.driversPermit,
                        self.ownTransportation, self.startDate)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', othTuple)

    def othEditPBClicked(self):
        selectedOTH = self.openToHireDialogUi.openToHireTableWidget.selectedItems()
        if selectedOTH:
            self.getOpenToHirefromSelection(selectedOTH)
            self.setDialogFromOpenToHire()
            result = self.openToHireDialogUi.exec()
            if result != 0:
                othTuple = (self.othId, self.contract, self.classification, self.location,
                            self.numberOfPositions, self.ftpt, self.shift, self.seasonal,
                            self.driversPermit,
                            self.ownTransportation, self.startDate)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                       '00000000-0000-0000-0000-000000000000', self.userId,
                                                       '', '', '', '', '', othTuple)

    def othDeletePBClicked(self):
        selectedOTH = self.openToHireDialogUi.openToHireTableWidget.selectedItems()
        if selectedOTH:
            self.getOpenToHirefromSelection(selectedOTH)
            self.setDialogFromOpenToHire()
            result = self.openToHireDialogUi.exec()
            if result != 0:
                othTuple = (self.othId, self.contract, self.classification, self.location,
                            self.numberOfPositions, self.ftpt, self.shift, self.seasonal,
                            self.driversPermit,
                            self.ownTransportation, self.startDate)

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

    def refreshOpenToHireTable(self):
        self.openToHireDialogUi.openToHireTableWidget.clearContents()
        self.openToHireDialogUi.openToHireTableWidget.setRowCount(0)
        dataList = self.getRecords(self.recordType, 'I-Tech H.R. Portal')
        for record in dataList:
            self.othId = record[self.firstData]
            self.contract = record[self.firstData + 1]
            self.classification = record[self.firstData + 2]
            self.location = record[self.firstData + 3]
            self.numberOfPositions = record[self.firstData + 4]
            self.ftpt = record[self.firstData + 5]
            self.shift = record[self.firstData + 6]
            self.seasonal = record[self.firstData + 7]
            self.driversPermit = record[self.firstData + 8]
            self.ownTransportation = record[self.firstData + 9]
            self.startDate = record[self.firstData + 10]
            rowPosition = self.openToHireDialogUi.openToHireTableWidget.rowCount()
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.othId))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.contract))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.classification))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.location))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 4,
                                                                  QTableWidgetItem(self.numberOfPositions))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.ftpt))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.shift))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.seasonal))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 8, QTableWidgetItem(self.driversPermit))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 9,
                                                                  QTableWidgetItem(self.ownTransportation))
            self.openToHireDialogUi.openToHireTableWidget.setItem(rowPosition, 10, QTableWidgetItem(self.startDate))
            # Done - clean up thetable
            self.openToHireDialogUi.openToHireTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.ui.dwbEmployeesTable.setColumnWidth(0, 0)    # Hide the ID

