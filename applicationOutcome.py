import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class applicationOutcome(object):
    def __init__(self, applicationOutcomeUi, publisher, librarianClient, dsParam, utilities):
        # def __init__(self, openToHireDialogUi, othId, contract, classification, location, criteriaPassFail,
        #              shift, seasonal, driversPermit, ownTransportation, startDate):
        self.publisher = publisher
        self.userId = 'I-Tech H.R. Portal'
        self.LibrarianClient = librarianClient
        self.dsParam = dsParam
        self.utilities = utilities

        self.recordType = '16100.00'
        self.applicationOutcomeUi = applicationOutcomeUi
        self.firstData = dsParam.firstData
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.criteriaPassFail = ''
        self.reasonforFailure = ''
        self.seasonal = False
        self.driversPermit = 0
        self.ownTransportation = 0
        self.startDate = datetime.strftime(datetime.now() , '%Y-%m-%d')

def getapplicationOutcomefromDialog(self):
    self.othId = self.applicationOutcomeUi.othIdLabel.text()
    self.contract = self.applicationOutcomeUi.othContractComboBox.currentText()
    self.classification = self.applicationOutcomeUi.othClassificationLineEdit.text()
    self.location = self.applicationOutcomeUi.othLocationLineEdit.text()
    if self.criteriaPassFail = self.appOutcomeCriteriaPassButton.QRadioButton.isDown()
        self.criteriaPassFail = 1 # True
    else:
        self.criteriaPassFail = 0 # False
    self.reasonforFailure = self.applicationOutomeUi.appOutcomeReasonForFailureLineEdit.text()
    if self.applicationOutcomeUi.othSeasonalCheckBox.isChecked():
        self.seasonal = 1  # True
    else:
        self.seasonal = 0  # False
    if self.applicationOutcomeUi.othDriversPermitCheckBox.isChecked():
        self.driversPermit = 1  # True
    else:
        self.driversPermit = 0  # False
    if self.applicationOutcomeUi.othOwnTransportationCheckBox.isChecked():
        self.ownTransportation = 1  # True
    else:
        self.ownTransportation = 0  # False
    self.startDate = self.applicationOutcomeUi.othStartDateDateEdit.text()

    def getapplicationOutcomefromSelection(self, selectedItem):
        self.othId = selectedItem[0].text()
        self.contract = selectedItem[1].text()
        self.classification = selectedItem[2].text()
        self.location = selectedItem[3].text()
        self.criteriaPassFail = selectedItem[4].text()
        self.ReasonForFailure = int(selectedItem[5].text())
        if selectedItem[6].text() == '1':
            self.seasonal = '1'  # True
        else:
            self.seasonal = 0  # False
        if selectedItem[7].text() == '1':
            self.driversPermit = 1  # True
        else:
            self.driversPermit = 0  # False
        if selectedItem[8].text() == '1':
            self.ownTransportation = 1  # True
        else:
            self.ownTransportation = 0  # False
        self.startDate = selectedItem[9].text()

        def setDialogFromapplicationOutcome(self):
            self.applicationOutcomeUi.othIdLabel.setText(self.othId)
        applicationOutcomeContractComboBoxIndex = self.applicationOutcomeUi.applicationOutcomeContractComboBox.findText(self.contract)
        self.applicationOutcomeUi.appOutcomeContractComboBox.setCurrentIndex(applicationOutcomeContractComboBoxIndex)
        self.applicationOutcomegUi.appOutcomeClassificationLineEdit.setText(self.classification)
        self.applicationOutcomeUi.appOutcomeClassificationLineEdit.setText(self.location)
        self.applicationOutcomeUi.applicationOutcomeRadioButton.setValue(self.criteriaPassFail)
        if self.applicationOutcomeRadioButton == 1:
            self.applicationOutcomeUi.appOutcomeCriteriaPassButton.down(True)
        else:
            self.applicationOutcomeUi.appOutcomeCriteriaPassButton.down(False)
        self.applicationOutcomeUi.othClassificationLineEdit.setText(self.ReasonForFailure)
        self.MyersBriggs = self.applicationOutcomeDialogUi.appOutcomeMyerBriggsComboBox_2.currentText()
        self.applicationOutcomeUi.appOutcomewhoMadeDeterminationLineEdit_3.text()
        self.applicationOutcomeUi.appOutcomeRadioButton.setValue(self.JobOfferedYesNo)
        if self.applicationOutcomeRadioButton == 1:
            self.applicationOutcomeUi.appOutcomeJobOfferedYesRadioButton.down(True)
        else:
            self.applicationOutcomeUi.appOutcomeJobOfferedYesRadioButton.down(False)

        '''These methods handle CRUD for this record type'''

    def applicationOutcomeNewPBClicked(self):
        self.applicationOutcomeUi.appOutcomeStartDateDateEdit.setDate(QDate.currentDate())
        result = self.applicationOutcomeUi.exec()
        if result != 0:
            self.getapplicationOutcomeDialog()
            self.applicationOutcomeId = str(uuid.uuid4())  # Give it a new ID
            appOutcomeTuple = (self.appPutcomeId, self.contract, self.classification, self.location,
                        self.criteriaPassFail, self.ReasonForFailure, self.MyersBriggs, self.WhoMadeDetermination,
                        self.JobOfferedYesNo)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', othTuple)

    def applicationOutcomeEditPBClicked(self):
        selectedappOutcome = self.applicationOutcomeUi.applicationOutcomeTableWidget.selectedItems()
        if selectedappOutcome:
            self.getapplicationOutcomefromSelection(selectedappOutcome)
            self.setDialogFromapplicationOutcome()
            result = self.applicationOutcomeUi.exec()
            if result != 0:
                appOutcomeTuple = (self.appPutcomeId, self.contract, self.classification, self.location,
                                   self.criteriaPassFail, self.ReasonForFailure, self.MyersBriggs, self.WhoMadeDetermination,
                                   self.JobOfferedYesNo)

                 dataPublished = self.publisher.publish(self.recordType, 1,
                                                   '00000000-0000-0000-0000-000000000000', self.userId,
                                                   '', '', '', '', '', othTuple)

    def applicationOutcomeDeletePBClicked(self):
        selectedappOutcome = self.applicationOutcomeUi.applicationOutcomeTableWidget.selectedItems()
        if selectedappOutcome:
            self.getapplicationOutcomefromSelection(selectedappOutcome)
            self.setDialogFromapplicationOutcome()
            result = self.applicationOutcomeUi.exec()
            if result != 0:
                appOutcomeTuple = (self.appPutcomeId, self.contract, self.classification, self.location,
                                   self.criteriaPassFail, self.ReasonForFailure, self.MyersBriggs, self.WhoMadeDetermination,
                                   self.JobOfferedYesNo)

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

    def refreshapplicationOutcomeTable(self):
        self.applicationOutcomeUi.applicationOutcomeTableWidget.clearContents()
        self.applicationOutcomeUi.aplicationOutcomeTableWidget.setRowCount(0)
        dataList = self.getRecords(self.recordType, 'I-Tech H.R. Portal')
        for record in dataList:
            self.appOutcomeId = record[self.firstData]
            self.contract = record[self.firstData + 1]
            self.classification = record[self.firstData + 2]
            self.location = record[self.firstData + 3]
            self.criteriaPassFail = record[self.firstData + 4]
            self.ReasonForFailure = record[self.firstData + 5]
            self.MyersBriggs = record[self.firstData + 6]
            self.WhoMadeDetermination = record[self.firstData + 7]
            self.JobOfferedYesNo = record[self.firstData + 8]
            rowPosition = self.applicationOutcomeUi.applicationOutcomeTableWidget.rowCount()
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.othId))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.contract))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.classification))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.location))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 4, QTableWidgetItem(self.criteriaPassFail))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.ReasonForFailure))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.MyersBriggs))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.WhoMadeDetermination))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 8, QTableWidgetItem(self.JobOfferedYesNo))
            # Done - clean up thetable
            self.applicationOutcomeUi.applicationOutcomeTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.ui.dwbEmployeesTable.setColumnWidth(0, 0)    # Hide the ID

