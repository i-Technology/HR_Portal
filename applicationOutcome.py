import uuid
from dsapi import Publisher, Gui, DS_Logger, DS_Utility, DS_Parameters, LibrarianClient, DS_Init, QueryTerm, dsQuery
from PyQt5.QtWidgets import QMainWindow, QTableWidgetItem, QApplication, QDialogButtonBox
from PyQt5.QtCore import QDate
from datetime import datetime


class ApplicationOutcome(object):
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
        self.appId = ''
        self.othId = ''
        self.contract = ''
        self.classification = ''
        self.location = ''
        self.criteriaPassFail = ''
        self.reasonforFailure = ''
        self.myersBriggs = ''
        self.whoMadeDetermination = ''
        self.jobOffered = 0

    def getapplicationOutcomefromDialog(self):
        self.appId = self.applicationOutcomeUi.appOutAppLabel.text()
        self.othId = self.applicationOutcomeUi.appOutOthLabel.text()
        self.contract = self.applicationOutcomeUi.othContractComboBox.currentText()
        self.classification = self.applicationOutcomeUi.appClassificationLineEdit.text()
        self.location = self.applicationOutcomeUi.appLocationLineEdit.text()
        if self.criteriaPassFail == self.applicationOutcomeUi.appOutcomeCriteriaPassButton.isDown():
            self.criteriaPassFail = 1 # True
        else:
            self.criteriaPassFail = 0 # False
        self.reasonforFailure = self.applicationOutomeUi.appOutcomeReasonForFailureLineEdit.text()
        self.myersBriggs = self.applicationOutcomeUi.appOutcomeMyersBriggsComboBox.currentText()
        self.whoMadeDetermination = self.applicationOutcomeUi.appOutcomeWhoMadeDeterminationLineEdit.text()
        if self.jobOffered == self.applicationOutcomeUi.appOutcomeJobOfferedYesradioButton.isDown():
            self.jobOffered = 1 # True
        else:
            self.jobOffered = 0 # False

    def getapplicationOutcomefromSelection(self, selectedItem):
        self.applicationId = selectedItem(0).text()
        self.othId = selectedItem[1].text()
        self.contract = selectedItem[2].text()
        self.classification = selectedItem[3].text()
        self.location = selectedItem[4].text()
        if selectedItem[5].text() == '1':
            self.criteriaPassFail = '1'  # True
        else:
            self.criteriaPassFail = 0  # False
        self.ReasonForFailure = selectedItem[6].text()
        self.myersBriggs = selectedItem[7].text()
        self.whoMadeDetermination = selectedItem[8].text()
        if selectedItem[8].text() == '1':
            self.jobOffered = 1  # True
        else:
            self.jobOffered = 0  # False


    def setDialogFromapplicationOutcome(self):
        self.applicationOutcomeUi.appOutAppLabel.setText(self.applicationId)
        self.applicationOutcomeUi.appOutOthLabel.setText(self.othId)
        applicationOutcomeContractComboBoxIndex = self.applicationOutcomeUi.applicationOutcomeContractComboBox.findText(self.contract)
        self.applicationOutcomeUi.appOutcomeContractComboBox.setCurrentIndex(applicationOutcomeContractComboBoxIndex)
        self.applicationOutcomeUi.appOutcomeClassificationLineEdit.setText(self.classification)
        self.applicationOutcomeUi.appOutcomeClassificationLineEdit.setText(self.location)
        if self.criteriaPassFail == 1:
            self.applicationOutcomeUi.appOutcomeCriteriaPassButton.down(True)
        else:
            self.applicationOutcomeUi.appOutcomeCriteriaPassButton.down(False)
        self.applicationOutcomeUi.appOutcomeReasonForFailureLineEdit.setText(self.ReasonForFailure)
        self.MyersBriggs = self.applicationOutcomeDialogUi.appOutcomeMyersBriggsComboBox.setCurrentText()
        self.applicationOutcomeUi.appOutcomeWhoMadeDeterminationLineEdit.setText()
        if self.JobOfferedYesNo == 1:
            self.applicationOutcomeUi.appOutcomeJobOfferedYesRadioButton.down(True)
        else:
            self.applicationOutcomeUi.appOutcomeJobOfferedYesRadioButton.down(False)

        '''These methods handle CRUD for this record type'''

    def appOutcomeNewPBClicked(self):
#        self.applicationOutcomeUi.appOutcomeStartDateDateEdit.setDate(QDate.currentDate())
        result = self.applicationOutcomeUi.exec()
        if result != 0:
            self.getapplicationOutcomeDialog()
            self.applicationId = str(uuid.uuid4())  # Give it a new ID
            appOutcomeTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                        self.criteriaPassFail, self.ReasonForFailure, self.MyersBriggs, self.WhoMadeDetermination,
                        self.JobOfferedYesNo)

            dataPublished = self.publisher.publish(self.recordType, 0, '00000000-0000-0000-0000-000000000000',
                                                   self.userId, '', '', '', '', '', appOutcomeTuple)

    def appOutcomeEditPBClicked(self):
        selectedappOutcome = self.applicationOutcomeUi.applicationOutcomeTableWidget.selectedItems()
        if selectedappOutcome:
            self.getapplicationOutcomefromSelection(selectedappOutcome)
            self.setDialogFromapplicationOutcome()
            result = self.applicationOutcomeUi.exec()
            if result != 0:
                appOutcomeTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                   self.criteriaPassFail, self.ReasonForFailure, self.MyersBriggs, self.WhoMadeDetermination,
                                   self.JobOfferedYesNo)

                dataPublished = self.publisher.publish(self.recordType, 1,
                                                   '00000000-0000-0000-0000-000000000000', self.userId,
                                                   '', '', '', '', '', appOutcomeTuple)

    def appOutcomeDeletePBClicked(self):
        selectedappOutcome = self.applicationOutcomeUi.applicationOutcomeTableWidget.selectedItems()
        if selectedappOutcome:
            self.getapplicationOutcomefromSelection(selectedappOutcome)
            self.setDialogFromapplicationOutcome()
            result = self.applicationOutcomeUi.exec()
            if result != 0:
                appOutcomeTuple = (self.applicationId, self.othId, self.contract, self.classification, self.location,
                                   self.criteriaPassFail, self.ReasonForFailure, self.MyersBriggs, self.WhoMadeDetermination,
                                   self.JobOfferedYesNo)

                dataPublished = self.publisher.publish(self.recordType, 2,'00000000-0000-0000-0000-000000000000',
                                                       self.userId, '', '', '', '', '', appOutcomeTuple)

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
            self.othId = record[self.firstData + 1]
            self.contract = record[self.firstData + 2]
            self.classification = record[self.firstData + 3]
            self.location = record[self.firstData + 4]
            self.criteriaPassFail = record[self.firstData + 5]
            self.ReasonForFailure = record[self.firstData + 6]
            self.MyersBriggs = record[self.firstData + 7]
            self.WhoMadeDetermination = record[self.firstData + 8]
            self.JobOfferedYesNo = record[self.firstData + 9]
            rowPosition = self.applicationOutcomeUi.applicationOutcomeTableWidget.rowCount()
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 0, QTableWidgetItem(self.appOutcomeId))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 1, QTableWidgetItem(self.othId))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 2, QTableWidgetItem(self.contract))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 3, QTableWidgetItem(self.classification))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 4, QTableWidgetItem(self.location))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 5, QTableWidgetItem(self.criteriaPassFail))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 6, QTableWidgetItem(self.ReasonForFailure))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 7, QTableWidgetItem(self.MyersBriggs))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 8, QTableWidgetItem(self.WhoMadeDetermination))
            self.applicationOutcomeUi.applicationOutcomeTableWidget.setItem(rowPosition, 9, QTableWidgetItem(self.JobOfferedYesNo))
            # Done - clean up thetable
            self.applicationOutcomeUi.applicationOutcomeTableWidget.resizeColumnsToContents()
            # ToDo uncomment below
            # self.ui.dwbEmployeesTable.setColumnWidth(0, 0)    # Hide the ID