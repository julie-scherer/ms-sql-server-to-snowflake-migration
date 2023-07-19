class Utils:
    ## Microsoft SQL server
    # MSSQL_SERVER = 'rds-ue2-prod-data-read-replica-creo01.cmctpgdigwuk.us-east-2.rds.amazonaws.com'

    ## Name of Snowflake database
    # SF_DATABASE = 'SRC'
    SF_DATABASE = 'CREO'

    ## MSSQL database(s) and table names
    #  >> Format: BATCH1 = ('database_name', ['column_name', 'column_name', ... ])
    #             ...
    #             BATCHX = ('database_name', ['column_name', 'column_name', ... ])

    ## List of MSSQL databases/tables to run in current batch
    #  >> Format: BATCH = [ BATCH1, ... , BATCHX ]

    ## WINCHK BATCHES
    # WIN_BATCH1 = ('WINCHK', ['__CardsToDelete', '__RefactorLog', '_AARCC', '_ACHBankFor2104PostScript', '_achHistoryPresentmentXrefForConvert', '_achHistoryTransDetailVerify', '_achPendingConvert1905', '_AchPresentment1904Convert', '_achProcessingQueueConvert1905', '_achRequestConvert1905', '_AchUniquePresentment1904Convert', '_CH022850StatementFixes', '_CH035718_DOCLOST', '_CH035718_DWDOCID', '_CH039338_PendingReason', '_CH040985_CSV_Data_For_NOOA', '_CH040985_MissingEquifaxReasons', '_CollectionsDebtRecall', '_DsExclude', '_Dup_CustomerIdentificationRecords', '_FormLetterOnDemand_200103', '_formLetterOnDemand_Delete_201003', '_FormLetterPrinted_200103', '_FormLetterResult_200103', '_IN594077_RIFees', '_IN600631', '_IN600631Test', '_IssuerBankAccount', '_NextStatementFixes', '_OELoansOutOfBalance', '_RCC2022aaspeedy', '_RCC2022AdAstra', '_RCC2022AdAstraDryRun', '_RCC2022NonAdAstra', '_RCC2022NonAdAstraDryRun', '_RCC2022srconly', '_RISREPTCollectionsDebtRecall', '_ScheduledPrimaryCardManualFixes', '_tmpACHProcessingQueue', '_tmpFundingMethod', '_tmpLocationFundingMethod', '_VirginiaDateFixes', 'AACbExportDataArchive', 'ABLFacility', 'AccumConfig', 'AccumConfigHistory', 'ACH_History', 'ACH_Recv', 'ach_recv_fix', 'ACH_ReturnCode', 'ACH_ReturnCodeHistory', 'ACH_Sent', 'ACHGroup', 'ACHInterestCreditOverride', 'ACHLoanPaymentRefund', 'ACHLoanPaymentRefundStatus', 'ACHOpenEndLoanStreamInterestCredit', 'ACHPending', 'ACHProcessingQueue', 'ACHProcessingType', 'ACHQueue', 'ACHReason', 'AchRecvItem', 'ACHRequest', 'ACHSentParent', 'AchUseLegacyScheduledAchCollectionsAmtLogic', 'AdAstraWebInventory', 'AddressFormat', 'AddressRemovedReason', 'AddressStatus', 'AddressSuffix', 'AddressSuite', 'AddressType', 'AdjustmentAmountType', 'AgentAction', 'AgentResult', 'AMLAdditionalParty', 'AMLFileLog', 'AMLForeignAddress', 'AMLOccupation', 'AMLOccupationReason', 'AMLThresholdRule', 'AMLThresholdRuleTransXref', 'AMLThresholdType', 'ApiApplication', 'ApplyDueType', 'ApplyPaymentOrder', 'AreaCode', 'AspectAddOnMetricId', 'AspectAddOnMetricIdDimensionIdXref', 'AspectDimensionId', 'AspectExportJob', 'AspectExportJobAddOnMetricIdXref', 'Attorney', 'AttorneyType'])
    # WIN_BATCH2 = ('WINCHK', ['AttorneyTypeXRef', 'AuthorizedVisitorContact', 'AutoReport', 'AutoReportEditHistory', 'AutoReportEmail', 'AutoReportParameter', 'AutoReportRunSchedule', 'AutoReportSchedule', 'AutoReportTab', 'BalSheet_TransDetail', 'BalSheet2', 'BalSheetColumns2', 'Bank', 'BankAccount', 'BankClassification', 'BankClassificationType', 'BankClosed', 'BankParent', 'BankruptcyTrustee', 'BankruptcyTrusteeClaimType', 'BankStatus', 'BatchExecution', 'BillerOCRRegion', 'BillPayBiller', 'BillPayBillerStatus', 'BillPayVendor', 'BlockReason', 'BooleanQuestion', 'BooleanQuestionCompany', 'BooleanQuestionProcessType', 'BooleanQuestionResponse', 'BooleanQuestionResponseRefinanceLoanApplication', 'BumpUpReason', 'BumpUpTierType', 'BusinessEntity', 'BusinessLegalType', 'BusinessLoan', 'BusinessType', 'CABLender', 'CallCampaign', 'CallCampaignAppointment', 'CallCampaignDoNotCall', 'CallCampaignHistory', 'CallCampaignQueue', 'CallCampaignQueueActivity', 'CallCampaignQueueStatus', 'CallCampaignQueueStatusReason', 'CapsCCTXRef', 'CapsHold', 'CapsRun', 'CapsRunStatus', 'CapsSkipReason', 'CapsUpdates', 'CardBatchSettle', 'CardCoolingOff', 'CardFundingRequestType', 'CardFundingStatusCode', 'CardFundingTransaction', 'CardFundingVendor', 'CardGovernorActionType', 'CardGovernorHistory', 'CardGovernorOverrideHistory', 'CardGovernorValidation', 'CardGovernorValidationLocation', 'CardResponseType', 'CardReview', 'CardType', 'CashedCheck', 'CashedCheckImage', 'CashedCheckMICR', 'CashedCheckPayment', 'CashedCheckPaymentRefund', 'CC_Status', 'CCardResponses', 'Certificate', 'CFPB_AssumedBadAuths', 'CFPB_AssumedBadCardAuths', 'CFPB_AssumedBadLoanAuths', 'CFPB_AuthorizationGroups', 'CFPB_BadAuths', 'CFPB_BadLoans', 'CFPB_LatestAuths', 'CH029414LoanBankCardUpdateFromTransLogSnapshot', 'Channel', 'ChannelPaymentCommunicationLimit', 'CheckImageType', 'CheckPaymentType', 'CheckReturn', 'CheckType', 'chk_type', 'ClientApplication', 'ClrDataType', 'CollBonusDetail', 'CollBonusPTP', 'CollBonusTasks', 'CollectionAction', 'CollectionAgency', 'CollectionAgencyPct', 'CollectionAgingConfig', 'CollectionAgingConfigDays', 'CollectionAgingConfigDaysBackfill', 'CollectionAgingItem', 'CollectionAgingItemBackfill', 'CollectionMovement', 'CollectionNote', 'CollectionSettlement', 'CollectionSettlementReason', 'CollectionStream', 'Communication', 'CommunicationCommunicationEvent', 'CommunicationConsentConfig', 'CommunicationConsentConfigCommunicationEvent', 'CommunicationConsentConfigHistory', 'CommunicationConsentThirdPartyQueue', 'CommunicationEvent', 'CommunicationEventQueue', 'CommunicationEventQueueValue', 'CommunicationGroup', 'CommunicationGroupChannel', 'CommunicationGroupChannelVisibility', 'CommunicationLocation', 'CommunicationPreferenceOverride', 'CommunicationType', 'Company', 'CompanyBankAccount', 'CompanyBankAccountGLAcct', 'CompanyCredential', 'CompanyCredentialType', 'CompanyDetail', 'CompanyDetailHistory', 'CompanyDocument', 'CompanyExpenseType', 'CompanyHistory', 'ComponentOneReportLog', 'ConfigurableQuestion', 'ConfigurableQuestionAllowableResponse', 'ConfigurableQuestionCategory', 'ConfigurableQuestionNumericRange', 'ConfigurableQuestionSet', 'ConfigurableQuestionSetConfigurableQuestion', 'Country', 'CourtesyPayout', 'CourtesyPayoutType', 'CpiuDetail'])
    # WIN_BATCH3 = ('WINCHK', ['CpiuExceptionReport', 'CpiuMaster', 'CreditCardAttempts', 'CreditCardBlock', 'CreditCardBrand', 'CreditCardResultCode', 'CreditCardResultCodeEdit', 'CreditCardResultCodeType', 'CreditCards', 'CreditCardsEdit', 'CreditCardTrans', 'CreditCardTransRepostPayment', 'CreditCardVendor', 'CreditLimitBumpUp', 'CreditLimitBumpUpReason', 'CreditLimitOffer', 'CreditLimitOfferAudit', 'CreditLimitOfferDecline', 'CreditReportingActivity', 'CreditReportingBaseSegment', 'CreditReportingBaseSegmentHistory', 'CreditReportingBaseSegmentTag', 'CreditReportingBaseSegmentTrace', 'CreditReportingDisputeCode', 'CreditReportingDisputeResponseCode', 'CreditReportingDisputeSource', 'CreditReportingL1Segment', 'CreditReportingL1SegmentHistory', 'CreditReportingLoanActivity', 'CreditReportingLoanActivityHistory', 'CreditReportingLoanDispute', 'CreditReportingLoanDisputeNote', 'CreditReportingLoanDisputeRequest', 'CreditReportingLoanDisputeResponseDetail', 'CreditReportingLoanProductLocation', 'CreditReportingLoanProductLocationHistory', 'CreditReportingLoanStatus', 'CreditReportingLoanStatusHistory', 'CreditReportingNaturalDisaster', 'CreditReportingProcessingQueue', 'CreditReportingProcessingQueueHistory', 'CreditReportingRarrActivityXRef', 'CreditReportingRule', 'CreditReportingRuleHistory', 'CreditReportingRuleType', 'CreditReportingRun', 'CreditReportingRunStatus', 'CreditReportingStatus', 'CreditReportingTarrActivityXref', 'CreditReportingTransCodeActivityXRef', 'CreditReportingVendorConfig', 'CreditReportingVendorConfigHistory', 'CreditRptPrint', 'CreditVendor', 'CreditVendorApiHistory', 'CreditVendorData', 'CuroHelp', 'Currency', 'CurrencyExchangeConfig', 'CurrencyExchangeConfigHistory', 'CurrencyExchangeTrans', 'Customer', 'CustomerActivity', 'CustomerAddress', 'CustomerAddressEdit', 'CustomerAppDate', 'CustomerBusiness', 'CustomerCardReview', 'CustomerCoolingOff', 'CustomerCreditRpt', 'CustomerCreditRptDetail', 'CustomerEarnedCredit', 'CustomerEdit', 'CustomerEmployer', 'CustomerEmployerEdit', 'CustomerExpense', 'CustomerExpenseDetail', 'CustomerFeedback', 'CustomerFeedbackCategory', 'CustomerFeedbackResolution', 'CustomerFeedbackSource', 'CustomerFeedbackSubCategory', 'CustomerFeedbackType', 'CustomerFeedbackTypeCategoryXRef', 'CustomerFlash', 'CustomerFlashMPayRebates', 'CustomerFlashORRebates', 'CustomerFlashQuestions', 'CustomerFlashResponse', 'CustomerIdentification', 'CustomerIdentificationEdit', 'CustomerIncome', 'CustomerIncomeBackup', 'CustomerLastCreditReport', 'CustomerLead', 'CustomerLeadAction', 'CustomerLeadActivity', 'CustomerLeadInsertLoanByPhoneDenied', 'CustomerLeadLocation', 'CustomerLeadNote', 'CustomerLeadReason', 'CustomerLeadStatus', 'CustomerLeadStatusReason', 'CustomerLoanPaymentForgivenessEligible' ])
    # WIN_BATCH4 = ('WINCHK', ['CustomerMergeHistory', 'CustomerMLA', 'CustomerNote', 'CustomerPaymentAccount', 'CustomerPaymentAccountEvent', 'CustomerPhoneNumber', 'CustomerPhoneNumberEdit', 'CustomerResponse', 'CustomerSDNCert', 'CustomerServiceMessageDisposition', 'CustomerSurvey', 'CustomerThirdPartyLink', 'CustomerThirdPartyLinkVaultMgrAuthorization', 'DbPurgeTables', 'DebtSale', 'DebtSaleExport', 'DebtSaleExportDetail', 'DebtSaleExportLog', 'DecreaseAmountOwedReason', 'Denomination', 'DepositBag', 'DepositBagDetail', 'DepositBagHistory', 'DepositChk', 'DepositChkDetail', 'DepositDebitCard', 'DepositDebitCardDetail', 'DepositOrder', 'DepositStatus', 'DialerJob', 'DialerJobAllowedAutoDialTCPAResult', 'DialerJobChecksLocation', 'DialerJobLoanProductEnableNewLoan', 'DialerJobRisAudit', 'DialerKeys', 'DialerResultCodes', 'DiscountLocations', 'DiscountMaster', 'DiscountMasterLoanProduct', 'DiscountSecurity', 'DiscountType', 'DiscountUsed', 'District', 'DMA', 'Doc10000Trans', 'Doc10000TransDetail', 'DocImage', 'DocPrint', 'DocTemplate', 'DocumentSignatureAction', 'DocumentSigningStatus', 'DocumentSplitHistory', 'DocumentSplitHistoryDetail', 'DocuwareCabinet', 'DocuwareCabinetEdit', 'DocuwareCabinetType', 'DocuwareCashedCheckXRef', 'DocuwareCustomerIdentificationXRef', 'DocuwareDocument', 'DocuwareID', 'DocuwareLoanDoc', 'DocuwareLoanLkup', 'DocuwareScanError', 'DocuwareScannedDocXRef', 'DocuwareStatus', 'DocuwareStatusUpdate', 'DocuwareStatusXRef', 'DocuwareVisitorDocXRef', 'DocuwareVisitorEmailAttachmentXref', 'DrawerBag', 'DrawerBagParsedCash', 'DrawerMaster', 'DrawerMasterParsedCash', 'DrawerService', 'DrawerX', 'DrawerXService', 'DrawerZ', 'DrawerZCalcParsedCash', 'DrawerZCash', 'DrawerZDrawerBag', 'DrawerZEnteredParsedCash', 'DrawerZService', 'DualApprovalMessage', 'EarnedCreditTrans', 'EarnedCreditType', 'EditHistory', 'EmailAttachmentType', 'EmailDisposition', 'EmailTemplate', 'EmailVerification', 'EmploymentRegions', 'EmploymentRequest', 'EmploymentStatus', 'EndOfDayInventoryDetail', 'EndOfDayRpt', 'EndOfDayRptDetail', 'EOscarBatch', 'EOscarBatchDetail', 'EOscarDetailDisputeCode', 'EOscarDisputeCode', 'ErrorLog', 'ESignDocMethod', 'ESignLoan', 'ESignLoanDoc', 'ESignLoanStatus', 'ESignOptIn', 'ESignOptInDoc', 'ExchangeRate', 'ExchangeRateDaily', 'ExcludeFromCapsHistory', 'ExpenseType', 'ExportProfile', 'ExportProfilePath', 'ExternalAppConfig', 'ExternalAppConfigHistory', 'ExternalAppMaster', 'ExternalAppRunDates', 'ExternalApps', 'EyeColor', 'FcrmAmlCheckTypeXref', 'FcrmAmlServiceXRef', 'FcrmAmlTransCode', 'FcrmAmlTransCodeXRef', 'Feature'])
    # WIN_BATCH5 = ('WINCHK', ['FileUploadQueue', 'FirstDataGlobalBinFile', 'ForceApprovalQuestion', 'ForceApprovalValue', 'FormLetter', 'FormLetterAfterLetterXRef', 'FormLetterAuditCode', 'FormLetterBatch', 'FormLetterBatchBuildLetterProgress', 'FormLetterBatchStatus', 'FormLetterBatchValidation', 'FormLetterBatchVendorFile', 'FormLetterConfig', 'FormLetterEmail', 'FormLetterLoanDocumentsFile', 'FormLetterLoanDocumentsState', 'FormLetterLoanHistoryFile', 'FormLetterLoanHistoryState', 'FormLetterLocation', 'FormLetterOnDemand', 'FormLetterPrinted', 'FormLetterProduct', 'FormLetterReplacesXRef', 'FormLetterResult', 'FuelType', 'FundingMethod', 'FundingMethodGroupConfig', 'FundingMethodGroupConfigHistory', 'FundingMethodGroupItemConfig', 'FundingMethodGroupItemConfigHistory', 'FundingMethodLocationXRefConfig', 'FundingMethodLocationXRefConfigHistory', 'GalileoAlertType', 'GalileoResponseCode', 'GLAcct', 'GLAcctGlobal', 'GLAcctGlobalHistory', 'GLAcctHistory', 'GLAcctLoanProductGroup', 'GLAcctLoanProductGroupHistory', 'GLAcctLocationGroup', 'GLAcctLocationGroupHistory', 'Global', 'GlobalHistory', 'GlobalStates', 'GlobalStatesHistory', 'GoldConfig', 'GoldConfigHistory', 'GoldConfigItem', 'GoldDailyBag', 'GoldDailyBagDetail', 'GoldItem', 'GoldTrans', 'GoldTransCustomer', 'GoldTransDetail', 'GoldTransfer', 'GoldTransferDetail', 'GoldTransStoneDetail', 'GoodCustomerStudy', 'HairColor', 'Holiday', 'IatDialerResults', 'ICSBatchImportRun', 'ICSBatchImportRunData', 'IdentificationType', 'IdentificationTypeAML', 'IdentificationTypeCategory', 'IdentificationTypeHistory', 'IdentificationTypeRule', 'IdentificationTypeRuleXRef', 'IdentificationTypeState', 'IdentificationTypeStateHistory', 'IdentificationTypeVerify', 'ImageCashLetter', 'ImageCashLetterBundle', 'ImageCashLetterDetail', 'IN459126', 'IN466857', 'IncomeAmountType', 'IncomeCalculationCycle', 'IncomeJobType', 'IncomeSource', 'IncomeSourceHistory', 'IncomeType', 'IncomeTypeHistory', 'IncomeTypeLocation', 'IncomeTypeLocationHistory', 'IncomeVerificationMessage', 'IncomeVerificationSource', 'IncomeVerificationSourceLocation', 'IncomeVerificationType', 'IncomeVerifyMethod', 'IncomeVerifyMethodHistory', 'IncomeVerifyMethodLocation', 'IncomeVerifyMethodLocationHistory', 'InitGLList', 'InitGLListHistory', 'InsuranceCancelReason', 'InsuranceClaim', 'InsuranceClaimEdit', 'InsuranceClaimReason', 'InsuranceClaimStatus', 'InsuranceEnrollment', 'InsurancePaymentBatch', 'InsurancePaymentBatchItem', 'InsurancePaymentBatchPayment', 'InsuranceStatus', 'InsuranceStatusHistory', 'InterCompanyLeadSaleConfig', 'InterestType', 'InternalProcessEmail', 'InternalProcessEmailBody', 'InternalProcessEmailType', 'InterpersonalRelationshipType', 'InvalidCustomerAddress', 'InvalidStreet', 'IPBlock', 'IpToCountry', 'Issuer', 'IssuerEdit'])
    # WIN_BATCH6 = ('WINCHK', ['KBB_Log_Config', 'KbbApiCall', 'LanguageType', 'LegalVerification', 'Lender', 'LenderHistory', 'LetterNotification', 'LetterNotificationHistory', 'LienHolder', 'Loan', 'LoanABLFacility', 'LoanAdjustmentBatchSettings', 'LoanAdjustmentType', 'LoanApplication', 'LoanApplicationAddress', 'LoanApplicationBusiness', 'LoanApplicationCommunicationConsent', 'LoanApplicationDiscounts', 'LoanApplicationEmployer', 'LoanApplicationEmployerPayCheck', 'LoanApplicationExpense', 'LoanApplicationExpenseDetail', 'LoanApplicationIdentification', 'LoanApplicationIncome', 'LoanApplicationOutOfWallet_LEGACY', 'LoanApplicationPendingReason', 'LoanApplicationPendingReasonConfigurableQuestionResponse', 'LoanApplicationProduct', 'LoanApplicationProductConfigurableQuestionResponse', 'LoanApplicationProductLoanDoc', 'LoanApplicationProductScheduledPayment', 'LoanApplicationScoreHistory', 'LoanApplicationSourceLoan', 'LoanApplicationStatusChange', 'LoanApplicationThirdPartyIncome', 'LoanApplicationVehicleInformation', 'LoanAuthorizedPaymentMethod', 'LoanAuthorizedPaymentMethodPending', 'LoanBankCardPending_Hold', 'LoanBankCardPending_Hold_Prev', 'LoanCCallChange', 'LoanChkAcctChange', 'LoanConfigApplyPaymentOrder', 'LoanConversionEligible', 'LoanCoPledger', 'LoanCreditLimit', 'LoanDepositOrderHistory', 'LoanDepositOrderResetReason', 'LoanDepositStatusHistory', 'LoanDoc', 'LoanDocAmort', 'LoanDocHistory', 'LoanDocPrinted', 'LoanDocType', 'LoanDocUpload', 'LoanDocUsed', 'LoanDueDateChange', 'LoanExpense', 'LoanFunding', 'LoanFundingAchHistoryXRef', 'LoanFundingAchProcessingQueueXRef', 'LoanFundingHistory', 'LoanFundingHistoryDetail', 'LoanFundingMethodHistory', 'LoanFundingStatus', 'LoanFundingType', 'LoanImport', 'LoanIncome', 'LoanNote', 'LoanNSFFee', 'LoanOriginationCode', 'LoanOverride', 'LoanOverrideReason', 'LoanOverrideType', 'LoanPayment', 'LoanPaymentAchQueueDetail', 'LoanPaymentAddToQueue', 'LoanPaymentCashAdvance', 'LoanPaymentCheckPaymentTypeXref', 'LoanPaymentDecreaseAmountOwed', 'LoanPaymentDueDate', 'LoanPaymentForgiven', 'LoanPaymentInsuranceClaimXRef', 'LoanPaymentMPay', 'LoanPaymentMPayAuth', 'LoanPaymentOpenEnd', 'LoanPaymentOpenEndStream', 'LoanPaymentPTPDetail', 'LoanPaymentRefund', 'LoanPaymentRescind', 'LoanPaymentSkip', 'LoanPaymentSPay', 'LoanPaymentStaged', 'LoanPaymentStagedCommunication', 'LoanPaymentStagedCommunicationAccount', 'LoanPaymentStagedCommunicationCard', 'LoanPaymentStagedNotSent', 'LoanPaymentStagedNotSentReason', 'LoanPaymentStagedStatus', 'LoanPaymentSuspendInterest', 'LoanPaymentWaiveRIFee', 'LoanPayoffDate', 'LoanProduct', 'LoanProductBlocked', 'LoanProductBlockType', 'LoanProductConfig', 'LoanProductConfigAnnualRateBandLoanAmtRange', 'LoanProductConfigApprovalRate', 'LoanProductConfigBumpUp'])
    # WIN_BATCH7 = ('WINCHK', ['LoanProductConfigBumpUpIncomeTypePriority', 'LoanProductConfigEdit', 'LoanProductConfigEligibilityLoanHistory', 'LoanProductConfigEligibilityLoanHistoryProducts', 'LoanProductConfigExpenseType', 'LoanProductConfigFixedPaymentCount', 'LoanProductConfigInsuranceRate', 'LoanProductConfigInterestRate', 'LoanProductConfigLoanFeeRate', 'LoanProductConfigLoanStats', 'LoanProductConfigMaintenanceFeeRate', 'LoanProductConfigMaxLoanAmtRate', 'LoanProductConfigOpenEnd', 'LoanProductConfigRIRate', 'LoanProductConfigTitle', 'LoanProductConfigVariableRate', 'LoanProductConversion', 'LoanProductEnableNewLoan', 'LoanProductFeature', 'LoanProductFeatureType', 'LoanProductFinancialGroup', 'LoanProductInternetZipCodeExclusion', 'LoanProductRollover', 'LoanProductTila', 'LoanRateSource', 'LoanService', 'LoanStatus', 'LoanStatusChange', 'LoanType', 'LocaleSetting', 'LocaleTranslator', 'LocationTransactionProcessor', 'LocationUS_ZipcodesXRef', 'MaritalStatus', 'MarketingInvitation', 'MarketingInvitationHistory', 'MarketingInvitationOverrideType', 'MarketingInvitationSummary', 'Markets', 'Message', 'MessageClass', 'MessageScenario', 'MimeType', 'MoneyGramLookupLog', 'MoneyGramLookupType', 'MoneyGramTransmissionLog', 'MoneyGramTransmissionType', 'MoneyOrderDailyTotal', 'MoneyOrderPrinter', 'MoneyOrderPrinterStoreWindows', 'MOStatus', 'MOStatusHistory', 'MPayAmort', 'MPayAmortDueDateChange', 'MPayInterest', 'MPayLoan', 'MPayLoanInSyncAdj', 'MPayRecalcAmortAdj', 'MPayRecalcInterestAdj', 'MPayRecalcLoanPaymentAdj', 'MSA', 'NetSpendTrans', 'NoAdverseActionLetter', 'NobleConfiguration', 'NobleConfigurationHistory', 'NoteType', 'NotificationType', 'OCRRegion', 'OEndLoanInSyncAdj', 'OEndLoanInSyncAdjDetail', 'OpenEndInterest', 'OpenEndInterestRate', 'OpenEndInterestStream', 'OpenEndLoan', 'OpenEndLoanCycle', 'OpenEndLoanCycleCustomerSnapshot', 'OpenEndLoanStatement', 'OpenEndLoanStatementHistory', 'OpenEndLoanStatementSnapshot', 'OpenEndLoanStream', 'OpenEndLoanStreamInterestRate', 'OpenEndLoanStreamStatementSnapshot', 'OpenEndQueueLoanCycleUpdate', 'OpenEndRebootAdj', 'OpenEndRecalcInterestAdj', 'OpenEndRecalcLoanPaymentAdj', 'OpenEndRecalcStatementAdj', 'OpenEndRecalcStatementSnapshot', 'OpenEndScheduledPayment', 'OptPlusBinProduct', 'OptPlusBinService', 'OptPlusCardDetail', 'OptPlusCarrier', 'OptPlusCarrierHistory', 'OptPlusDirectDeposit', 'OptPlusEdit', 'OptPlusEmail', 'OptPlusEmailLocation', 'OptPlusEmployment', 'OptPlusExportInitGL', 'OptPlusExportTransCodes', 'OptPlusGlobal', 'OptPlusMerchant', 'OptPlusProduct', 'OptPlusRDFAccountCard', 'OptPlusRDFAuthorizedTransactions', 'OptPlusRDFConsent', 'OptPlusRDFCustomerMaster', 'OptPlusRDFLookUp'])
    # WIN_BATCH8 = ('WINCHK', ['OptPlusRDFODTransition', 'OptPlusRDFPostedTrans', 'OutOfWalletAlert', 'OutOfWalletError', 'OutOfWalletQuestion', 'OutOfWalletQuestionAnswerChoice', 'OutOfWalletQuiz', 'OutOfWalletQuizLoanApplication', 'OutOfWalletQuizLoanFunding', 'OutOfWalletVendor', 'OverShort', 'OverShortCategory', 'ParseCash', 'PasswordType', 'PayCycle', 'PaydayLoan', 'PaydayLoanApproval', 'PaydayLoanQualification', 'PaymentAccountEventType', 'PaymentAuthHistory', 'PaymentAuthorizationDocumentRequirement', 'PaymentAuthType', 'PaymentMethod', 'PaymentMethodDeauthorizationReason', 'PaymentMethodProvisionalApprovalType', 'PaymentPlan', 'PaymentPlanRequest', 'PaymentPlanRequestDetail', 'PaymentPlanRequestDetailPaymentInfo', 'PaymentPlanRequestLoanXRef', 'PaymentPlanRequestPTPXRef', 'PaymentPlanRequestStatus', 'PaymentPlanRequestVisitorDocumentXRef', 'PaymentPlanType', 'PaymentsPastDue', 'PaymentsPastDueBackfill', 'PaymentsPastDueDetail', 'PaymentsPastDueDetailBackfill', 'PaymentsPastDueTransactionType', 'PAYROLL1', 'PayStub', 'PaywareTsysTimezone', 'PendingReason', 'PendingReasonClientMessage', 'PendingReasonConfig', 'PendingReasonQuestionSet', 'PendingReasonResolveReason', 'PersonTitle', 'PhoneSkillsCall', 'PhoneSkillsGrade', 'PhoneSkillsGrader', 'PhoneSkillsReason', 'PhoneSkillsSequence', 'PhotoIdType', 'PostalHoliday', 'PowerOfAttorney', 'PowerOfAttorneyType', 'PrepaidCardBin', 'PrepaidCardBinCompany', 'PrepaidCardBinCompanyHistory', 'PrepaidCardGroup', 'PrepaidCardStopPayment', 'PrepaidCardStopPaymentEdit', 'PrepaidCardStopPaymentReason', 'PrepaidCardTransAction', 'PreQualificationConsentSource', 'PrescreenQuestion', 'PrescreenQuestionState', 'PrescreenQuestionType', 'Presentment', 'PresentmentCreditCardTransXRef', 'PresentmentNotSentReason', 'PresentmentPaymentMethod', 'PresentmentRequest', 'PresentmentRequestACHHistoryXRef', 'PresentmentRequestNotSent', 'PresentmentRequestNotSentReason', 'PresentmentRequestReason', 'PresentmentType', 'PRICES', 'ProcessConfig', 'ProcessConfigDetail', 'ProcessConfigDetailHistory', 'ProcessConfigInstance', 'ProcessConfigInstanceGroup', 'ProcessConfigInstanceTeller', 'ProcessSchedule', 'ProductOpenLoanMatrix', 'ProductType', 'PromiseToPay', 'PromiseToPayCommunication', 'PromiseToPayDetail', 'PromiseToPayDetailEdit', 'PromiseToPayDetailTrans', 'PromiseToPayTimeSlotConfig', 'PSiGateResponse', 'PTPPaymentMethod', 'PTPPaymentPlanCheck', 'PTPPaymentPlanCheckHistory', 'PTPPaymentPlanConfig', 'PTPPaymentPlanConfigDocumentXRef', 'PTPPaymentPlanConfigDocumentXRefHistory', 'PTPPaymentPlanConfigHistory', 'PTPPaymentPlanDueDateChangeType', 'PTPPaymentPlanLoanProductEnableNewLoan'])
    # WIN_BATCH9 = ('WINCHK', ['PTPPaymentPlanLoanProductEnableNewLoanHistory', 'PTPPaymentPlanPaymentMethod', 'PTPPaymentPlanPaymentMethodHistory', 'PTPPaymentPlanPaymentSchedule', 'PTPPaymentPlanSecurityGroup', 'PTPPaymentPlanSecurityGroupHistory', 'PTPPaymentPlanType', 'PurchaseService', 'Race', 'RawDataType', 'RbcEFundBatch', 'RbcEFundBatchDetail', 'RbcEFundBatchSummary', 'RbcEFundResponseCode', 'RbcEFundSecurity', 'ReasonForArrears', 'Receipt', 'RedactedWords', 'ReferralMethod', 'RefinanceLoanApplication', 'RefinanceLoanApplicationPendingReason', 'RefinanceLoanApplicationPendingReasonConfigurableQuestionResponse', 'Region', 'RepoCaseHistory', 'ResumeInterestType', 'ReturnCheckDetail', 'ReturnCheckFile', 'RightPartyContact', 'RIPTPPaymentPlanConfig', 'RIS', 'RISAUDIT', 'RISREPT', 'RisReptDoNotContact', 'RisReptDoNotContactReason', 'RisReptDoNotContactType', 'RisTask', 'RISTYPE', 'RitaPwd', 'RiUrgentNote', 'RuleDef', 'RuleDefEdit', 'RuleDefSet', 'RuleDefSetDetail', 'RuleDefSetDetailEdit', 'RuleDefType', 'ScannedDocument', 'ScannedDocumentOverride', 'ScheduledLoanMod', 'ScheduledLoanModStatus', 'ScheduledLoanModType', 'ScheduledPayment', 'ScoringPendingReasonXref', 'SDNAddress', 'SDNAlternate', 'SDNList', 'SDNMain', 'SdnMatch', 'SecurityAnswer', 'SecurityGroup', 'SecurityGroupHistory', 'SecurityQuestion', 'ServiceDetail', 'ServiceMaster', 'ServiceTrans', 'ServiceTransDetail', 'ServiceTransNote', 'SG_RIGHTS', 'SignatureLoanApproval', 'SkipPayment', 'SkipPaymentEligible', 'SkipTraceConfig', 'SkipTraceConfig_RisAudit', 'SkipTraceConfigHistory', 'SkipTraceEvents_NotUsed', 'SkipTraceStep', 'SkipTraceStep_AuditCategory', 'SkipTraceStep_Location', 'SkipTraceStep_ProductCode', 'SkipTraceThread', 'SkipTraceVendor', 'SkipTraceVendorHistory', 'SPayInterest', 'SPayLoan', 'SPaySchedRollover', 'SPaySchedRolloverDetail', 'SpecialMessage', 'SpecialMessage_AzCusts', 'SpecialMessageHistory', 'SpecialMessageLoanProduct', 'SpecialMessageLocation', 'SpousalNotificationExport', 'Store_Windows', 'StoreClosed', 'TaskActionResult', 'TaskActionResultXref', 'tecodes', 'TeletrackReportingData', 'TellerComputer', 'TELLERID', 'TellerIDEdit', 'TellerLogin', 'TellerLoginFail', 'TellerLoginFailReason', 'TellerParsedCashException', 'TellerPwdHistory', 'TellerSecurity', 'TellerTitle', 'TellerTitleEdit', 'TellerType', 'TestCreditCard', 'ThirdParty', 'ThirdPartyEmploymentStatus', 'ThirdPartyReferenceType', 'TitleLoan', 'TitleLoanApproval', 'TotalDailyFees', 'TotalDailyFees2', 'TransactionDirection', 'TransactionNote', 'TransactionProcessor', 'TransactionProcessorCompanyBankAccountConfig', 'TransactionProcessorCompanyBankAccountConfigHistory', 'TransactionProcessorConfig', 'TransactionProcessorConfigHistory'])
    # WIN_BATCH10 = ('WINCHK', ['TransCode', 'TransDetail', 'TransDetailAcct', 'TransDetailCardProducts', 'TransDetailCash', 'TransDetailCashParsedCash', 'TransDetailCheck', 'TransDetailIntShort', 'TransDetailLoan', 'TransDetailService', 'TransferFunds', 'TransferFundsInterStore', 'TransPOS', 'TransUnionCodes', 'US_States', 'US_Zipcodes', 'VariableRateType', 'VaultCount', 'VaultCountCalcParsedCash', 'VaultCountEnteredParsedCash', 'VaultCountService', 'VaultMaster', 'VaultMasterParsedCash', 'VaultMgrAssignment', 'VaultMgrAuthorization', 'VaultMgrAuthorizationDetail', 'VaultMgrAuthorizationNote', 'VaultRecalcAdj', 'VaultService', 'Vehicle', 'VehicleCondition', 'VehicleHistory', 'VehicleLegalStatus', 'VehicleOdometerCode', 'VehicleQuote', 'VehicleType', 'VergeLoanTransferEligibility', 'VeritecLoanID', 'Visitor', 'VisitorApiAuthorization', 'VisitorAuthData', 'VisitorAuthenticationCode', 'VisitorAuthenticationCodeAttempt', 'VisitorAuthenticationCodeConfig', 'VisitorAuthenticationCodeConfigChannel', 'VisitorAuthenticationCodeConfigHistory', 'VisitorBlock', 'VisitorBlockHistory', 'VisitorBlockType', 'VisitorCommunicationConsent', 'VisitorCommunicationPreference', 'VisitorDevice', 'VisitorDeviceAuthenticationCertificate', 'VisitorDeviceAuthenticationType', 'VisitorDeviceType', 'VisitorDocument', 'VisitorDocumentTemplate', 'VisitorDocumentTemplateType', 'VisitorDocumentType', 'VisitorEditHistory', 'VisitorEditHistoryDetail', 'VisitorEmail', 'VisitorEmailDisposition', 'VisitorLoanProductAnnualRateBand', 'VisitorNonMilitaryVerification', 'VisitorNonMilitaryVerificationVaultMgrAuthorization', 'VisitorPasswordHistory', 'VisitorPreQualification', 'VisitorPreQualificationXRef', 'VisitorSecurityQuestion', 'VMATransType', 'WebAlert', 'WebCallApplicationStatusHistory', 'WebCallCampaign', 'WebCallCampaignCategory', 'WebCallCategory', 'WebCallCategoryHistory', 'WebCallCatRarrAlias', 'WebCallCenterLogin', 'WebCallChatCannedResponses', 'WebCallCSRReportColumn', 'WebCallCSRReportColumnRARR', 'WebCallDualAuth'])
    # WIN_BATCH11 = ('WINCHK', ['WebCallEmailTemplateAttachment', 'WebCallEmailTemplateCategory', 'WebCallEmailTemplates', 'WebCallEmailTemplatesHistory', 'WebCallEmailTemplateToCategoryXref', 'WebCallFeatures', 'WebCallFeaturesHistory', 'WebCallInvalidPhoneNumber', 'WebCallLoanAppSourceApp', 'WebCallLoggingCategory', 'WebCallLoggingCategoryLog', 'WebCallLoggingLog', 'WebCallQueue', 'WebCallQueueAudit', 'WebCallQueueConfiguration', 'WebCallQueueStatus', 'WebCallQueueType', 'WebCallQuickNote', 'WebCallRARR', 'WebCallRARRAction', 'WebCallRARRActionHistory', 'WebCallRARRCategoryReason', 'WebCallRARRCategoryReasonHistory', 'WebCallRARRConfigHistory', 'WebCallRARReason', 'WebCallRARReasonHistory', 'WebCallRARResult1', 'WebCallRARResult1History', 'WebCallRARResult2', 'WebCallRARResult2History', 'WebCallRARRFeatures', 'WebCallRARRFeaturesHistory', 'WebCallRarrGroup', 'WebCallRarrGroupHistory', 'WebCallRARRHistory', 'WebCallRARRType', 'WebCallRARRTypeHistory', 'WebCallUserSetting', 'WebCallVisitorAlerts', 'WebCallVisitorAlertsAudit', 'WebCallWebEmergencyAlertTemplate', 'WebCallWorkItemCategoryHistory', 'WebCallWorkQueue', 'WebDailyReport', 'WebDailyReportFields', 'WebDailyReportStates', 'WebDialerAgentSurveyResult', 'WebDialerCallResult', 'WebDialerCallResultNoble', 'WebDialerPhoneLine', 'WebDialerResult', 'WebDialerResultType', 'WebDialerStatus', 'WebDialerUploadHistory', 'WebDialerUser', 'WebLead', 'WebLeadArchive', 'WebLeadBuyers', 'WebLeadCallCampaignQueueXRef', 'WebLeadCriteria', 'WebLeadCriteriaAuditCategory', 'WebLeadGen', 'WebLeadGenTiers', 'WebLeadPostData', 'WebLeadPostDataArchive', 'WebLeadPostDataValues', 'WebLeadSale', 'WebLoanCreditFraud', 'WebPixelVendor', 'WebPixelVendorData', 'WebPixelVendorDetail', 'WebPixelVendorReasonPulled', 'WebReferralMethod', 'WireTransferFileImport', 'WireTransferMatch', 'WireTransferMatchEdit', 'wucodes', 'wuprstat', 'ZipArea'])
    # BATCH = [ WIN_BATCH1, WIN_BATCH2, WIN_BATCH3, WIN_BATCH4, WIN_BATCH5, WIN_BATCH6, WIN_BATCH7, WIN_BATCH8, WIN_BATCH9, WIN_BATCH10, WIN_BATCH11 ]

    B1_CREO = ('CREO', ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication', 'CommunicationMailing', 'Config', 'ConfigHistory', 'ContactType', 'Container', 'Dataset', 'DatasetColumn', 'Datasource', 'DeadMessages', 'DeadMessages2', 'DeliveryStatus', 'Emoji', 'Folder', 'FolderContact', 'FolderMessage', 'Global', 'Log', 'MessageContact', 'MessageContactType', 'MessagePart', 'MessageStatusQueue', 'MessageType', 'Package', 'Parameter', 'Rule', 'Template', 'TemplateRule', 'TemplateType', 'TempMessage', 'User', 'WebHook'])
    B2_CREO = ('CREO', ['DatasetCell', 'DatasetRow', 'MessageContactV2', 'PackageTemplate'])
    # B2_CREO_FAILED = ('CREO', ['Contact', 'Message', 'MessageDeliveryStatus',])
    # BATCH = [ B1_CREO, B2_CREO ]
    BATCH = [ B2_CREO ]
    