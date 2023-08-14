
fullTableListCM = {
    ## >> #1 Custom SQL Loads 
    'CM_PRESENTMENTREQUEST_HIST':{"sql":'COPY_CM_PRESENTMENTREQUEST.sql', "has_date_entered":True},
    'CM_OPENENDLOANSTATEMENT_HIST':{"sql":'COPY_CM_OPENENDLOANSTATEMENT.sql', "has_date_entered":True},
    'CM_INSURANCEENROLLMENT_HIST':{"sql":'COPY_CM_INSURANCEENROLLMENT.sql', "has_date_entered":True},
    'CM_LOANFUNDING_HIST':{"sql":'COPY_CM_LOANFUNDING.sql', "has_date_entered":True},
    'CM_OFFERACCEPTED_HIST':{"sql":'COPY_CM_OFFERACCEPTED.sql', "has_date_entered":True},
    'CM_AVAILABLEOFFER_HIST':{"sql":'COPY_CM_AVAILABLEOFFER.sql', "has_date_entered":True},
    ## << #1 Custom SQL Loads 
    
    ## >> #2 Incremental load using Date_Entered
    'CM_TRANSDETAIL_HIST':{"sql":None, "has_date_entered":True},
    'CM_ACH_HISTORY_HIST':{"sql":None, "has_date_entered":True},    
    'CM_COMPANY_HIST':{"sql":None, "has_date_entered":True},
    'CM_CREDITCARDATTEMPTS_HIST':{"sql":None, "has_date_entered":True},
    'CM_CREDITCARDRESULTCODE_HIST':{"sql":None, "has_date_entered":True},
    'CM_CREDITCARDTRANS_HIST':{"sql":None, "has_date_entered":True},
    'CM_CUSTOMER_HIST':{"sql":None, "has_date_entered":True},
    'CM_CUSTOMERADDRESS_HIST':{"sql":None, "has_date_entered":True},
    'CM_CUSTOMERINCOME_HIST':{"sql":None, "has_date_entered":True},
    'CM_LOANAPPLICATION_HIST':{"sql":None, "has_date_entered":True},    
    'CM_LOANPAYMENT_HIST':{"sql":None, "has_date_entered":True},
    'CM_LOANPRODUCT_HIST':{"sql":None, "has_date_entered":True},
    'CM_LOANPRODUCTCONFIG_HIST':{"sql":None, "has_date_entered":True},
    'CM_MPAYINTEREST_HIST':{"sql":None, "has_date_entered":True},
    'CM_OPENENDINTEREST_HIST':{"sql":None, "has_date_entered":True},   
    'CM_PRESENTMENT_HIST':{"sql":None, "has_date_entered":True},    
    'CM_PROMISETOPAY_HIST':{"sql":None, "has_date_entered":True},
    'CM_RBCEFUNDBATCH_HIST':{"sql":None, "has_date_entered":True},
    'CM_SPAYINTEREST_HIST':{"sql":None, "has_date_entered":True},
    'CM_SERVICETRANS_HIST':{"sql":None, "has_date_entered":True},
    'CM_SERVICEDETAIL_HIST':{"sql":None, "has_date_entered":True},
    'CM_CASHEDCHECK_HIST':{"sql":None, "has_date_entered":True}, 
    # << #2 Incremental load using Date_Entered
    
    ## >> #3 Full Loads 
    'CM_BALSHEET2_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_ENDOFDAYRPT_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_LOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_CAPSRUN_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_CREDITCARDVENDOR_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_CURRENCY_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_DISTRICT_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_ACH_SENT_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_LOANFUNDINGSTATUS_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_LOANPRODUCTFINANCIALGROUP_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_MARKETS_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_PRESENTMENTCREDITCARDTRANSXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":None},    
    'CM_PRESENTMENTTYPE_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_PROMISETOPAYDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_REGION_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_OPENENDLOANSTATEMENTBALANCE_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_TELLERID_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    'CM_SECURITYGROUPHISTORY_HIST':{"sql":None, "has_date_entered":False, "key_column":None},
    
    ## << #3 Full Loads 
    
    ## >> #4 Incremental load using key   
    'CM_BALSHEET_TRANSDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_KEY"},
    'CM_ENDOFDAYINVENTORYDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_KEY"},
    'CM_LOANINCOME_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_INCOME_KEY"},
    'CM_LOANPAYMENTMPAY_HIST':{"sql":None, "has_date_entered":False, "key_column":"loan_payment_mpay_key"},
    'CM_LOANPAYMENTOPENEND_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_OPEN_END_KEY"},
    'CM_LOANPAYMENTSPAY_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_SPAY_KEY"},
    'CM_MPAYAMORT_HIST':{"sql":None, "has_date_entered":False, "key_column":"MPAY_AMORT_KEY"},
    'CM_MPAYLOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":"MPAY_LOAN_KEY"},    
    'CM_OPENENDLOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":"OPEN_END_LOAN_KEY"},
    'CM_PRESENTMENTREQUESTACHHISTORYXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_HISTORY_KEY"},
    'CM_TRANSCODE_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_CODE_KEY"},
    'CM_RBCEFUNDBATCHDETAIL_HIST':{"sql":None, "has_date_entered":False, "key_column":"RBC_EFUND_BATCH_DETAIL_KEY"},
    'CM_RISREPT_HIST':{"sql":None, "has_date_entered":False, "key_column":"RISREPT_KEY"},
    'CM_SPAYLOAN_HIST':{"sql":None, "has_date_entered":False, "key_column":"SPAY_LOAN_KEY"},
    'CM_TRANSDETAILACCT_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"}
    # ## << #4 Incremental load using key   
    
    ## >> #5 Decommissioned
    #'CM_CAPSCCTXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":None},   # Zero recrods in Winchk
    #'CM_CAPSHOLD_HIST':{"sql":None, "has_date_entered":False, "key_column":None},      # Zero recrods in Winchk
    #'CM_CAPSSKIPREASON_HIST':{"sql":None, "has_date_entered":False, "key_column":None},# One time backfill
}


fullTableListLD = {
    ## >> #1 Custom SQL Loads 
    'LD_InsuranceEnrollment_HIST':{"sql":'COPY_LD_INSURANCEENROLLMENT.sql',"has_date_entered":True}
    ,'LD_LoanFunding_HIST':{"sql":'COPY_LD_LOANFUNDING.sql',"has_date_entered":True}
    ,'LD_PresentmentRequest_HIST':{"sql":'COPY_LD_PRESENTMENTREQUEST.sql', "has_date_entered":True}
    ,'LD_OpenEndLoanStatement_HIST':{"sql":'COPY_LD_OPENENDLOANSTATEMENT.sql', "has_date_entered":True}
    ,'LD_OfferAccepted_HIST':{"sql":'COPY_LD_OFFERACCEPTED.sql', "has_date_entered":True}
    ,'LD_AvailableOffer_HIST':{"sql":'COPY_LD_AVAILABLEOFFER.sql', "has_date_entered":True}
    ## << #1 Custom SQL Loads 
    
    ## >> #2 Incremental load using Date_Entered    
    ,'LD_TransDetail_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_OpenEndInterest_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_MPayInterest_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_Presentment_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CustomerIncome_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_RBCEFundBatch_HIST':{"sql":None, "has_date_entered":True} 
    ,'LD_PromiseToPay_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CreditCardResultCode_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanProductConfig_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanProduct_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanPayment_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_LoanApplication_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_SPayInterest_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_Customer_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CustomerAddress_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CreditCardAttempts_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CreditCardTrans_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_ACH_History_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_SERVICETRANS_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_SERVICEDETAIL_HIST':{"sql":None, "has_date_entered":True}
    ,'LD_CASHEDCHECK_HIST':{"sql":None, "has_date_entered":True}
    
    ## << #2 Incremental load using Date_Entered    

    ## >> #3 Full Loads 
    ,'LD_BalSheet2_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_EndOfDayRpt_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Company_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Loan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_LoanIncome_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_PromiseToPayDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_OpenEndLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_RISREPT_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_ACH_Sent_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_MPayLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Transcode_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_CapsSkipReason_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_LoanProductFinancialGroup_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    ,'LD_LoanFundingStatus_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_PresentmentType_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_District_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Region_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Currency_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_Markets_HIST':{"sql":None, "has_date_entered":False, "key_column":None}        
    ,'LD_LoanPaymentMPay_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_MPayAmort_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_RBCEFundBatchDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_PresentmentRequestACHHistoryXREF_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    ,'LD_ACHBank_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_CreditCardVendor_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_OpenEndLoanStatementBalance_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_TellerID_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    ,'LD_SecurityGroupHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    
    ## << #3 Full Loads    

    ## >> #4 Incremental load using key  
    ,'LD_BalSheet_TransDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_KEY"}    
    ,'LD_LoanPaymentOpenEnd_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_OPEN_END_KEY"}    
    ,'LD_TRANSDETAILACCT_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"}
    ,'LD_EndOfDayInventoryDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_KEY"}
    
    ## << #4 Incremental load using key      
    
    ## >> #5 Decommissioned
    # ,'LD_CapsCCTXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    # ,'LD_CapsRun_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    # ,'LD_SPayLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    # ,'LD_PresentmentCreditCardTransXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
    # ,'LD_LoanPaymentSPay_HIST':{"sql":None, "has_date_entered":False, "key_column":None}    
    # ,'LD_CapsHold_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
}


fullTableListVC = {
    #These tables all have the date_entered field
    'VC_ACH_History_HIST':{"sql":None, "has_date_entered":True},
    'VC_AutoReportEditHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_BalSheet2_HIST':{"sql":None, "has_date_entered":True},
    'VC_BankruptcyTrustee_HIST':{"sql":None, "has_date_entered":True},
    'VC_CashedCheck_HIST':{"sql":None, "has_date_entered":True},
    'VC_CCardResponses_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollBonusPTP_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionAction_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionMovement_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_CollectionStream_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardAttempts_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardBlock_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCards_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardsEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditCardTrans_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingBaseSegment_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingBaseSegmentHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingLoanActivity_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingLoanDisputeNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingProcessingQueue_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditReportingProcessingQueueHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_CreditVendorData_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerAddress_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerAddressEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerEmployerEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerPhoneNumber_HIST':{"sql":None, "has_date_entered":True},
    'VC_CustomerPhoneNumberEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_DocuwareDocument_HIST':{"sql":None, "has_date_entered":True},
    'VC_DocuwareID_HIST':{"sql":None, "has_date_entered":True},
    'VC_DocuwareVisitorEmailAttachmentXref_HIST':{"sql":None, "has_date_entered":True},
    'VC_DrawerZ_HIST':{"sql":None, "has_date_entered":True},
    'VC_EndOfDayRpt_HIST':{"sql":None, "has_date_entered":True},
    'VC_ExcludeFromCapsHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_IssuerEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanChkAcctChange_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanDepositStatusHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanDueDateChange_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanNote_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPayment_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPaymentCheckPaymentTypeXref_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPaymentRefund_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPaymentStaged_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanPayoffDate_HIST':{"sql":None, "has_date_entered":True},
    'VC_LoanStatusChange_HIST':{"sql":None, "has_date_entered":True},
    'VC_LocationUS_ZipcodesXRef_HIST':{"sql":None, "has_date_entered":True},
    'VC_MPayInterest_HIST':{"sql":None, "has_date_entered":True},
    'VC_MPayLoanInSyncAdj_HIST':{"sql":None, "has_date_entered":True},
    'VC_MPayRecalcLoanPaymentAdj_HIST':{"sql":None, "has_date_entered":True},
    'VC_OverShort_HIST':{"sql":None, "has_date_entered":True},
    'VC_PaymentsPastDue_HIST':{"sql":None, "has_date_entered":True},
    'VC_Presentment_HIST':{"sql":None, "has_date_entered":True},
    'VC_PresentmentRequest_HIST':{"sql":None, "has_date_entered":True},
    'VC_PresentmentRequestNotSent_HIST':{"sql":None, "has_date_entered":True},
    'VC_PromiseToPay_HIST':{"sql":None, "has_date_entered":True},
    'VC_PromiseToPayCommunication_HIST':{"sql":None, "has_date_entered":True},
    'VC_PromiseToPayDetailEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_Receipt_HIST':{"sql":None, "has_date_entered":True},
    'VC_TellerIDEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_TellerPwdHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_TellerSecurity_HIST':{"sql":None, "has_date_entered":True},
    'VC_TransDetail_HIST':{"sql":None, "has_date_entered":True},
    'VC_TransPOS_HIST':{"sql":None, "has_date_entered":True},
    'VC_VaultCount_HIST':{"sql":None, "has_date_entered":True},
    'VC_VaultRecalcAdj_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorAuthenticationCode_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorAuthenticationCodeAttempt_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorDevice_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorDocument_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorEdit_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorEmail_HIST':{"sql":None, "has_date_entered":True},
    'VC_VisitorPasswordHistory_HIST':{"sql":None, "has_date_entered":True},
    'VC_WebPixelVendorData_HIST':{"sql":None, "has_date_entered":True},

    #These tables do not have the date_entered field and must be loaded using another strategy
    'VC_ACH_Recv_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_RECV_KEY"},
    'VC_ACH_Sent_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_SENT_KEY"},
    'VC_ACHSentParent_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_SENT_PARENT_KEY"},
    'VC_BalSheet_TransDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"BALSHEET_KEY"},
    'VC_BalSheetColumns2_HIST':{"sql":None, "has_date_entered":False, "key_column":"BSC_KEY"},
    'VC_BankAccount_HIST':{"sql":None, "has_date_entered":False, "key_column":"Bank_account_KEY"},
    'VC_BatchExecution_HIST':{"sql":None, "has_date_entered":False, "key_column":"Batch_EXECUTION_KEY"},
    'VC_CapsRun_HIST':{"sql":None, "has_date_entered":False, "key_column":"CAPS_RUN_KEY"},
    'VC_CollectionAgingItem_HIST':{"sql":None, "has_date_entered":False, "key_column":"COLLECTION_AGING_CONFIG_DAYS_KEY"},
    'VC_CreditReportingLoanDispute_HIST':{"sql":None, "has_date_entered":False, "key_column":"CREDIT_REPORTING_LOAN_DISPUTE_KEY"},
    'VC_CreditReportingLoanStatusHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_KEY"},
    'VC_CreditReportingRun_HIST':{"sql":None, "has_date_entered":False, "key_column":"CREDIT_REPORTING_RUN_KEY"},
    'VC_DepositChk_HIST':{"sql":None, "has_date_entered":False, "key_column":"DEPOSIT_CHK_KEY"},
    'VC_DepositChkDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"DEPOSIT_CHK_DETAIL_KEY"},
    'VC_DialerKeys_HIST':{"sql":None, "has_date_entered":False, "key_column":"DIALER_KEYS_KEY"},
    'VC_DocuwareLoanDoc_HIST':{"sql":None, "has_date_entered":False, "key_column":"DOCUWARE_LOAN_DOC_KEY"},
    'VC_DocuwareStatus_HIST':{"sql":None, "has_date_entered":False, "key_column":"DocuwareStatus_KEY"},
    'VC_DocuwareVisitorDocXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":"VISITOR_DOCUMENT_KEY"},
    'VC_DrawerMaster_HIST':{"sql":None, "has_date_entered":False, "key_column":"Drawer_key"},
    'VC_DrawerZCash_HIST':{"sql":None, "has_date_entered":False, "key_column":"DRAWERZ_CASH_KEY"},
    'VC_EndOfDayInventoryDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_KEY"},
    'VC_EndOfDayRptDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"EODR_DET_KEY"},
    'VC_FormLetterBatch_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_BATCH_KEY"},
    'VC_FormLetterBatchVendorFile_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_BATCH_BUILD_VENDOR_FILE_KEY"},
    'VC_FormLetterPrinted_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_PRINTED_KEY"},
    'VC_FormLetterResult_HIST':{"sql":None, "has_date_entered":False, "key_column":"FORM_LETTER_RESULT_KEY"},
    'VC_GLAcct_HIST':{"sql":None, "has_date_entered":False, "key_column":"GL_ACCT_KEY"},   
    'VC_Issuer_HIST':{"sql":None, "has_date_entered":False, "key_column":"ISSUER_KEY"},
    'VC_LoanAuthorizedPaymentMethod_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_AUTHORIZED_PAYMENT_METHOD_KEY"},
    'VC_LoanFundingHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_FUNDING_HISTORY_KEY"},
    'VC_LoanFundingHistoryDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_FUNDING_HISTORY_DETAIL_KEY"},
    'VC_LoanPaymentDecreaseAmountOwed_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_DECREASE_AMOUNT_OWED_KEY"},
    'VC_LoanPaymentDueDate_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_DUE_DATE_KEY"},
    'VC_LoanPaymentMPay_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_MPAY_KEY"},
    'VC_LoanPaymentSuspendInterest_HIST':{"sql":None, "has_date_entered":False, "key_column":"LOAN_PAYMENT_SUSPEND_INTEREST_KEY"},
    'VC_PaymentsPastDueDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"PAYMENTS_PAST_DUE_DETAIL_KEY"},
    'VC_PresentmentCreditCardTransXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":"CREDIT_CARD_TRANS_KEY"},
    'VC_PresentmentRequestACHHistoryXRef_HIST':{"sql":None, "has_date_entered":False, "key_column":"ACH_HISTORY_KEY"},
    'VC_PresentmentRequestNotSentReason_HIST':{"sql":None, "has_date_entered":False, "key_column":"PRESENTMENT_REQUEST_NOT_SENT_REASON_KEY"},
    'VC_PromiseToPayDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"PTP_DETAIL_KEY"},
    'VC_PromiseToPayDetailTrans_HIST':{"sql":None, "has_date_entered":False, "key_column":"PTP_DETAIL_TRANS_KEY"},
    'VC_RISREPT_HIST':{"sql":None, "has_date_entered":False, "key_column":"RISREPT_KEY"},
    'VC_RisReptDoNotContact_HIST':{"sql":None, "has_date_entered":False, "key_column":"RIS_REPT_DO_NOT_CONTACT_KEY"},
    'VC_Store_Windows_HIST':{"sql":None, "has_date_entered":False, "key_column":"Store_Windows_Key"},
    'VC_TELLERID_HIST':{"sql":None, "has_date_entered":False, "key_column":"TELLER_ID_KEY"},
    'VC_TellerLogin_HIST':{"sql":None, "has_date_entered":False, "key_column":"TELLER_LOGIN_KEY"},
    'VC_TransDetailAcct_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_ACCT_KEY"},
    'VC_TransDetailCash_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_CASH_KEY"},
    'VC_TransDetailCashParsedCash_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_CASH_PARSED_CASH_KEY"},
    'VC_TransDetailCheck_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_CHECK_KEY"},
    'VC_TransDetailLoan_HIST':{"sql":None, "has_date_entered":False, "key_column":"TRANS_DETAIL_LOAN_KEY"},
    'VC_US_Zipcodes_HIST':{"sql":None, "has_date_entered":False, "key_column":"ZIPCODE"},
    'VC_VaultMgrAuthorization_HIST':{"sql":None, "has_date_entered":False, "key_column":"VM_AUTH_KEY"},
    'VC_VaultMgrAuthorizationDetail_HIST':{"sql":None, "has_date_entered":False, "key_column":"VM_AUTH_DETAIL_KEY"},
    'VC_VisitorCommunicationPreference_HIST':{"sql":None, "has_date_entered":False, "key_column":"VISITOR_COMMUNICATION_PREFERENCE_KEY"},
    'VC_VisitorEmailDisposition_HIST':{"sql":None, "has_date_entered":False, "key_column":"VISITOR_EMAIL_DISPOSITION_KEY"},
    'VC_WebCallCenterLogin_HIST':{"sql":None, "has_date_entered":False, "key_column":"CallCenter_Login_Key"},
    'VC_WebCallQueue_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_QUEUE_KEY"},
    'VC_WebCallQueueAudit_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_QUEUE_AUDIT_KEY"},
    'VC_WebCallRARRHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_RARR_HISTORY_KEY"},
    'VC_WebCallUserSetting_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_USER_SETTING_KEY"},
    'VC_WebCallWorkItemCategoryHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_WORK_ITEM_CATEGORY_HISTORY_KEY"},
    'VC_WebCallWorkQueue_HIST':{"sql":None, "has_date_entered":False, "key_column":"WEB_CALL_WORK_QUEUE_KEY"},

    #These tables will be loaded via a full snapshot for every day with an additional view created from the table where only the most recent days worth of data is included
    'VC_GlobalHistory_HIST':{"sql":None, "has_date_entered":False, "key_column":None}
}



def main(fullTableList):
    keys_list = list(fullTableList.keys())
    table_names = []
    for key in keys_list:
        table_name = key.replace('CM_','').replace('VC_','').replace('LD_','').replace('_HIST','')
        table_names.append(table_name)
    print(f"{key[0:2]}_TABLE_LIST = {table_names}\n")
    return table_names

full_table_lists = [fullTableListCM, fullTableListLD, fullTableListVC]
for table_list in full_table_lists:
    main(table_list)

'''
CM_TABLE_LIST = ['PRESENTMENTREQUEST', 'OPENENDLOANSTATEMENT', 'INSURANCEENROLLMENT', 'LOANFUNDING', 'OFFERACCEPTED', 'AVAILABLEOFFER', 'TRANSDETAIL', 'ACHORY', 'COMPANY', 'CREDITCARDATTEMPTS', 'CREDITCARDRESULTCODE', 'CREDITCARDTRANS', 'CUSTOMER', 'CUSTOMERADDRESS', 'CUSTOMERINCOME', 'LOANAPPLICATION', 'LOANPAYMENT', 'LOANPRODUCT', 'LOANPRODUCTCONFIG', 'MPAYINTEREST', 'OPENENDINTEREST', 'PRESENTMENT', 'PROMISETOPAY', 'RBCEFUNDBATCH', 'SPAYINTEREST', 'SERVICETRANS', 'SERVICEDETAIL', 'CASHEDCHECK', 'BALSHEET2', 'ENDOFDAYRPT', 'LOAN', 'CAPSRUN', 'CREDITCARDVENDOR', 'CURRENCY', 'DISTRICT', 'ACH_SENT', 'LOANFUNDINGSTATUS', 'LOANPRODUCTFINANCIALGROUP', 'MARKETS', 'PRESENTMENTCREDITCARDTRANSXREF', 'PRESENTMENTTYPE', 'PROMISETOPAYDETAIL', 'REGION', 'OPENENDLOANSTATEMENTBALANCE', 'TELLERID', 'SECURITYGROUPHISTORY', 'BALSHEET_TRANSDETAIL', 'ENDOFDAYINVENTORYDETAIL', 'LOANINCOME', 'LOANPAYMENTMPAY', 'LOANPAYMENTOPENEND', 'LOANPAYMENTSPAY', 'MPAYAMORT', 'MPAYLOAN', 'OPENENDLOAN', 'PRESENTMENTREQUESTACHHISTORYXREF', 'TRANSCODE', 'RBCEFUNDBATCHDETAIL', 'RISREPT', 'SPAYLOAN', 'TRANSDETAILACCT']

LD_TABLE_LIST = ['InsuranceEnrollment', 'LoanFunding', 'PresentmentRequest', 'OpenEndLoanStatement', 'OfferAccepted', 'AvailableOffer', 'TransDetail', 'OpenEndInterest', 'MPayInterest', 'Presentment', 'CustomerIncome', 'RBCEFundBatch', 'PromiseToPay', 'CreditCardResultCode', 'LoanProductConfig', 'LoanProduct', 'LoanPayment', 'LoanApplication', 'SPayInterest', 'Customer', 'CustomerAddress', 'CreditCardAttempts', 'CreditCardTrans', 'ACH_History', 'SERVICETRANS', 'SERVICEDETAIL', 'CASHEDCHECK', 'BalSheet2', 'EndOfDayRpt', 'Company', 'Loan', 'LoanIncome', 'PromiseToPayDetail', 'OpenEndLoan', 'RISREPT', 'ACH_Sent', 'MPayLoan', 'Transcode', 'CapsSkipReason', 'LoanProductFinancialGroup', 'LoanFundingStatus', 'PresentmentType', 'District', 'Region', 'Currency', 'Markets', 'LoanPaymentMPay', 'MPayAmort', 'RBCEFundBatchDetail', 'PresentmentRequestACHHistoryXREF', 'ACHBank', 'CreditCardVendor', 'OpenEndLoanStatementBalance', 'TellerID', 'SecurityGroupHistory', 'BalSheet_TransDetail', 'LoanPaymentOpenEnd', 'TRANSDETAILACCT', 'EndOfDayInventoryDetail']

VC_TABLE_LIST = ['ACH_History', 'AutoReportEditHistory', 'BalSheet2', 'BankruptcyTrustee', 'CashedCheck', 'CCardResponses', 'CollBonusPTP', 'CollectionAction', 'CollectionMovement', 'CollectionNote', 'CollectionStream', 'CreditCardAttempts', 'CreditCardBlock', 'CreditCards', 'CreditCardsEdit', 'CreditCardTrans', 'CreditReportingBaseSegment', 'CreditReportingBaseSegmentHistory', 'CreditReportingLoanActivity', 'CreditReportingLoanDisputeNote', 'CreditReportingProcessingQueue', 'CreditReportingProcessingQueueHistory', 'CreditVendorData', 'CustomerAddress', 'CustomerAddressEdit', 'CustomerEdit', 'CustomerEmployerEdit', 'CustomerNote', 'CustomerPhoneNumber', 'CustomerPhoneNumberEdit', 'DocuwareDocument', 'DocuwareID', 'DocuwareVisitorEmailAttachmentXref', 'DrawerZ', 'EndOfDayRpt', 'ExcludeFromCapsHistory', 'IssuerEdit', 'LoanChkAcctChange', 'LoanDepositStatusHistory', 'LoanDueDateChange', 'LoanNote', 'LoanPayment', 'LoanPaymentCheckPaymentTypeXref', 'LoanPaymentRefund', 'LoanPaymentStaged', 'LoanPayoffDate', 'LoanStatusChange', 'LocationUS_ZipcodesXRef', 'MPayInterest', 'MPayLoanInSyncAdj', 'MPayRecalcLoanPaymentAdj', 'OverShort', 'PaymentsPastDue', 'Presentment', 'PresentmentRequest', 'PresentmentRequestNotSent', 'PromiseToPay', 'PromiseToPayCommunication', 'PromiseToPayDetailEdit', 'Receipt', 'TellerIDEdit', 'TellerPwdHistory', 'TellerSecurity', 'TransDetail', 'TransPOS', 'VaultCount', 'VaultRecalcAdj', 'VisitorAuthenticationCode', 'VisitorAuthenticationCodeAttempt', 'VisitorDevice', 'VisitorDocument', 'VisitorEdit', 'VisitorEmail', 'VisitorPasswordHistory', 'WebPixelVendorData', 'ACH_Recv', 'ACH_Sent', 'ACHSentParent', 'BalSheet_TransDetail', 'BalSheetColumns2', 'BankAccount', 'BatchExecution', 'CapsRun', 'CollectionAgingItem', 'CreditReportingLoanDispute', 'CreditReportingLoanStatusHistory', 'CreditReportingRun', 'DepositChk', 'DepositChkDetail', 'DialerKeys', 'DocuwareLoanDoc', 'DocuwareStatus', 'DocuwareVisitorDocXRef', 'DrawerMaster', 'DrawerZCash', 'EndOfDayInventoryDetail', 'EndOfDayRptDetail', 'FormLetterBatch', 'FormLetterBatchVendorFile', 'FormLetterPrinted', 'FormLetterResult', 'GLAcct', 'Issuer', 'LoanAuthorizedPaymentMethod', 'LoanFundingHistory', 'LoanFundingHistoryDetail', 'LoanPaymentDecreaseAmountOwed', 'LoanPaymentDueDate', 'LoanPaymentMPay', 'LoanPaymentSuspendInterest', 'PaymentsPastDueDetail', 'PresentmentCreditCardTransXRef', 'PresentmentRequestACHHistoryXRef', 'PresentmentRequestNotSentReason', 'PromiseToPayDetail', 'PromiseToPayDetailTrans', 'RISREPT', 'RisReptDoNotContact', 'Store_Windows', 'TELLERID', 'TellerLogin', 'TransDetailAcct', 'TransDetailCash', 'TransDetailCashParsedCash', 'TransDetailCheck', 'TransDetailLoan', 'US_Zipcodes', 'VaultMgrAuthorization', 'VaultMgrAuthorizationDetail', 'VisitorCommunicationPreference', 'VisitorEmailDisposition', 'WebCallCenterLogin', 'WebCallQueue', 'WebCallQueueAudit', 'WebCallRARRHistory', 'WebCallUserSetting', 'WebCallWorkItemCategoryHistory', 'WebCallWorkQueue', 'GlobalHistory']
'''