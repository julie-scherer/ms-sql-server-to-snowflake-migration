COPY INTO STG.VC_WEBREFERRALMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
	END,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebReferralMethod/WebReferralMethod_Backfill.csv*';

COPY INTO STG.VC_WEBPIXELVENDORDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    $14,$15
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebPixelVendorDetail/WebPixelVendorDetail_Backfill.csv*';

COPY INTO STG.VC_WEBDIALERSTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerStatus/WebDialerStatus_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARRTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARRType/WebCallRARRType_Backfill.csv*';

COPY INTO STG.VC_WEBCALLQUICKNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,$2,$3,$4,$5,$6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallQuickNote/WebCallQuickNote_Backfill.csv*';

COPY INTO STG.VC_WEBCALLQUEUETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
	END,
    $4,
    TRY_TO_TIMESTAMP($5)
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallQueueType/WebCallQueueType_Backfill.csv*';

COPY INTO STG.VC_WEBCALLLOGGINGCATEGORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallLoggingCategory/WebCallLoggingCategory_Backfill.csv*';


COPY INTO STG.VC_WEBCALLEMAILTEMPLATEATTACHMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,$2,$3,$4,
    TRY_TO_TIMESTAMP($5),
    $6,
    TRY_TO_TIMESTAMP($7),
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallEmailTemplateAttachment/WebCallEmailTemplateAttachment_Backfill.csv*';

COPY INTO STG.VC_WEBCALLCAMPAIGN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallCampaign/WebCallCampaign_Backfill.csv*';

COPY INTO STG.VC_WEBCALLCSRREPORTCOLUMN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallCSRReportColumn/WebCallCSRReportColumn_Backfill.csv*';

COPY INTO STG.VC_VISITORDEVICETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorDeviceType/VisitorDeviceType_Backfill.csv*';


COPY INTO STG.VC_VISITORAUTHENTICATIONCODECONFIGCHANNEL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
	END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorAuthenticationCodeConfigChannel/VisitorAuthenticationCodeConfigChannel_Backfill.csv*';

COPY INTO STG.VC_VARIABLERATETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VariableRateType/VariableRateType_Backfill.csv*';

COPY INTO STG.VC_SPECIALPURCHASEVEHICLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SpecialPurchaseVehicle/SpecialPurchaseVehicle_Backfill.csv*';


COPY INTO STG.VC_SECURITYQUESTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    TRY_TO_TIMESTAMP($3),
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
	END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SecurityQuestion/SecurityQuestion_Backfill.csv*';


COPY INTO STG.VC_RISREPTDONOTCONTACTREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
	END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RisReptDoNotContactReason/RisReptDoNotContactReason_Backfill.csv*';

COPY INTO STG.VC_RIURGENTNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
	END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RiUrgentNote/RiUrgentNote_Backfill.csv*';


COPY INTO STG.VC_REGION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Region/Region_Backfill.csv*';


COPY INTO STG.VC_RACE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Race/Race_Backfill.csv*';


COPY INTO STG.VC_PROMISETOPAYTIMESLOTCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PromiseToPayTimeSlotConfig/PromiseToPayTimeSlotConfig_Backfill.csv*';


COPY INTO STG.VC_PROCESSCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    try_to_timestamp($7)
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ProcessConfig/ProcessConfig_Backfill.csv*';


COPY INTO STG.VC_PRESENTMENTTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentType/PresentmentType_Backfill.csv*';


COPY INTO STG.VC_PRESENTMENTPAYMENTMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentPaymentMethod/PresentmentPaymentMethod_Backfill.csv*';


COPY INTO STG.VC_PHOTOIDTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PhotoIdType/PhotoIdType_Backfill.csv*';


COPY INTO STG.VC_PERSONTITLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'), 
    $6, 
    to_timestamp_ntz($7,'MM/DD/YYYY HH12:MI:SS AM'), 
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PersonTitle/PersonTitle_Backfill.csv*';


COPY INTO STG.VC_PAYMENTMETHODPROVISIONALAPPROVALTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentMethodProvisionalApprovalType/PaymentMethodProvisionalApprovalType_Backfill.csv*';


COPY INTO STG.VC_PTPPAYMENTPLANCHECK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanCheck/PTPPaymentPlanCheck_Backfill.csv*';


COPY INTO STG.VC_PTPPAYMENTMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentMethod/PTPPaymentMethod_Backfill.csv*';


COPY INTO STG.VC_OPTPLUSCARRIER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    TRY_TO_TIMESTAMP($6),
    $7,
    TRY_TO_TIMESTAMP($8)
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/OptPlusCarrier/OptPlusCarrier_Backfill.csv*';


COPY INTO STG.VC_NOBLECONFIGURATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    TRY_TO_NUMBER($6),
    $7,
    $8,
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/NobleConfiguration/NobleConfiguration_Backfill.csv*';

COPY INTO STG.VC_MESSAGECLASS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MessageClass/MessageClass_Backfill.csv*';


COPY INTO STG.VC_LOCATIONUS_ZIPCODESXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
	END,
    TRY_TO_TIMESTAMP($5),
    $6,
    TRY_TO_TIMESTAMP($7),
    $8
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LocationUS_ZipcodesXRef/LocationUS_ZipcodesXRef_Backfill.csv*';


COPY INTO STG.VC_LOANPAYMENTSTAGEDSTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentStagedStatus/LoanPaymentStagedStatus_Backfill.csv*';


COPY INTO STG.VC_LOANPAYMENTSTAGEDNOTSENTREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentStagedNotSentReason/LoanPaymentStagedNotSentReason_Backfill.csv*';


COPY INTO STG.VC_LENDER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    TRY_TO_TIMESTAMP($6),
    $7,
    TRY_TO_TIMESTAMP($8),
    $9
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Lender/Lender_Backfill.csv*';


COPY INTO STG.VC_LPPTRANSCODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LPPTransCode/LPPTransCode_Backfill.csv*';


COPY INTO STG.VC_INVALIDSTREET_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/InvalidStreet/InvalidStreet_Backfill.csv*';


COPY INTO STG.VC_INSURANCECLAIMREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/InsuranceClaimReason/InsuranceClaimReason_Backfill.csv*';

COPY INTO STG.VC_LOANTRANSFERSOURCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanTransferSource/LoanTransferSource_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERLOANHISTORYSTATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterLoanHistoryState/FormLetterLoanHistoryState_Backfill.csv*';

COPY INTO STG.VC_PRODUCTSPONSOR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ProductSponsor/ProductSponsor_Backfill.csv*';

COPY INTO STG.VC_DISTRICT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    TRY_TO_NUMBER($4),
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/District/District_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGNATURALDISASTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $4 = '' THEN NULL 
    ELSE $4
    END,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingNaturalDisaster/CreditReportingNaturalDisaster_Backfill.csv*';

COPY INTO STG.VC_COLLECTIONAGINGCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionAgingConfig/CollectionAgingConfig_Backfill.csv*';

COPY INTO STG.VC_WEBPIXELVENDORREASONPULLED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebPixelVendorReasonPulled/WebPixelVendorReasonPulled_Backfill.csv*';

COPY INTO STG.VC_WEBCALLQUEUESTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallQueueStatus/WebCallQueueStatus_Backfill.csv*';

COPY INTO STG.VC_WEBPIXELVENDORREASONPULLED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerType/TellerType_Backfill.csv*';

COPY INTO STG.VC_PTPPAYMENTPLANTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanType/PTPPaymentPlanType_Backfill.csv*';

COPY INTO STG.VC_PTPPAYMENTPLANPAYMENTSCHEDULE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanPaymentSchedule/PTPPaymentPlanPaymentSchedule_Backfill.csv*';

COPY INTO STG.VC_PAYMENTAUTHORIZATIONDOCUMENTREQUIREMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentAuthorizationDocumentRequirement/PaymentAuthorizationDocumentRequirement_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGRULETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingRuleType/CreditReportingRuleType_Backfill.csv*';

COPY INTO STG.VC_CREDITCARDRESULTCODETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardResultCodeType/CreditCardResultCodeType_Backfill.csv*';

COPY INTO STG.VC_CONFIGURABLEQUESTIONCATEGORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ConfigurableQuestionCategory/ConfigurableQuestionCategory_Backfill.csv*';

COPY INTO STG.VC_CFPBNOTIFICATIONTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CFPBNotificationType/CFPBNotificationType_Backfill.csv*';

COPY INTO STG.VC_TELLERTITLE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerTitle/TellerTitle_Backfill.csv*';

COPY INTO STG.VC_EXPENSETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ExpenseType/ExpenseType_Backfill.csv*';

COPY INTO STG.VC_WEBCALLQUEUECONFIGURATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallQueueConfiguration/WebCallQueueConfiguration_Backfill.csv*';

COPY INTO STG.VC_ACHGROUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHGroup/ACHGroup_Backfill.csv*';

COPY INTO STG.VC_PTPPAYMENTPLANDUEDATECHANGETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanDueDateChangeType/PTPPaymentPlanDueDateChangeType_Backfill.csv*';

COPY INTO STG.VC_PRESCREENQUESTIONTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PrescreenQuestionType/PrescreenQuestionType_Backfill.csv*';

COPY INTO STG.VC_PAYMENTMETHODDEAUTHORIZATIONREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentMethodDeauthorizationReason/PaymentMethodDeauthorizationReason_Backfill.csv*';

COPY INTO STG.VC_INTERNALPROCESSEMAILTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/InternalProcessEmailType/InternalProcessEmailType_Backfill.csv*';

COPY INTO STG.VC_LOCALESETTING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LocaleSetting/LocaleSetting_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERAFTERLETTERXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterAfterLetterXRef/FormLetterAfterLetterXRef_Backfill.csv*';


COPY INTO STG.VC_ACH_RETURNCODEHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_ReturnCodeHistory/ACH_ReturnCodeHistory_Backfill.csv*';

COPY INTO STG.VC_PENDINGREASONRESOLVEREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PendingReasonResolveReason/PendingReasonResolveReason_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGRARRACTIVITYXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingRarrActivityXRef/CreditReportingRarrActivityXRef_Backfill.csv*';

COPY INTO STG.VC_LOANADJUSTMENTBATCHSETTINGS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($10,'MM/DD/YYYY HH12:MI:SS AM'),
    $11,
    $12,
    $13,
    $14
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanAdjustmentBatchSettings/LoanAdjustmentBatchSettings_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGRULE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    $5
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingRule/CreditReportingRule_Backfill.csv*';

COPY INTO STG.VC_RULEDEFTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    try_to_timestamp($6),
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RuleDefType/RuleDefType_Backfill.csv*';

COPY INTO STG.VC_TRANSACTIONNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    try_to_timestamp($8),
    $9,
    to_timestamp_ntz($10,'MM/DD/YYYY HH12:MI:SS AM'),
    $11,
    try_to_date($12)
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransactionNote/TransactionNote_Backfill.csv*';

COPY INTO STG.VC_INCOMESOURCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4,
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'),
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeSource/IncomeSource_Backfill.csv*';

COPY INTO STG.VC_AMLTHRESHOLDRULE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END, 
    $4, 
    $5, 
    $6, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $16 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($17,'MM/DD/YYYY HH12:MI:SS AM'), 
    $18, 
    try_to_timestamp($19), 
    $20, 
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END

  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AMLThresholdRule/AMLThresholdRule_Backfill.csv*';


COPY INTO STG.VC_LOANDEPOSITORDERHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    try_to_number($2),
    $3,
    $4, 
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'),
    $6, 
    $7, 
    $8, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDepositOrderHistory/LoanDepositOrderHistory_Backfill.csv*';

COPY INTO STG.VC_IDENTIFICATIONTYPEVERIFY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($4,'MM/DD/YYYY HH12:MI:SS AM'), 
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IdentificationTypeVerify/IdentificationTypeVerify_Backfill.csv*';


COPY INTO STG.VC_INSURANCESTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'),
    $6,
    try_to_timestamp($7),
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/InsuranceStatus/InsuranceStatus_Backfill.csv*';

COPY INTO STG.VC_CREDITVENDOR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    to_timestamp_ntz($2,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($3,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($4,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($6,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($13,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($14,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($16,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($17,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($19,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($22,'MM/DD/YYYY HH12:MI:SS AM'), 
    to_timestamp_ntz($23,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $25 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($26,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($29,'MM/DD/YYYY HH12:MI:SS AM'), 
    $30 

  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditVendor/CreditVendor_Backfill.csv*';

COPY INTO STG.VC_PREQUALAPPLICATIONINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    CASE WHEN $2 = '' THEN NULL
    ELSE 0
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE 0
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PrequalApplicationIncome/PrequalApplicationIncome_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGLOANPRODUCTLOCATIONHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingLoanProductLocationHistory/CreditReportingLoanProductLocationHistory_Backfill.csv*';


COPY INTO STG.VC_IDENTIFICATIONTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    $5, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($12,'MM/DD/YYYY HH12:MI:SS AM'),
    $13, 
    to_timestamp_ntz($14,'MM/DD/YYYY HH12:MI:SS AM'), 
    $15, 
    $16, 
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IdentificationType/IdentificationType_Backfill.csv*';

COPY INTO STG.VC_NOADVERSEACTIONLETTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $10,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $13,
    $14,
    $15 

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/NoAdverseActionLetter/NoAdverseActionLetter_Backfill.csv*';


COPY INTO STG.VC_LOANAUTHORIZEDPAYMENTMETHODPENDING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6, 
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanAuthorizedPaymentMethodPending/LoanAuthorizedPaymentMethodPending_Backfill.csv*';


COPY INTO STG.VC_VAULTMASTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END, 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END, 
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END, 
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END, 
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END, 
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END, 
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END, 
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END, 
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END, 
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END, 
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END, 
    $18, 
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END, 
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $21 = 'True' THEN 1
    ELSE 0
    END, 
    $22, 
    $23, 
    $24, 
    $25, 
    $26, 
    $27, 
    $28, 
    $29, 
    $30, 
    $31, 
    $32, 
    $33, 
    $34, 
    $35, 
    $36, 
    $37, 
    $38, 
    $39, 
    $40, 
    $41, 
    $42, 
    $43, 
    $44, 
    $45, 
    $46, 
    $47, 
    $48, 
    $49, 
    $50, 
    $51, 
    $52, 
    $53, 
    $54, 
    $55, 
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END, 
    CASE WHEN $57 = '' THEN NULL
    ELSE $57
    END, 
    CASE WHEN $58 = '' THEN NULL
    ELSE $58
    END, 
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VaultMaster/VaultMaster_Backfill.csv*';


COPY INTO STG.VC_GLACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GLAcct/GLAcct_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERONDEMAND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterOnDemand/FormLetterOnDemand_Backfill.csv*';

COPY INTO STG.VC_TASKACTIONRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TaskActionResult/TaskActionResult_Backfill.csv*';

COPY INTO STG.VC_OVERSHORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'),
    $4, 
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    $16,
    $17

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/OverShort/OverShort_Backfill.csv*';

COPY INTO STG.VC_CARDGOVERNORVALIDATIONLOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4, 
    $5,
    $6,
    $7, 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    $9, 
    try_to_timestamp($10)

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CardGovernorValidationLocation/CardGovernorValidationLocation_Backfill.csv*';

COPY INTO STG.VC_SERVICEMASTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    $13,
    try_to_timestamp($14),
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ServiceMaster/ServiceMaster_Backfill.csv*';

COPY INTO STG.VC_AUTOREPORT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    CASE WHEN $25 = '' THEN NULL 
    ELSE to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $26,
    $27,
    $28,
    CASE WHEN $29 = '' THEN NULL 
    ELSE to_timestamp_ntz($29, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $30 = '' THEN NULL 
    ELSE to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $31 = '' THEN NULL 
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $32

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AutoReport/AutoReport_Backfill.csv*';

COPY INTO STG.VC_LOANDOCUSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDocUsed/LoanDocUsed_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGRUN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingRun/CreditReportingRun_Backfill.csv*';

COPY INTO STG.VC_COLLECTIONSETTLEMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $5,
    try_to_timestamp($6),
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END, 
    try_to_timestamp($8), 
    $9, 
    $10,
    $11,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    try_to_timestamp($13),
    $14

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionSettlement/CollectionSettlement_Backfill.csv*';

COPY INTO STG.VC_REFINANCELOANAPPLICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RefinanceLoanApplication/RefinanceLoanApplication_Backfill.csv*';

COPY INTO STG.VC_ACHSENTPARENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    try_to_timestamp($11)
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHSentParent/ACHSentParent_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGPROCESSINGQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7, 
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingProcessingQueue/CreditReportingProcessingQueue_Backfill.csv*';

COPY INTO STG.VC_LOANTRANSFEROFFER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $9 = '' THEN NULL 
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanTransferOffer/LoanTransferOffer_Backfill.csv*';

COPY INTO STG.VC_CAPSRUN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CapsRun/CapsRun_Backfill.csv*';

COPY INTO STG.VC_LOANPAYMENTREFUND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    $8, 
    $9, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentRefund/LoanPaymentRefund_Backfill.csv*';

COPY INTO STG.VC_RISREPTDONOTCONTACT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RisReptDoNotContact/RisReptDoNotContact_Backfill.csv*';

COPY INTO STG.VC_BALSHEET2_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = '' THEN NULL 
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BalSheet2/BalSheet2_Backfill.csv*';

COPY INTO STG.VC_ACH_SENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    $12,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    $14,
    $15
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_Sent/ACH_Sent_Backfill.csv*';

COPY INTO STG.VC_LOANAPPLICATIONPENDINGREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    $9, 
    to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationPendingReason/LoanApplicationPendingReason_Backfill.csv*';

COPY INTO STG.VC_BATCHEXECUTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BatchExecution/BatchExecution_Backfill.csv*';

COPY INTO STG.VC_COLLECTIONSTREAM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionStream/CollectionStream_Backfill.csv*';

COPY INTO STG.VC_VISITORAUTHENTICATIONCODEATTEMPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorAuthenticationCodeAttempt/VisitorAuthenticationCodeAttempt_Backfill.csv*';

COPY INTO STG.VC_AUTOREPORTEDITHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AutoReportEditHistory/AutoReportEditHistory_Backfill.csv*';

COPY INTO STG.VC_DOCUWAREVISITOREMAILATTACHMENTXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareVisitorEmailAttachmentXref/DocuwareVisitorEmailAttachmentXref_Backfill.csv*';

COPY INTO STG.VC_LOANFUNDING_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM') 
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanFunding/LoanFunding_Backfill.csv*';

COPY INTO STG.VC_PROMISETOPAYDETAILEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END, 
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PromiseToPayDetailEdit/PromiseToPayDetailEdit_Backfill.csv*';

COPY INTO STG.VC_PRESENTMENTCREDITCARDTRANSXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentCreditCardTransXRef/PresentmentCreditCardTransXRef_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERIDENTIFICATIONEDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerIdentificationEdit/CustomerIdentificationEdit_Backfill.csv*';

COPY INTO STG.VC_CREDITCARDS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL 
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $15,
    $16,
    CASE WHEN $17 = '' THEN NULL 
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    CASE WHEN $26 = '' THEN NULL 
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39,
    $40,
    $41,
    $42,
    $43,
    $44,
    $45
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCards/CreditCards_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERPHONENUMBER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $14 = '' THEN NULL 
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $15,
    CASE WHEN $16 = '' THEN NULL 
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $17,
    $18,
    CASE WHEN $19 = '' THEN NULL 
    ELSE to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $20,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL 
    ELSE to_timestamp_ntz($22, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $23 = '' THEN NULL 
    ELSE to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $24,
    CASE WHEN $25 = '' THEN NULL 
    ELSE to_timestamp_ntz($25, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $26 = '' THEN NULL 
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    $29
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerPhoneNumber/CustomerPhoneNumber_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END, 
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $12,
    $13,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = '' THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerIncome/CustomerIncome_Backfill.csv*';

COPY INTO STG.VC_DOCUWAREID_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,--
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareID/DocuwareID_Backfill.csv*';

COPY INTO STG.VC_CUSTOMEREMPLOYER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END, 
    $9, 
    $10,
    $11,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $13,
    $14,
    $15,
    $16,
    $17,
    CASE WHEN $18 = '' THEN NULL 
    ELSE to_timestamp_ntz($18, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $19,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    $21,
    $22,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL 
    ELSE to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $25,
    CASE WHEN $26 = '' THEN NULL 
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $27,
    CASE WHEN $28 = '' THEN NULL 
    ELSE to_timestamp_ntz($28, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $29,
    CASE WHEN $30 = '' THEN NULL 
    ELSE to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $31,
    $32,
    CASE WHEN $33 = '' THEN NULL 
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerEmployer/CustomerEmployer_Backfill.csv*';

COPY INTO STG.VC_VISITORPASSWORDHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorPasswordHistory/VisitorPasswordHistory_Backfill.csv*';

COPY INTO STG.VC_ISSUER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $4,
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    $14,
    $15,
    CASE WHEN $16 = '' THEN NULL 
    ELSE to_timestamp_ntz($16, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END,
    $19,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    CASE WHEN $31 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = 'True' THEN 1
    ELSE 0
    END,
    $34,
    $35,
    $36,
    $37,
    $38,
    CASE WHEN $39 = 'True' THEN 1
    ELSE 0
    END,
    $40
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Issuer/Issuer_Backfill.csv*';

COPY INTO STG.VC_MPAYLOANINSYNCADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL 
    ELSE to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $15 = '' THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16,
    CASE WHEN $17 = '' THEN NULL 
    ELSE to_timestamp_ntz($17, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $18,
    CASE WHEN $19 = '' THEN NULL 
    ELSE to_timestamp_ntz($19, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $20 = '' THEN NULL 
    ELSE to_timestamp_ntz($20, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $21,
    CASE WHEN $22 = '' THEN NULL 
    ELSE to_timestamp_ntz($22, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0
    END,
    $31,
    $32,
    CASE WHEN $33 = '' THEN NULL 
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $34 = '' THEN NULL 
    ELSE to_timestamp_ntz($34, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $35,
    CASE WHEN $36 = '' THEN NULL 
    ELSE to_timestamp_ntz($36, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $37,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $39 = '' THEN NULL 
    ELSE to_timestamp_ntz($39, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $40,
    CASE WHEN $41 = '' THEN NULL 
    ELSE to_timestamp_ntz($41, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $42,
    $43,
    $44,
    $45,
    $46,
    $47,
    $48,
    $49,
    $50,
    $51,
    $52,
    $53,
    $54,
    $55,
    $56,
    $57,
    $58,
    $59,
    $60,
    $61,
    $62,
    $63,
    $64,
    $65,
    $66,
    $67,
    $68,
    $69,
    $70,
    $71,
    $72,
    $73,
    $74,
    $75,
    $76,
    $77,
    $78,
    $79,
    $80,
    $81,
    $82,
    CASE WHEN $83 = '' THEN NULL 
    ELSE to_timestamp_ntz($83, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $84 = '' THEN NULL 
    ELSE to_timestamp_ntz($84, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $85,
    $86,
    $87,
    $88,
    $89,
    $90,
    $91,
    $92,
    $93,
    $94,
    $95,
    $96
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayLoanInSyncAdj/MPayLoanInSyncAdj_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterResult/FormLetterResult_Backfill.csv*';

COPY INTO STG.VC_WEBPIXELVENDORDATA_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebPixelVendorData/WebPixelVendorData_Backfill.csv*';

COPY INTO STG.VC_OVERRIDETERMSOPTIONS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/OverrideTermsOptions/OverrideTermsOptions_Backfill.csv*';

COPY INTO STG.VC_TRANSDETAILLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailLoan/TransDetailLoan_Backfill.csv*';

COPY INTO STG.VC_LOANDEPOSITSTATUSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDepositStatusHistory/LoanDepositStatusHistory_Backfill.csv*';

COPY INTO STG.VC_TRANSDETAILCASH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailCash/TransDetailCash_Backfill.csv*';

COPY INTO STG.VC_LOANPAYMENTDUEDATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentDueDate/LoanPaymentDueDate_Backfill.csv*';

COPY INTO STG.VC_LOANPAYMENTSTAGED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentStaged/LoanPaymentStaged_Backfill.csv*';

COPY INTO STG.VC_VAULTCOUNT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39,
    $40,
    $41,
    CASE WHEN $42 = '' THEN NULL 
    ELSE to_timestamp_ntz($42, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $43,
    $44,
    $45,
    $46,
    $47,
    $48,
    $49,
    $50,
    $51,
    $52
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VaultCount/VaultCount_Backfill.csv*';

COPY INTO STG.VC_DRAWERZ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    CASE WHEN $15 = '' THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16,
    $17,
    $18,
    $19
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DrawerZ/DrawerZ_Backfill.csv*';

COPY INTO STG.VC_DOCUWAREDOCUMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareDocument/DocuwareDocument_Backfill.csv*';

COPY INTO STG.VC_TRANSPOS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransPOS/TransPOS_Backfill.csv*';

COPY INTO STG.VC_CAPSHOLD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    $8,
    $9,
    $10,
    CASE WHEN $11 = '' THEN NULL 
    ELSE to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    CASE WHEN $37 = '' THEN NULL 
    ELSE to_timestamp_ntz($37, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $38,
    CASE WHEN $39 = '' THEN NULL
    ELSE $39
    END,
    $40,
    CASE WHEN $41 = 'True' THEN 1
    ELSE 0
    END,
    $42,
    $43,
    CASE WHEN $44 = '' THEN NULL 
    ELSE to_timestamp_ntz($44, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $45,
    $46,
    $47,
    $48,
    $49,
    $50
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CapsHold/CapsHold_Backfill.csv*';

COPY INTO STG.VC_LOANPAYMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = '' THEN NULL 
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $10,
    $11,
    $12,
    $13,
    $14
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPayment/LoanPayment_Backfill.csv*';

COPY INTO STG.VC_WEBCALLAPPLICATIONSTATUSHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallApplicationStatusHistory/WebCallApplicationStatusHistory_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARRHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = '' THEN NULL 
    ELSE to_timestamp_ntz($8, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $9,
    $10,
    $11,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    $18
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARRHistory/WebCallRARRHistory_Backfill.csv*';

COPY INTO STG.VC_LOANAPPLICATIONADDRESS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    CASE WHEN $12 = '' THEN NULL 
    ELSE to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $13 = '' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationAddress/LoanApplicationAddress_Backfill.csv*';

COPY INTO STG.VC_ENDOFDAYRPTDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    CASE WHEN $13 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EndOfDayRptDetail/EndOfDayRptDetail_Backfill.csv*';

COPY INTO STG.VC_WEBCALLQUEUEAUDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    $5,
    $6,
    $7
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallQueueAudit/WebCallQueueAudit_Backfill.csv*';

COPY INTO STG.VC_COLLECTIONACTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    $6,
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = 'True' THEN 1 
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionAction/CollectionAction_Backfill.csv*';

COPY INTO STG.VC_LOANPAYMENTMPAY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    CASE WHEN $17 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END,
    $19,
    $20,
    $21,
    $22,
    $23
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPaymentMPay/LoanPaymentMPay_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGBASESEGMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    $10,
    $11,
    $12,
    $13,
    $14,
    CASE WHEN $15 = '' THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    CASE WHEN $30 = '' THEN NULL 
    ELSE to_timestamp_ntz($30, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $31 = '' THEN NULL 
    ELSE to_timestamp_ntz($31, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $32 = '' THEN NULL 
    ELSE to_timestamp_ntz($32, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $33 = '' THEN NULL 
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $34,
    CASE WHEN $35 IS NULL THEN ''
    ELSE $35
    END,
    $36,
    $37,
    $38,
    $39,
    CASE WHEN $40 = '' THEN NULL 
    ELSE to_timestamp_ntz($40, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $41,
    $42,
    $43,
    $44,
    $45,
    $46,
    $47,
    $48,
    $49,
    $50,
    $51,
    CASE WHEN $52 = '' THEN NULL 
    ELSE to_timestamp_ntz($52, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $53 = '' THEN NULL 
    ELSE to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingBaseSegment/CreditReportingBaseSegment_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGBASESEGMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')--
    END,
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    CASE WHEN $15 = '' THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39,
    $40,
    $41,
    $42,
    $43,
    $44,
    $45,
    $46,
    $47,
    $48,
    $49,
    $50,
    $51,
    $52,
    $53
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingBaseSegment/CreditReportingBaseSegment_Backfill.csv*';

COPY INTO STG.VC_MPAYRECALCINTERESTADJ_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $7 = '' THEN NULL 
    ELSE to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayRecalcInterestAdj/MPayRecalcInterestAdj_Backfill.csv*';

COPY INTO STG.VC_BALSHEET_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BalSheet_TransDetail/BalSheet_TransDetail_Backfill.csv*';

COPY INTO STG.VC_VISITOREDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorEdit/VisitorEdit_Backfill.csv*';

COPY INTO STG.VC_CREDITCARDTRANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL
    ELSE $2
    END,
    CASE WHEN $3 = '' THEN NULL 
    ELSE to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $4,
    $5,
    $6,
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    $18,
    $19,
    $20,
    $21,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    $24,
    $25,
    $26,
    CASE WHEN $27 = '' THEN NULL
    ELSE $27
    END,
    $28,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    CASE WHEN $30 = '' THEN NULL
    ELSE $30
    END,
    $31,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardTrans/CreditCardTrans_Backfill.csv*';

COPY INTO STG.VC_VISITORCOMMUNICATIONCONSENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $7,
    $8,
    CASE WHEN $9 = '' THEN NULL 
    ELSE to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM +TZH:TZM')
    END,
    $10
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorCommunicationConsent/VisitorCommunicationConsent_Backfill.csv*';

COPY INTO STG.VC_LOANAPPLICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    CASE WHEN $23 = '' THEN NULL 
    ELSE to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39,
    $40,
    $41,
    $42,
    $43,
    $44,
    $45,
    $46,
    $47,
    CASE WHEN $48 = '' THEN NULL 
    ELSE to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $49,
    CASE WHEN $50 = '' THEN NULL 
    ELSE to_timestamp_ntz($50, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $51,
    $52,
    CASE WHEN $53 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $54 = 'True' THEN 1
    ELSE 0
    END,
    $55,
    CASE WHEN $56 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $57 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $58 = '' THEN NULL 
    ELSE to_timestamp_ntz($58, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $59,
    CASE WHEN $60 = '' THEN NULL 
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $61,
    $62,
    $63,
    $64,
    $65,
    CASE WHEN $66 = 'True' THEN 1
    ELSE 0
    END,
    $67,
    $68,
    $69,
    $70,
    $71,
    $72,
    $73,
    $74,
    $75,
    $76,
    $77,
    $78,
    $79,
    $80,
    $81,
    $82,
    CASE WHEN $83 = '' THEN NULL
    ELSE $83
    END,
    $84,
    CASE WHEN $85 = '' THEN NULL
    ELSE $85
    END,
    CASE WHEN $86 = '' THEN NULL
    ELSE $86
    END,
    CASE WHEN $87 = '' THEN NULL
    ELSE $87
    END,
    CASE WHEN $88 = '' THEN NULL
    ELSE $88
    END,
    CASE WHEN $89 = '' THEN NULL
    ELSE $89
    END,
    $90,
    CASE WHEN $91 = '' THEN NULL
    ELSE $91
    END
  
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplication/LoanApplication_Backfill.csv*';

COPY INTO STG.VC_WEBCALLQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL 
    ELSE to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $6,
    $7,
    $8,
    $9,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    CASE WHEN $20 = '' THEN NULL
    ELSE $20
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL
    ELSE $24
    END,
    $25,
    CASE WHEN $26 = '' THEN NULL 
    ELSE to_timestamp_ntz($26, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    $29,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    $34,
    CASE WHEN $35 = '' THEN NULL
    ELSE $35
    END,
    CASE WHEN $36 = '' THEN NULL
    ELSE $36
    END,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $39,
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END,
    $41,
    $42,
    $43,
    $44,
    $45,
    $46,
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END,
    CASE WHEN $48 = '' THEN NULL 
    ELSE to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $49 = '' THEN NULL
    ELSE $49
    END,
    CASE WHEN $50 = '' or $50 = '2' THEN NULL 
    ELSE to_timestamp_ntz($50, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END,
    CASE WHEN $53 = '' THEN NULL 
    ELSE to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $54 = '' THEN NULL
    ELSE $54
    END,
    CASE WHEN $55 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $56 = '' THEN NULL 
    ELSE to_timestamp_ntz($56, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $57 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $58 = '' THEN NULL
    ELSE $58
    END,
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END,
    $60
  
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallQueue/WebCallQueue_Backfill.csv*';

COPY INTO STG.VC_VISITORCOMMUNICATIONPREFERENCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL 
    ELSE to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorCommunicationPreference/VisitorCommunicationPreference_Backfill.csv*';


COPY INTO STG.VC_MARKETINGINVITATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    CASE WHEN $26 = '' THEN NULL
    ELSE $26
    END,
    $27,
    CASE WHEN $28 = '' THEN NULL
    ELSE $28
    END,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    $30,
    $31,
    $32,
    CASE WHEN $33 = '' THEN NULL 
    ELSE to_timestamp_ntz($33, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $34 = '' THEN NULL 
    ELSE to_timestamp_ntz($34, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $35,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    $38,
    $39,
    CASE WHEN $40 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $41 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $42 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    $45,
    $46,
    $47,
    $48,
    $49,
    $50
  
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MarketingInvitation/MarketingInvitation_Backfill.csv*';

COPY INTO STG.VC_ENDOFDAYINVENTORYDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    $7,
    $8,
    $9
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EndOfDayInventoryDetail/EndOfDayInventoryDetail_Backfill.csv*';

COPY INTO STG.VC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9,
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetail/TransDetail_Backfill_2020.csv*';

COPY INTO STG.VC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9,
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetail/TransDetail_Backfill_2021.csv*';

COPY INTO STG.VC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9,
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetail/TransDetail_Backfill_2022.csv*';

COPY INTO STG.VC_TRANSDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = '' THEN NULL 
    ELSE to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END,
    $9,
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetail/TransDetail_Backfill_2023.csv*';

COPY INTO STG.VC_WEBDIALERCALLRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL 
    ELSE to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $25,
    $26,
    $27,
    CASE WHEN $28 = '' THEN NULL
    ELSE $28
    END,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $39,
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    $42,
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    CASE WHEN $46 = '' THEN NULL
    ELSE $46
    END,
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END,
    CASE WHEN $48 = '' THEN NULL
    ELSE $48
    END,
    $49,
    CASE WHEN $50 = '' THEN NULL
    ELSE $50
    END,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END,
    $53,
    $54,
    CASE WHEN $55 = '' THEN NULL
    ELSE $55
    END,
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END,
    CASE WHEN $57 = '' THEN NULL
    ELSE $57
    END,
    $58,
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END,
    CASE WHEN $60 = '' THEN NULL 
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $61 = '' THEN NULL
    ELSE $61
    END,
    CASE WHEN $62 = '' THEN NULL
    ELSE $62
    END,
    CASE WHEN $63 = '' THEN NULL
    ELSE $63
    END,
    CASE WHEN $64 = '' THEN NULL
    ELSE $64
    END,
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END,
    CASE WHEN $66 = '' THEN NULL
    ELSE $66
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE $67
    END,
    CASE WHEN $68 = '' THEN NULL
    ELSE $68
    END,
    CASE WHEN $69 = '' THEN NULL
    ELSE $69
    END,
    $70,
    $71,
    CASE WHEN $72 = '' THEN NULL 
    ELSE to_timestamp_ntz($72, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerCallResult/WebDialerCallResult_Backfill_2020.csv*';

COPY INTO STG.VC_WEBDIALERCALLRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL 
    ELSE to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $25,
    $26,
    $27,
    CASE WHEN $28 = '' THEN NULL
    ELSE $28
    END,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $39,
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    $42,
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    CASE WHEN $46 = '' THEN NULL
    ELSE $46
    END,
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END,
    CASE WHEN $48 = '' THEN NULL
    ELSE $48
    END,
    $49,
    CASE WHEN $50 = '' THEN NULL
    ELSE $50
    END,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END,
    $53,
    $54,
    CASE WHEN $55 = '' THEN NULL
    ELSE $55
    END,
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END,
    CASE WHEN $57 = '' THEN NULL
    ELSE $57
    END,
    $58,
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END,
    CASE WHEN $60 = '' THEN NULL 
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $61 = '' THEN NULL
    ELSE $61
    END,
    CASE WHEN $62 = '' THEN NULL
    ELSE $62
    END,
    CASE WHEN $63 = '' THEN NULL
    ELSE $63
    END,
    CASE WHEN $64 = '' THEN NULL
    ELSE $64
    END,
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END,
    CASE WHEN $66 = '' THEN NULL
    ELSE $66
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE $67
    END,
    CASE WHEN $68 = '' THEN NULL
    ELSE $68
    END,
    CASE WHEN $69 = '' THEN NULL
    ELSE $69
    END,
    $70,
    $71,
    CASE WHEN $72 = '' THEN NULL 
    ELSE to_timestamp_ntz($72, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerCallResult/WebDialerCallResult_Backfill_2021.csv*';

COPY INTO STG.VC_WEBDIALERCALLRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL 
    ELSE to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $25,
    $26,
    $27,
    CASE WHEN $28 = '' THEN NULL
    ELSE $28
    END,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $39,
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    $42,
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    CASE WHEN $46 = '' THEN NULL
    ELSE $46
    END,
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END,
    CASE WHEN $48 = '' THEN NULL
    ELSE $48
    END,
    $49,
    CASE WHEN $50 = '' THEN NULL
    ELSE $50
    END,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END,
    $53,
    $54,
    CASE WHEN $55 = '' THEN NULL
    ELSE $55
    END,
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END,
    CASE WHEN $57 = '' THEN NULL
    ELSE $57
    END,
    $58,
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END,
    CASE WHEN $60 = '' THEN NULL 
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $61 = '' THEN NULL
    ELSE $61
    END,
    CASE WHEN $62 = '' THEN NULL
    ELSE $62
    END,
    CASE WHEN $63 = '' THEN NULL
    ELSE $63
    END,
    CASE WHEN $64 = '' THEN NULL
    ELSE $64
    END,
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END,
    CASE WHEN $66 = '' THEN NULL
    ELSE $66
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE $67
    END,
    CASE WHEN $68 = '' THEN NULL
    ELSE $68
    END,
    CASE WHEN $69 = '' THEN NULL
    ELSE $69
    END,
    $70,
    $71,
    CASE WHEN $72 = '' THEN NULL 
    ELSE to_timestamp_ntz($72, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerCallResult/WebDialerCallResult_Backfill_2022.csv*';

COPY INTO STG.VC_WEBDIALERCALLRESULT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE $6
    END,
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    $8,
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END,
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    CASE WHEN $12 = '' THEN NULL
    ELSE $12
    END,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    $15,
    $16,
    CASE WHEN $17 = '' THEN NULL
    ELSE $17
    END,
    CASE WHEN $18 = '' THEN NULL
    ELSE $18
    END,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END,
    CASE WHEN $20 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL 
    ELSE to_timestamp_ntz($24, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $25,
    $26,
    $27,
    CASE WHEN $28 = '' THEN NULL
    ELSE $28
    END,
    CASE WHEN $29 = '' THEN NULL
    ELSE $29
    END,
    $30,
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END,
    CASE WHEN $32 = '' THEN NULL
    ELSE $32
    END,
    CASE WHEN $33 = '' THEN NULL
    ELSE $33
    END,
    CASE WHEN $34 = '' THEN NULL
    ELSE $34
    END,
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END,
    $36,
    CASE WHEN $37 = '' THEN NULL
    ELSE $37
    END,
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $39,
    CASE WHEN $40 = '' THEN NULL
    ELSE $40
    END,
    CASE WHEN $41 = '' THEN NULL
    ELSE $41
    END,
    $42,
    CASE WHEN $43 = '' THEN NULL
    ELSE $43
    END,
    CASE WHEN $44 = '' THEN NULL
    ELSE $44
    END,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    CASE WHEN $46 = '' THEN NULL
    ELSE $46
    END,
    CASE WHEN $47 = '' THEN NULL
    ELSE $47
    END,
    CASE WHEN $48 = '' THEN NULL
    ELSE $48
    END,
    $49,
    CASE WHEN $50 = '' THEN NULL
    ELSE $50
    END,
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END,
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END,
    $53,
    $54,
    CASE WHEN $55 = '' THEN NULL
    ELSE $55
    END,
    CASE WHEN $56 = '' THEN NULL
    ELSE $56
    END,
    CASE WHEN $57 = '' THEN NULL
    ELSE $57
    END,
    $58,
    CASE WHEN $59 = '' THEN NULL
    ELSE $59
    END,
    CASE WHEN $60 = '' THEN NULL 
    ELSE to_timestamp_ntz($60, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $61 = '' THEN NULL
    ELSE $61
    END,
    CASE WHEN $62 = '' THEN NULL
    ELSE $62
    END,
    CASE WHEN $63 = '' THEN NULL
    ELSE $63
    END,
    CASE WHEN $64 = '' THEN NULL
    ELSE $64
    END,
    CASE WHEN $65 = '' THEN NULL
    ELSE $65
    END,
    CASE WHEN $66 = '' THEN NULL
    ELSE $66
    END,
    CASE WHEN $67 = '' THEN NULL
    ELSE $67
    END,
    CASE WHEN $68 = '' THEN NULL
    ELSE $68
    END,
    CASE WHEN $69 = '' THEN NULL
    ELSE $69
    END,
    $70,
    $71,
    CASE WHEN $72 = '' THEN NULL 
    ELSE to_timestamp_ntz($72, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerCallResult/WebDialerCallResult_Backfill_2023.csv*';

COPY INTO STG.VC_GLOBAL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    CASE WHEN $5 = '' THEN NULL 
    ELSE to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7,
    $8,
    $9,
     CASE WHEN $10 = '' THEN NULL 
    ELSE to_timestamp_ntz($10, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $11 = '' THEN NULL
    ELSE $11
    END,
    $12,
    CASE WHEN $13 = '' or $13 = '0' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $14,
    CASE WHEN $15 = ''  THEN NULL 
    ELSE to_timestamp_ntz($15, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END,
    CASE WHEN $23 = '' THEN NULL 
    ELSE to_timestamp_ntz($23, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $24 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $25 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $26 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $29 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $30 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $31 = 'True' THEN 1
    ELSE 0
    END,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    $39,
    $40,
    $41,
    $42,
    $43,
    $44,
    CASE WHEN $45 = '' THEN NULL
    ELSE $45
    END,
    $46,
    $47,
    CASE WHEN $48 = ''  THEN NULL 
    ELSE to_timestamp_ntz($48, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $49,
    $50,
    $51,
    $52,
    CASE WHEN $53 = '' THEN NULL 
    ELSE to_timestamp_ntz($53, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $54 = '' THEN NULL 
    ELSE to_timestamp_ntz($54, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    CASE WHEN $55 = '' THEN NULL 
    ELSE to_timestamp_ntz($55, 'MM/DD/YYYY HH12:MI:ss AM')
    END,
    $56,
    $57,
    $58,
    $59,
    $60,
    CASE WHEN $61 = 'True' THEN 1
    ELSE 0
    END,
    $62,
    $63,
    CASE WHEN $64 = 'True' THEN 1
    ELSE 0
    END,
    $65,
    CASE WHEN $66 = 'True' THEN 1
    ELSE 0
    END,
    $67,
    $68,
    $69,
    $70,
    CASE WHEN $71 = '' THEN NULL
    ELSE $71
    END,
    CASE WHEN $72 = '' THEN NULL
    ELSE $72
    END,
    $73,
    CASE WHEN $74 = 'True' THEN 1
    ELSE 0
    END,
    $75,
    CASE WHEN $76 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $77 = 'True' THEN 1
    ELSE 0
    END,
    $78,
    $79,
    $80,
    CASE WHEN $81 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $82 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $83 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $84 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $85 = 'True' THEN 1
    ELSE 0
    END,
    $86,
    $87,
    $88,
    $89,
    CASE WHEN $90 = 'True' THEN 1
    ELSE 0
    END,
    $91,
    CASE WHEN $92 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $93 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $94 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $95 = 'True' THEN 1
    ELSE 0
    END,
    $96,
    $97,
    $98,
    $99
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Global/Global_Backfill.csv*';

COPY INTO STG.VC_COMPANY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    $10, 
    $11, 
    $12, 
    $13, 
    $14, 
    $15, 
    $16, 
    $17, 
    $18, 
    $19, 
    $20, 
    $21, 
    $22, 
    $23, 
    $24, 
    $25, 
    $26, 
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $28 = '' THEN NULL 
    ELSE to_timestamp_ntz($28, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $29, 
    $30, 
    $31, 
    $32, 
    CASE WHEN $33 = 'True' THEN 1
    ELSE 0
    END, 
    $34, 
    $35, 
    $36, 
    $37, 
    CASE WHEN $38 = '' THEN NULL 
    ELSE to_timestamp_ntz($38, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $39, 
    $40, 
    $41, 
    $42, 
    $43, 
    $44, 
    $45, 
    $46, 
    $47, 
    CASE WHEN $48 = 'True' THEN 1
    ELSE 0
    END, 
    $49, 
    $50, 
    $51, 
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END, 
    $53, 
    $54, 
    $55, 
    $56, 
    $57, 
    $58, 
    $59, 
    $60, 
    $61, 
    $62, 
    $63, 
    $64, 
    $65, 
    $66, 
    $67, 
    $68, 
    $69, 
    $70, 
    $71, 
    $72, 
    $73, 
    $74, 
    $75, 
    $76, 
    CASE WHEN $77 = 'True' THEN 1
    ELSE 0
    END, 
    $78, 
    $79, 
    $80, 
    $81, 
    $82, 
    $83, 
    $84, 
    $85, 
    $86, 
    $87, 
    $88, 
    $89, 
    $90, 
    CASE WHEN $91 = 'True' THEN 1
    ELSE 0
    END, 
    $92, 
    $93, 
    CASE WHEN $94 = '' THEN NULL
    ELSE $94
    END, 
    $95, 
    $96, 
    $97, 
    $98, 
    $99, 
    $100, 
    $101, 
    $102, 
    $103, 
    $104, 
    $105, 
    $106, 
    $107, 
    $108, 
    $109, 
    $110, 
    $111, 
    $112, 
    $113, 
    CASE WHEN $114 = 'True' THEN 1
    ELSE 0
    END, 
    $115, 
    $116, 
    $117, 
    $118, 
    $119, 
    CASE WHEN $120 = '' THEN NULL 
    ELSE to_timestamp_ntz($120, 'MM/DD/YYYY HH12:MI:ss AM')
    END, 
    $121, 
    $122, 
    $123, 
    $124, 
    $125, 
    $126, 
    $127, 
    $128, 
    $129, 
    $130, 
    $131, 
    $132, 
    $133, 
    $134, 
    $135, 
    $136, 
    $137, 
    $138, 
    $139, 
    $140, 
    CASE WHEN $141 = 'True' THEN 1
    ELSE 0
    END, 
    $142, 
    $143, 
    $144, 
    $145, 
    $146, 
    $147, 
    CASE WHEN $148 = '' THEN NULL
    ELSE $148
    END, 
    $149, 
    $150, 
    $151, 
    $152, 
    $153, 
    $154, 
    $155, 
    $156, 
    $157, 
    $158, 
    $159, 
    $160, 
    $161, 
    $162, 
    $163, 
    $164, 
    $165, 
    $166, 
    CASE WHEN $167 = '' THEN NULL
    ELSE $167
    END, 
    $168, 
    $169, 
    $170, 
    $171, 
    $172, 
    $173, 
    $174, 
    $175 
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Company/Company_Backfill.csv*';

COPY INTO STG.VC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayInterest/MPayInterest_Backfill_2020.csv*';

COPY INTO STG.VC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayInterest/MPayInterest_Backfill_2021.csv*';

COPY INTO STG.VC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayInterest/MPayInterest_Backfill_2022.csv*';

COPY INTO STG.VC_MPAYINTEREST_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MPayInterest/MPayInterest_Backfill_2023.csv*';

COPY INTO STG.VC_WEBDIALERUPLOADHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4, 
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerUploadHistory/WebDialerUploadHistory_Backfill_2020.csv*';

COPY INTO STG.VC_WEBDIALERUPLOADHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4, 
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerUploadHistory/WebDialerUploadHistory_Backfill_2021.csv*';

COPY INTO STG.VC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailAcct/TransDetailAcct_Backfill_1.csv*';

COPY INTO STG.VC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailAcct/TransDetailAcct_Backfill_2.csv*';

COPY INTO STG.VC_TRANSDETAILACCT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailAcct/TransDetailAcct_Backfill_3.csv*';

COPY INTO STG.VC_LOANDUEDATECHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $8, 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $13 = '' THEN NULL 
    ELSE to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM')
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanDueDateChange/LoanDueDateChange_Backfill.csv*';

COPY INTO STG.VC_LOANFUNDINGHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanFundingHistory/LoanFundingHistory_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARRFEATURES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARRFeatures/WebCallRARRFeatures_Backfill.csv*';

COPY INTO STG.VC_DOCUWARELOANDOC_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareLoanDoc/DocuwareLoanDoc_Backfill.csv*';

COPY INTO STG.VC_CUSTOMEREDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7, 
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerEdit/CustomerEdit_Backfill.csv*';

COPY INTO STG.VC_LOANCHKACCTCHANGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    $7, 
    $8, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanChkAcctChange/SLoanChkAcctChange_Backfill.csv*';

COPY INTO STG.VC_PRESENTMENTREQUESTACHHISTORYXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequestACHHistoryXRef/PresentmentRequestACHHistoryXRef_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERPAYMENTACCOUNT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE $4
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerPaymentAccount/CustomerPaymentAccount_Backfill.csv*';

COPY INTO STG.VC_LOANAPPLICATIONSATISFACTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanApplicationSatisfaction/LoanApplicationSatisfaction_Backfill.csv*';

COPY INTO STG.VC_COLLECTIONAGINGITEM_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionAgingItem/CollectionAgingItem_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'),
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerActivity/CustomerActivity_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERPHONENUMBEREDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5,
    $6,
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerPhoneNumberEdit/CustomerPhoneNumberEdit_Backfill.csv*';

COPY INTO STG.VC_LOANINCOME_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanIncome/LoanIncome_Backfill.csv*';

COPY INTO STG.VC_VISITOREMAILDISPOSITION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorEmailDisposition/VisitorEmailDisposition_Backfill.csv*';

COPY INTO STG.VC_RECEIPT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Receipt/Receipt_Backfill.csv*';

COPY INTO STG.VC_WEBCALLVISITORALERTS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    $8, 
    to_timestamp_ntz($9, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallVisitorAlerts/WebCallVisitorAlerts_Backfill.csv*';

COPY INTO STG.VC_TRANSDETAILCHECK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8, 
    CASE WHEN $9 = '' THEN NULL
    ELSE $9
    END, 
    $10
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TransDetailCheck/TransDetailCheck_Backfill.csv*';

COPY INTO STG.VC_CAPSCCTXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CapsCCTXRef/CapsCCTXRef_Backfill.csv*';

COPY INTO STG.VC_TELLERPWDHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    $7, 
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TellerPwdHistory/TellerPwdHistory_Backfill.csv*';

COPY INTO STG.VC_COLLECTIONNOTE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    try_to_timestamp($8), 
    $9, 
    CASE WHEN $10 = '' THEN NULL
    ELSE $10
    END,
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM'),
    $12,
    $13,
    try_to_timestamp($14)
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollectionNote/CollectionNote_Backfill.csv*';

COPY INTO STG.VC_SDNADDRESS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SDNAddress/SDNAddress_Backfill.csv*';

COPY INTO STG.VC_COLLBONUSPTP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $8, 
    $9, 
    $10,
    $11

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CollBonusPTP/CollBonusPTP_Backfill.csv*';

COPY INTO STG.VC_ACH_RECV_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:ss AM'), 
    $4,
    $5,
    $6,
    $7, 
    try_to_timestamp($8)

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACH_Recv/ACH_Recv_Backfill.csv*';

COPY INTO STG.VC_STORE_WINDOWS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3,
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    try_to_timestamp($11),
    try_to_timestamp($12),
    $13,
    $14,
    $15,
    $16,
    $17

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Store_Windows/Store_Windows_Backfill.csv*';

COPY INTO STG.VC_PROMISETOPAYDETAILTRANS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    try_to_timestamp($5), 
    $6

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PromiseToPayDetailTrans/PromiseToPayDetailTrans_Backfill.csv*';

COPY INTO STG.VC_SDNALTERNATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SDNAlternate/SDNAlternate_Backfill.csv*';

COPY INTO STG.VC_CUSTOMEREMPLOYEREDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7, 
    $8

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerEmployerEdit/CustomerEmployerEdit_Backfill.csv*';

COPY INTO STG.VC_PROCESSCONFIGDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ProcessConfigDetail/ProcessConfigDetail_Backfill.csv*';

COPY INTO STG.VC_BANKCLOSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:ss AM'),
    $3,
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BankClosed/BankClosed_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGL1SEGMENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingL1Segment/CreditReportingL1Segment_Backfill.csv*';

COPY INTO STG.VC_IDENTIFICATIONTYPESTATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    try_to_timestamp($11),
    $12

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IdentificationTypeState/IdentificationTypeState_Backfill.csv*';

COPY INTO STG.VC_SKIPTRACESTEP_AUDITCATEGORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SkipTraceStep_AuditCategory/SkipTraceStep_AuditCategory_Backfill.csv*';

COPY INTO STG.VC_DRAWERMASTER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $5 = '' THEN NULL
    ELSE $5
    END,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    $12,
    $13,
    $14,
    $15,
    $16

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DrawerMaster/DrawerMaster_Backfill.csv*';

COPY INTO STG.VC_INCOMEVERIFYMETHODLOCATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeVerifyMethodLocation/IncomeVerifyMethodLocation_Backfill.csv*';

COPY INTO STG.VC_BANKCLASSIFICATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    to_timestamp_ntz($4, 'MM/DD/YYYY HH12:MI:ss AM'),
    $5

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BankClassification/BankClassification_Backfill.csv*';

COPY INTO STG.VC_RISAUDIT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RISAUDIT/RISAUDIT_Backfill.csv*';

COPY INTO STG.VC_TESTCREDITCARD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TestCreditCard/TestCreditCard_Backfill.csv*';

COPY INTO STG.VC_PROCESSSCHEDULE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4,
    $5,
    $6,
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    to_timestamp_ntz($12, 'MM/DD/YYYY HH12:MI:ss AM'),
    try_to_timestamp($13),
    to_timestamp_ntz($14, 'MM/DD/YYYY HH12:MI:ss AM'),
    $15,
    $16

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ProcessSchedule/ProcessSchedule_Backfill.csv*';

COPY INTO STG.VC_WEBCALLCATRARRALIAS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE $3
    END,
    $4,
    $5,
    $6,
    $7,
    CASE WHEN $8 = '' THEN NULL
    ELSE $8
    END
    
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallCatRarrAlias/WebCallCatRarrAlias_Backfill.csv*';

COPY INTO STG.VC_GLOBALSTATES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END, 
    $10,
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END,
    $13,
    CASE WHEN $14 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $15 = 'True' THEN 1
    ELSE 0
    END,
    $16,
    $17,
    CASE WHEN $18 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $19 = 'True' THEN 1
    ELSE 0
    END,
    $20,
    $21,
    CASE WHEN $22 = 'True' THEN 1
    ELSE 0
    END,
    $23,
    $24

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GlobalStates/GlobalStates_Backfill.csv*';

COPY INTO STG.VC_REDACTEDWORDS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RedactedWords/RedactedWords_Backfill.csv*';

COPY INTO STG.VC_WEBCALLAUTHORIZEDVISITORCONTACT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5, 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM') 


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallAuthorizedVisitorContact/WebCallAuthorizedVisitorContact_Backfill.csv*';

COPY INTO STG.VC_WEBCALLCATEGORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($11, 'MM/DD/YYYY HH12:MI:ss AM')


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallCategory/WebCallCategory_Backfill.csv*';

COPY INTO STG.VC_ACHBANKCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM')

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHBankConfig/ACHBankConfig_Backfill.csv*';

COPY INTO STG.VC_ACHBANKCONFIGEXCEPTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHBankConfigException/ACHBankConfigException_Backfill.csv*';

COPY INTO STG.VC_LOANPRODUCTCONFIGBUMPUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    CASE WHEN $1 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END, 
    $4, 
    $5, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($8,'MM/DD/YYYY HH12:MI:SS AM'), 
    $9,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductConfigBumpUp/LoanProductConfigBumpUp_Backfill.csv*';

COPY INTO STG.VC_SECURITYGROUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    CASE WHEN $1 = 'True' THEN 1
    ELSE 0
    END, 
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SecurityGroup/SecurityGroup_Backfill.csv*';

COPY INTO STG.VC_WEBCALLWORKQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    try_to_timestamp($4),
    try_to_timestamp($5),
    $6,
    $7,
    $8,
    $9,
    $10, 
    CASE WHEN $11 = 'True' THEN 1
    ELSE 0
    END, 
    $12, 
    $13, 
    $14, 
    $15, 
    $16, 
    $17, 
    $18, 
    $19, 
    CASE WHEN $20 = '' THEN NULL
    ELSE $20
    END, 
    CASE WHEN $21 = '' THEN NULL
    ELSE $21
    END, 
    CASE WHEN $22 = '' THEN NULL
    ELSE $22
    END, 
    CASE WHEN $23 = '' THEN NULL
    ELSE $23
    END,
    CASE WHEN $24 = '' THEN NULL
    ELSE $24
    END, 
    $25, 
    try_to_timestamp($26),
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $28 = 'True' THEN 1
    ELSE 0
    END, 
    $29, 
    $30, 
    CASE WHEN $31 = '' THEN NULL
    ELSE $31
    END, 
    $32, 
    $33, 
    $34, 
    CASE WHEN $35 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $36 = '' THEN NULL
    ELSE $36
    END, 
    CASE WHEN $37 = 'True' THEN 1
    ELSE 0
    END, 
    try_to_timestamp($38), 
    $39, 
    $40, 
    $41, 
    $42, 
    $43, 
    $44, 
    $45, 
    $46, 
    $47, 
    $48, 
    $49, 
    $50, 
    CASE WHEN $51 = '' THEN NULL
    ELSE $51
    END, 
    CASE WHEN $52 = '' THEN NULL
    ELSE $52
    END, 
    try_to_timestamp($53), 
    CASE WHEN $54 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $55 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $56 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($57, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $58 = 'True' THEN 1
    ELSE 0
    END, 
    $59, 
    CASE WHEN $60 = '' THEN NULL
    ELSE $60
    END, 
    CASE WHEN $61 = '' THEN NULL
    ELSE $61
    END


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallWorkQueue/WebCallWorkQueue_Backfill.csv*';

COPY INTO STG.VC_GLACCTLOANPRODUCTGROUPHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    $23,
    $24,
    $25,
    $26,
    $27,
    $28,
    $29,
    $30,
    $31,
    $32,
    $33,
    $34,
    $35,
    $36,
    $37,
    $38,
    to_timestamp_ntz($39,'MM/DD/YYYY HH12:MI:SS AM'),
    to_timestamp_ntz($40,'MM/DD/YYYY HH12:MI:SS AM') 

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GLAcctLoanProductGroupHistory/GLAcctLoanProductGroupHistory_Backfill.csv*';

COPY INTO STG.VC_LOANPRODUCTENABLENEWLOAN_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'), 
    to_timestamp_ntz($6, 'MM/DD/YYYY HH12:MI:ss AM'), 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,  
    to_timestamp_ntz($9,'MM/DD/YYYY HH12:MI:SS AM'), 
    $10,
    to_timestamp_ntz($11,'MM/DD/YYYY HH12:MI:SS AM'),
    $12,
    CASE WHEN $13 = '' THEN NULL
    ELSE $13
    END,
    CASE WHEN $14 = '' THEN NULL
    ELSE $14
    END,
    CASE WHEN $15 = '' THEN NULL
    ELSE $15
    END,
    CASE WHEN $16 = '' THEN NULL
    ELSE $16
    END,
    $17,
    $18,
    CASE WHEN $19 = '' THEN NULL
    ELSE $19
    END


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductEnableNewLoan/LoanProductEnableNewLoan_Backfill.csv*';

COPY INTO STG.VC_DENOMINATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    $6, 
    CASE WHEN $7 = '' THEN NULL
    ELSE $7
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/Denomination/Denomination_Backfill.csv*';

COPY INTO STG.VC_CARDREVIEW_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    to_timestamp_ntz($4,'MM/DD/YYYY HH12:MI:SS AM'), 
    $5, 
    try_to_timestamp($6), 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $10 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CardReview/CardReview_Backfill.csv*';

COPY INTO STG.VC_CARDGOVERNORHISTORY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    to_timestamp_ntz($6,'MM/DD/YYYY HH12:MI:SS AM'), 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CardGovernorHistory/CardGovernorHistory_Backfill.csv*';

COPY INTO STG.VC_PHONESKILLSREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PhoneSkillsReason/PhoneSkillsReason_Backfill.csv*';

COPY INTO STG.VC_GALILEORESPONSECODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GalileoResponseCode/GalileoResponseCode_Backfill.csv*';

COPY INTO STG.VC_WEBDIALERRESULTTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END, 
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebDialerResultType/WebDialerResultType_Backfill.csv*';

COPY INTO STG.VC_LOCALETRANSLATOR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LocaleTranslator/LocaleTranslator_Backfill.csv*';

COPY INTO STG.VC_AREACODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AreaCode/AreaCode_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGACTIVITY_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    $7, 
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    $9, 
    $10

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingActivity/CreditReportingActivity_Backfill.csv*';

COPY INTO STG.VC_DOCUWARECABINET_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3, 
    $4, 
    $5, 
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END, 
    $7, 
    $8, 
    try_to_timestamp($9), 
    try_to_timestamp($10),
    $11,
    CASE WHEN $12 = 'True' THEN 1
    ELSE 0
    END

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareCabinet/DocuwareCabinet_Backfill.csv*';

COPY INTO STG.VC_DOCUWARECABINETTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DocuwareCabinetType/DocuwareCabinetType_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERAUDITCODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterAuditCode/FormLetterAuditCode_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARRCATEGORYREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARRCategoryReason/WebCallRARRCategoryReason_Backfill.csv*';

COPY INTO STG.VC_LOCATIONFUNDINGMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LocationFundingMethod/LocationFundingMethod_Backfill.csv*';

COPY INTO STG.VC_CABLENDER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    $10,
    $11,
    $12,
    $13,
    $14,
    $15,
    $16,
    $17,
    $18,
    $19,
    $20,
    $21,
    $22,
    to_timestamp_ntz($23,'MM/DD/YYYY HH12:MI:SS AM'),
    $24,
    to_timestamp_ntz($25,'MM/DD/YYYY HH12:MI:SS AM'),
    $26,
    CASE WHEN $27 = 'True' THEN 1
    ELSE 0
    END,
    $28,
    $29,
    $30,
    $31, 
    $32 


FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CABLender/CABLender_Backfill.csv*';

COPY INTO STG.VC_SDNMATCH_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    to_timestamp_ntz($13, 'MM/DD/YYYY HH12:MI:ss AM'),
    $14,
    $15,
    $16
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SdnMatch/SdnMatch_Backfill.csv*';

COPY INTO STG.VC_RULEDEFSETDETAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    to_timestamp_ntz($5, 'MM/DD/YYYY HH12:MI:ss AM'),
    $6
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RuleDefSetDetail/RuleDefSetDetail_Backfill.csv*';

COPY INTO STG.VC_COMMUNICATIONCOMMUNICATIONEVENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationCommunicationEvent/CommunicationCommunicationEvent_Backfill.csv*';

COPY INTO STG.VC_PTPPAYMENTPLANSECURITYGROUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanSecurityGroup/PTPPaymentPlanSecurityGroup_Backfill.csv*';

COPY INTO STG.VC_AUTOREPORTSCHEDULE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AutoReportSchedule/AutoReportSchedule_Backfill.csv*';

COPY INTO STG.VC_DUALAPPROVALMESSAGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/DualApprovalMessage/DualApprovalMessage_Backfill.csv*';

COPY INTO STG.VC_PROCESSCONFIGINSTANCE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    $6,
    to_timestamp_ntz($7, 'MM/DD/YYYY HH12:MI:ss AM'),
    $8,
    try_to_timestamp($9),
    try_to_timestamp($10)
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ProcessConfigInstance/ProcessConfigInstance_Backfill.csv*';

COPY INTO STG.VC_LOANPRODUCTCONFIGBANK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = '' THEN NULL
    ELSE 0
    END,
    CASE WHEN $4 = '' THEN NULL
    ELSE 0
    END,
    $5,
    CASE WHEN $6 = '' THEN NULL
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanProductConfigBank/LoanProductConfigBank_Backfill.csv*';

COPY INTO STG.VC_COMMUNICATIONEVENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationEvent/CommunicationEvent_Backfill.csv*';

COPY INTO STG.VC_RISTASK_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RisTask/RisTask_Backfill.csv*';

COPY INTO STG.VC___REFACTORLOG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/__RefactorLog/__RefactorLog_Backfill.csv*';

COPY INTO STG.VC_EOSCARDISPUTECODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EOscarDisputeCode/EOscarDisputeCode_Backfill.csv*';

COPY INTO STG.VC_COMMUNICATIONCONSENTCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    try_to_timestamp($6),
    CASE WHEN $7 = '' THEN NULL
    ELSE 0
    END,
    CASE WHEN $8 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $9 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationConsentConfig/CommunicationConsentConfig_Backfill.csv*';

COPY INTO STG.VC_FORCEAPPROVALQUESTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ForceApprovalQuestion/ForceApprovalQuestion_Backfill.csv*';

COPY INTO STG.VC_WEBCALLFEATURES_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallFeatures/WebCallFeatures_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARRACTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3, 
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRARRAction/WebCallRARRAction_Backfill.csv*';

COPY INTO STG.VC_CHECKTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CheckType/CheckType_Backfill.csv*';

COPY INTO STG.VC_CREDITCARDVENDORCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardVendorConfig/CreditCardVendorConfig_Backfill.csv*';

COPY INTO STG.VC_RELS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2, 
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RELS/RELS_Backfill.csv*';

COPY INTO STG.VC_PTPPAYMENTPLANPAYMENTMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PTPPaymentPlanPaymentMethod/PTPPaymentPlanPaymentMethod_Backfill.csv*';

COPY INTO STG.VC_GALILEOALERTTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    to_timestamp_ntz($3,'MM/DD/YYYY HH12:MI:SS AM'),
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GalileoAlertType/GalileoAlertType_Backfill.csv*';

COPY INTO STG.VC_ASPECTEXPORTJOBADDONMETRICIDXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/AspectExportJobAddOnMetricIdXref/AspectExportJobAddOnMetricIdXref_Backfill.csv*';

COPY INTO STG.VC_COMMUNICATIONCONSENTCONFIGCOMMUNICATIONEVENT_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CommunicationConsentConfigCommunicationEvent/CommunicationConsentConfigCommunicationEvent_Backfill.csv*';

COPY INTO STG.VC_REASONFORARREARS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    to_timestamp_ntz($2,'MM/DD/YYYY HH12:MI:SS AM'),
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ReasonForArrears/ReasonForArrears_Backfill.csv*';

COPY INTO STG.VC_TASKACTIONRESULTXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    to_timestamp_ntz($3, 'MM/DD/YYYY HH12:MI:SS AM'),
    $4,
    try_to_timestamp($5),
    $6,
    $7,
    $8
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/TaskActionResultXref/TaskActionResultXref_Backfill.csv*';

COPY INTO STG.VC_CREDITCARDBRAND_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditCardBrand/CreditCardBrand_Backfill.csv*';

COPY INTO STG.VC_PRESCREENQUESTIONSTATE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PrescreenQuestionState/PrescreenQuestionState_Backfill.csv*';

COPY INTO STG.VC_EMAILATTACHMENTTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EmailAttachmentType/EmailAttachmentType_Backfill.csv*';

COPY INTO STG.VC_WEBBLOBTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebBlobType/WebBlobType_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERLEADACTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerLeadAction/CustomerLeadAction_Backfill.csv*';

COPY INTO STG.VC_VISITORSECURITYQUESTION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorSecurityQuestion/VisitorSecurityQuestion_Backfill.csv*';

COPY INTO STG.VC_WEBLOANCREDITFRAUD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END,
    to_timestamp_ntz($8,'MM/DD/YYYY HH12:MI:SS AM')
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebLoanCreditFraud/WebLoanCreditFraud_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERLOANHISTORYFILE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterLoanHistoryFile/FormLetterLoanHistoryFile_Backfill.csv*';

COPY INTO STG.VC_CHK_TYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/chk_type/chk_type_Backfill.csv*';

COPY INTO STG.VC_BLOCKREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BlockReason/BlockReason_Backfill.csv*';

COPY INTO STG.VC_GLACCTLOCATIONGROUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4, 
    $5, 
    $6, 
    $7, 
    $8, 
    $9, 
    $10, 
    $11

FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/GLAcctLocationGroup/GLAcctLocationGroup_Backfill.csv*';

COPY INTO STG.VC_STORECLOSED_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    to_timestamp_ntz($2,'MM/DD/YYYY HH12:MI:SS AM'),
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/StoreClosed/StoreClosed_Backfill.csv*';

COPY INTO STG.VC_EXTERNALAPPS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ExternalApps/ExternalApps_Backfill.csv*';

COPY INTO STG.VC_EYECOLOR_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/EyeColor/EyeColor_Backfill.csv*';

COPY INTO STG.VC_WEBCALLRARRGROUP_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    $4,
    $5
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/WebCallRarrGroup/WebCallRarrGroup_Backfill.csv*';

COPY INTO STG.VC_LOANPURPOSE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanPurpose/LoanPurpose_Backfill.csv*';

COPY INTO STG.VC_PAYMENTAUTHTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentAuthType/PaymentAuthType_Backfill.csv*';

COPY INTO STG.VC_INCOMETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    $2,
    $3,
    CASE WHEN $4 = 'True' THEN 1
    ELSE 0
    END, 
    to_timestamp_ntz($5,'MM/DD/YYYY HH12:MI:SS AM'),
    $6
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IncomeType/IncomeType_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERLEADSTATUSREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    CASE WHEN $1 = 'True' THEN NULL
    ELSE 0
    END, 
    CASE WHEN $2 = 'True' THEN NULL
    ELSE 0
    END,
    CASE WHEN $3 = 'True' THEN NULL
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerLeadStatusReason/CustomerLeadStatusReason_Backfill.csv*';

COPY INTO STG.VC_FORMLETTEREMAIL_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1, 
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterEmail/FormLetterEmail_Backfill.csv*';

COPY INTO STG.VC_PAYMENTACCOUNTEVENTTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentAccountEventType/PaymentAccountEventType_Backfill.csv*';

COPY INTO STG.VC_ACHREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHReason/ACHReason_Backfill.csv*';

COPY INTO STG.VC_CREDITREPORTINGDISPUTERESPONSECODE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CreditReportingDisputeResponseCode/CreditReportingDisputeResponseCode_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERLEADREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerLeadReason/CustomerLeadReason_Backfill.csv*';

COPY INTO STG.VC_ACHPROCESSINGTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHProcessingType/ACHProcessingType_Backfill.csv*';

COPY INTO STG.VC_CUSTOMERFLASHQUESTIONS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/CustomerFlashQuestions/CustomerFlashQuestions_Backfill.csv*';

COPY INTO STG.VC_IDENTIFICATIONTYPEAML_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/IdentificationTypeAML/IdentificationTypeAML_Backfill.csv*';

COPY INTO STG.VC_LOANCONFIGAPPLYPAYMENTORDER_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanConfigApplyPaymentOrder/LoanConfigApplyPaymentOrder_Backfill.csv*';

COPY INTO STG.VC_MIMETYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/MimeType/MimeType_Backfill.csv*';

COPY INTO STG.VC_VISITORNONMILITARYVERIFICATIONVAULTMGRAUTHORIZATION_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/VisitorNonMilitaryVerificationVaultMgrAuthorization/VisitorNonMilitaryVerificationVaultMgrAuthorization_Backfill.csv*';

COPY INTO STG.VC_FORMLETTERCONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    $3,
    $4,
    CASE WHEN $5 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    CASE WHEN $7 = 'True' THEN 1
    ELSE 0
    END
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/FormLetterConfig/FormLetterConfig_Backfill.csv*';

COPY INTO STG.VC_SKIPTRACECONFIG_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    CASE WHEN $2 = 'True' THEN 1
    ELSE 0
    END,
    $3,
    $4,
    $5,
    CASE WHEN $6 = 'True' THEN 1
    ELSE 0
    END,
    $7
    
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/SkipTraceConfig/SkipTraceConfig_Backfill.csv*';

COPY INTO STG.VC_BUMPUPREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2 
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BumpUpReason/BumpUpReason_Backfill.csv*';

COPY INTO STG.VC_INTERESTTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2 
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/InterestType/InterestType_Backfill.csv*';

COPY INTO STG.VC_LOANFUNDINGMETHOD_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2 
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanFundingMethod/LoanFundingMethod_Backfill.csv*';

COPY INTO STG.VC_LOANFUNDINGSTATUS_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2 
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/LoanFundingStatus/LoanFundingStatus_Backfill.csv*';

COPY INTO STG.VC_PENDINGREASONCLIENTMESSAGE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3,
    $4
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PendingReasonClientMessage/PendingReasonClientMessage_Backfill.csv*';

COPY INTO STG.VC_BANKCLASSIFICATIONTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    $3
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/BankClassificationType/BankClassificationType_Backfill.csv*';

COPY INTO STG.VC_PAYMENTSPASTDUETRANSACTIONTYPE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2,
    CASE WHEN $3 = 'True' THEN 1
    ELSE 0
    END
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PaymentsPastDueTransactionType/PaymentsPastDueTransactionType_Backfill.csv*';

COPY INTO STG.VC_PRESENTMENTREQUESTREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/PresentmentRequestReason/PresentmentRequestReason_Backfill.csv*';

COPY INTO STG.VC_REMOVEDREASON_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/RemovedReason/RemovedReason_Backfill.csv*';

COPY INTO STG.VC_SCORINGPENDINGREASONXREF_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    $2
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ScoringPendingReasonXref/ScoringPendingReasonXref_Backfill.csv*';

COPY INTO STG.VC_ACHQUEUE_HIST FROM
(
    select METADATA$FILENAME, CURRENT_TIMESTAMP(), to_date('2023-04-10'),
    $1,
    to_timestamp_ntz($2, 'MM/DD/YYYY HH12:MI:SS AM'),
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11
  
FROM @ETL.INBOUND
)
FILE_FORMAT = ( FORMAT_NAME = STG.LD_CSV_PIPE_SH1_EON_GZ)
pattern= 'inbound/VC/Backfill/ACHQueue/ACHQueue_Backfill.csv*';