SELECT 
LOCATION,
COMPANY,
PHONE,
AREA_CODE,
MARKET_KEY,
IS_COLLECTIONS,
IS_INTERNET,
IS_AUTOPRINT_MO,
IS_AUTOPRINT_RECEIPT,
IS_AUTOPRINT_CTR,
ENABLE_DIALER,
ENABLE_VAULT_MGR,
ENABLE_CCARD,
ENABLE_NPA_REFERRAL,
EMAIL_ADDR,
EMAIL_SMTP,
DEF_SURCHARGE,
ACCTG_TELLER,
PASSWORD_EXP_DAYS,
AUTH_CUST_DAY_LIMIT,
AUTH_ISS_DAY_LIMIT,
APP_RENEWAL_DAYS,
REQ_FIELDS_DAYS_LAST_LOAN,
PRINT_CHK_FEE_NOTICE,
CHK_FEE_LAST_UPDATE,
RECEIPT_COPIES,
RECEIPT_DUNNING_MSG,
CURR_VAULT_MGR,
SOLOMON_ID,
SOLOMON_TITLE_EXISTS,
SOLOMON_TITLE_ID,
IS_SDN_CUSTOMER,
IS_SDN_ISSUER,
EMAIL_TEMPLATE_PATH,
DATE_ENTERED,
LONGITUDE,
LATITUDE,
STORE_HOURS,
IS_SEND_EMAIL,
ENABLE_VEREPAY,
VEREPAY_TERMINAL_ID,
VEREPAY_STORE_KEY,
IS_CORPORATE,
ENABLE_STATE_DB_REPORTING,
SOLOMON_NONCASH_EXISTS,
SOLOMON_NONCASH_ID,
COUNTRY_CODE,
BUSINESS_STATE,
DISTRICT_KEY,
ENABLE_OPTPLUS,
ADDRESS1,
CITY,
STATE,
ZIPCODE,
COUNTY,
COUNTRY,
ENABLE_LABOR_MODEL,
ADDRESS_FORMAT,
ADDR_STREET,
FLAT_NUM,
BUILDING_NUM,
BUILDING_NAME,
ADDRESS_LINE,
ADDRESS_CSZ,
ADDRESS_LINE_3,
YEARS_GOOD_ADDRESS,
REQD_PREVIOUS_ADDRESS_COUNT,
ENABLE_CURRENCY_EXCHANGE,
CR_TELLER,
ENABLE_PICTURES,
RETAKE_PICTURE_DAYS,
BASE_CURRENCY_KEY,
ADDRESS_LINE_1,
ADDRESS_LINE_2,
INCOME_VERIFICATION_DAYS,
ENABLE_INCOME_CALCULATOR,
VERIFICATION_DOCS_REQUIRED,
ENABLE_EXPERIAN_DEBIT_CARD_VERIFICATION,
ENABLE_DCARD_PAYMENT_FEE,
DCARD_PAYMENT_FEE_AMT,
ENABLE_EASYPAY_WAREHOUSE,
SALES_TAX_PERCENT,
ALTERNATE_LENDER_KEY,
ENABLE_DCARD_FRAUD_CHECK,
ENABLE_CHECK_IMAGING,
GROSS_TO_NET_INCOME_PCT,
WESTERN_UNION_BILLPAY_CONNECTORID,
USE_MONEYGRAM_MO,
USE_BLACKBOOK,
STORE_IS_OPEN,
CAB_LENDER_KEY,
ENABLE_CAP_WITHDRAWAL,
ENABLE_CURRENCY_EXCHANGE_RATE_CHANGE,
ENABLE_TELLER_PWD_SELF_RESET,
UTC_OFFSET,
UTC_DST_OFFSET,
TIME_ZONE,
ENABLE_CHECK_AMT_VERIFY,
ENABLE_ESIGN,
ACH_COOLING_OFF_PERIOD_DAYS,
ENABLE_REQUIRES_JOB_TYPE,
IS_AUTO_EOD,
ENABLE_CHECK_CASHING,
ENABLE_MATCH_DUECYCLE,
ACH_GROUP_KEY,
BOA_CLIENT_ID,
DAYS_BEFORE_CHECK_SHRED,
ENABLE_PRINT_CREDIT_AVAILABLE,
ENABLE_PHONE_DEPOSITS,
CHECK_CASHING_GOVT_ID_REQUIRED,
ENABLE_OPTPLUS_LOAN_FUNDING,
ENABLE_MOBILE_APP_MSG,
ENABLE_AVS_VALIDATION_DECLINES,
STORE_NICKNAME,
ENABLE_INTERNAL_AUDIT,
STORE_OPENED_DATE,
CURRENCY_EXCHANGE_NON_CUSTOMER_LIMIT,
INTER_STORE_AR_ACCT,
INTER_STORE_AP_ACCT,
INTER_CO_NONCASH_AR_ACCT,
INTER_CO_NONCASH_AP_ACCT,
INTER_CO_TITLE_AR_ACCT,
INTER_CO_TITLE_AP_ACCT,
GL_ACCT_LOCATION_GROUP_KEY,
SHOW_TITLE_PAID_AT_OTHER_LOCATIONS,
ENABLE_STORE_CREDIT_FOR_INTERNET_LOANS,
BUSINESS_ENTITY,
AIM_MIN_LOAN_RECORDS,
AIM_MIN_DM_RECORDS,
ENABLE_CASHED_CHECK_BARCODE_PAGE_PRINT,
ENABLE_TELLER_CASH_COUNT,
UPDATE_OPEN_LOANS_WITH_NEW_CARD,
ENABLE_STATE_NEW_APP_REQUIRED,
ENABLE_WIRE_TRANSFER_MATCHING,
ENABLE_FOREIGN_ADDRESSES,
MAX_COLLECTIONS_ATTEMPTS_PER_REPRESENTMENT_ENABLED,
MAX_COLLECTIONS_ATTEMPTS_SPAY,
MAX_COLLECTIONS_ATTEMPTS_MPAY,
MAX_COLLECTIONS_ATTEMPTS_OEND,
ENABLE_DECISION_LOGIC,
ENABLE_FLINKS,
CUSTOMER_CARD_COOLING_OFF_DAYS,
CARD_COOLING_OFF_DAYS,
CARD_GOVERNOR_RULE_DEF_SET_KEY,
IS_CARD_VALIDATION_REQUIRED,
ENABLE_DOD_MANUAL_VERIFICATION,
DOD_VERIFICATION_EXPIRATION_DAYS,
ICL_CLIENT_ID,
DEPOSIT_ACCOUNT,
CHECK_ENDORSEMENT,
ENABLE_SINGLE_CABINET_SCANNING,
ALLOW_VIEW_CONFIRMATION_NUMBER,
TITLE_PAID_REPORT_DAYS_BACK,
TITLE_PAID_REPORT_INCLUDE_OWN_LOCATION,
RECEIPT_PAYMENT_SCHEDULE_CNT,
ENABLE_PHOTO_THUMBNAIL,
PAYMENT_AUTHORIZATION_DOCUMENT_REQUIREMENT_KEY,
MAX_ACH_ATTEMPTS_REMOTE_CASHED_CHECKS,
ENABLE_SHARE_CITO_NOTES_IN_CURO,
ENABLE_BANKING_REPORT,
ALLOW_PAYMENT_ON_RESTRICTED_AUDIT_CATEGORIES,
ENABLE_INTERAC_INTERNET,
ALLOW_PRODUCT_SELECTION_BEFORE_LOAN_APP,
ENABLE_CREDIT_CONSENT_MESSAGE,
DEBIT_CARD_FUNDINGS_PER_DAY_AND_CUSTOMER_FOR_CARD,
MONEY_ORDER_MAX_AMOUNT_PER_DAY_PER_LOCATION,
DEFAULT_COMPANY_BANK_ACCOUNT_KEY,
DEFAULT_AA_COMPANY_BANK_ACCOUNT_KEY,
ENABLE_CURO_LOAN_APPLICATION,
ENABLE_CURO_COMPLETE_APPROVED_LOAN_APPLICATION,
ENABLE_REVOLVE_LOAN_FUNDING,
IS_CASHLESS,
ENABLE_INCOME_VERIFICATION,
ENABLE_PARTNER_REFERRAL 
FROM dbo.Company
WHERE CAST(DATE_ENTERED AS DATE) = CAST('{asOfDate}' as DATE)