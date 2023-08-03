# from export_mssql_to_csv import batch_size

custom_sql_queries = {
    'CREO_MessageDeliveryStatus_Batch': f"""
        SELECT
            MESSAGE_DELIVERY_STATUS_KEY
            ,MESSAGE_KEY
            ,DELIVERY_STATUS_KEY
            ,DATE_ENTERED
            -- REPLACE(DETAIL,CHAR(13)+CHAR(10),'; ') AS DETAIL
            ,Replace(Replace(DETAIL,CHAR(10),''),CHAR(13),'')
        FROM CREO.[dbo].[MessageDeliveryStatus]
        ORDER BY MESSAGE_DELIVERY_STATUS_KEY
        OFFSET 0 ROWS
        FETCH NEXT 10000 ROWS ONLY;
        """,

    'CREO_Message_Batch': f"""
        SELECT
            MESSAGE_KEY, SUBJECT, DATE_ENTERED, DATE_SENT, EXCEPTION, CONTAINER_KEY, COMMUNICATION_MAILING_KEY, TEMPLATE_KEY, IS_PRODUCTION, DATASET_ROW_KEY, SERVER, MESSAGE_ID, TYPE, NUM_EXCEPTIONS, DELIVERY_STATUS_KEY, IN_REPLY_TO_MESSAGE_KEY, DIRECTION, SEND_AFTER, SEND_AFTER_MESSAGE_KEY, IS_FINISHED, HEADERS, VENDOR_ID
            ,Replace(Replace(DETAIL,CHAR(10),''),CHAR(13),'')
        FROM CREO.[dbo].[MessageDeliveryStatus]
        ORDER BY MESSAGE_DELIVERY_STATUS_KEY
        OFFSET 0 ROWS
        FETCH NEXT 10000 ROWS ONLY;
        """,

    # 'key': 'value',

}

# 