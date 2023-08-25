class Utils:
    
    ## Name of Snowflake database
    SF_DATABASE = 'CREO'

    ## MSSQL database(s) and table names -- Format: ('database_name', ['column_name', 'column_name', ... ])
    #  - - - - - - - - - - - - - - - - - - - - 
    # CREO_BATCH1 = ('CREO', ['ApprovalRequest', 'ApprovalRequestItem', 'Campaign', 'CampaignType', 'Communication', 'CommunicationMailing', 'Config', 'ConfigHistory', 'ContactType', 'Container', 'Dataset', 'DatasetColumn', 'Datasource', 'DeadMessages', 'DeadMessages2', 'DeliveryStatus', 'Emoji', 'Folder', 'FolderContact', 'FolderMessage', 'Global', 'Log', 'MessageContact', 'MessageContactType', 'MessagePart', 'MessageStatusQueue', 'MessageType', 'Package', 'Parameter', 'Rule', 'Template', 'TemplateRule', 'TemplateType', 'TempMessage', 'User', 'WebHook'])
    # CREO_BATCH2 = ('CREO', [ 'Contact', 'DatasetCell', 'DatasetRow', 'DatasetValue', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2', 'PackageTemplate' ])
    # CREOArchive_BATCH1 = ('CREOArchive', ['DatasetCell', 'DatasetRow', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2'])
    # CREOArchive2_BATCH1 = ('CREOArchive2', ['DatasetCell', 'DatasetRow', 'Message', 'MessageContactV2', 'MessageDeliveryStatus', 'MessagePartV2'])

    ## List of MSSQL databases/tables to run in current batch
    #  - - - - - - - - - - - - - - - - - - - - 
    # BATCH = [ ('CREO', ['Contact', 'Message', 'MessageDeliveryStatus']) ]
    # BATCH = [ ('CREO', ['Contact']) ]
    # BATCH = [ ('CREO', ['MessageDeliveryStatus']) ]  #! ended at batch 754, start at 755
    # BATCH = [ ('CREO', ['Message']) ]

    # BATCH = [ ('CREO', ['Contact', 'DatasetValue', 'Message', 'MessageDeliveryStatus', 'MessagePartV2'] ) ]
    
    BATCH = [ ('CREO', ['DatasetValue'] ) ] ## finished at 215 on 7/26

    ## (optional) Stores approx row counts for each table, to skip selecting row count in the export CSV script
    #  - - - - - - - - - - - - - - - - - - - - 
    ROW_COUNTS = {
        'Contact': 22585141,
        'DatasetValue': 795207736,
        'Message': 15847177,
        'MessageDeliveryStatus': 52423259,
        'MessagePartV2': 1584766,
    }

    ## (optional) Specify optimal batch size for a given table
    #  - - - - - - - - - - - - - - - - - - - - 
    BATCH_SIZES = {
        'Contact': 5000000,
        'DatasetValue': 1500000,
        'Message': 1000000,
        'MessageDeliveryStatus': 2100000,
        'MessagePartV2': 10000,
    }
    
    ## (optional) Specify which batch number to start the export at for a given table
    #  - - - - - - - - - - - - - - - - - - - - 
    START_IDX = {
        'DatasetValue': 216,
    }
