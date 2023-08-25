fullTableList = {
	"CREO_APPROVALREQUEST_HIST": {
		"sql": None,
		"key_column": "APPROVAL_REQUEST_KEY",
		"keys": "{'APPROVAL_REQUEST_KEY', 'PACKAGE_KEY'}",
		"table_filtered_by": "ENTERED_AT"
	},
	"CREO_APPROVALREQUESTITEM_HIST": {
		"sql": None,
		"key_column": "APPROVAL_REQUEST_ITEM_KEY",
		"keys": "{'APPROVAL_REQUEST_KEY', 'APPROVAL_REQUEST_ITEM_KEY', 'TEMPLATE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_CAMPAIGN_HIST": {
		"sql": None,
		"key_column": "CAMPAIGN_KEY",
		"keys": "{'CAMPAIGN_TYPE_KEY', 'CONTAINER_KEY', 'SEED_LIST_DATASET_KEY', 'CAMPAIGN_KEY', 'DATASET_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_CAMPAIGNTYPE_HIST": {
		"sql": None,
		"key_column": "CAMPAIGN_TYPE_KEY",
		"keys": "{'CAMPAIGN_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_COMMUNICATION_HIST": {
		"sql": None,
		"key_column": "COMMUNICATION_KEY",
		"keys": "{'CAMPAIGN_KEY', 'COMMUNICATION_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_COMMUNICATIONMAILING_HIST": {
		"sql": None,
		"key_column": "COMMUNICATION_MAILING_KEY",
		"keys": "{'COMMUNICATION_KEY', 'COMMUNICATION_MAILING_KEY'}",
		"table_filtered_by": "DATE_COMPLETED"
	},
	"CREO_CONFIG_HIST": {
		"sql": None,
		"key_column": "CONFIG_KEY",
		"keys": "{'CONFIG_KEY'}",
		"table_filtered_by": None
	},
	"CREO_CONFIGHISTORY_HIST": {
		"sql": None,
		"key_column": "CONFIG_HISTORY_KEY",
		"keys": "{'CONFIG_KEY', 'CONFIG_HISTORY_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_CONTACT_HIST": {
		"sql": None,
		"key_column": "CONTACT_KEY",
		"keys": "{'CONTACT_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_CONTACTTYPE_HIST": {
		"sql": None,
		"key_column": "CONTACT_TYPE_KEY",
		"keys": "{'CONTACT_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_CONTAINER_HIST": {
		"sql": None,
		"key_column": "CONTAINER_KEY",
		"keys": "{'CONTAINER_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DATASET_HIST": {
		"sql": None,
		"key_column": "DATASET_KEY",
		"keys": "{'CONTAINER_KEY', 'DATASET_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DATASETCELL_HIST": {
		"sql": None,
		"key_column": "DATASET_ROW_KEY",
		"keys": "{'DATASET_VALUE_KEY', 'DATASET_COLUMN_KEY', 'DATASET_ROW_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASETCOLUMN_HIST": {
		"sql": None,
		"key_column": "DATASET_COLUMN_KEY",
		"keys": "{'DATASET_COLUMN_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASETROW_HIST": {
		"sql": None,
		"key_column": "DATASET_ROW_KEY",
		"keys": "{'DATASET_KEY', 'DATASET_ROW_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASETVALUE_HIST": {
		"sql": None,
		"key_column": "DATASET_VALUE_KEY",
		"keys": "{'DATASET_VALUE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DATASOURCE_HIST": {
		"sql": None,
		"key_column": "DATASOURCE_KEY",
		"keys": "{'DATASOURCE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_DEADMESSAGES_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'DELIVERY_STATUS_KEY', 'CONTAINER_KEY', 'SEND_AFTER_MESSAGE_KEY', 'COMMUNICATION_MAILING_KEY', 'IN_REPLY_TO_MESSAGE_KEY', 'TEMPLATE_KEY', 'DATASET_ROW_KEY', 'MESSAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DEADMESSAGES2_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'DELIVERY_STATUS_KEY', 'CONTAINER_KEY', 'SEND_AFTER_MESSAGE_KEY', 'COMMUNICATION_MAILING_KEY', 'IN_REPLY_TO_MESSAGE_KEY', 'TEMPLATE_KEY', 'DATASET_ROW_KEY', 'MESSAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_DELIVERYSTATUS_HIST": {
		"sql": None,
		"key_column": "DELIVERY_STATUS_KEY",
		"keys": "{'DELIVERY_STATUS_KEY'}",
		"table_filtered_by": None
	},
	"CREO_EMOJI_HIST": {
		"sql": None,
		"key_column": "EMOJI_KEY",
		"keys": "{'EMOJI_KEY'}",
		"table_filtered_by": None
	},
	"CREO_FOLDER_HIST": {
		"sql": None,
		"key_column": "FOLDER_KEY",
		"keys": "{'FOLDER_KEY', 'CONTAINER_KEY', 'PARENT_FOLDER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_FOLDERCONTACT_HIST": {
		"sql": None,
		"key_column": "CONTACT_KEY",
		"keys": "{'CONTACT_KEY', 'FOLDER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_FOLDERMESSAGE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'MESSAGE_KEY', 'FOLDER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_GLOBAL_HIST": {
		"sql": None,
		"key_column": "APP_VERSION",
		"keys": "set()",
		"table_filtered_by": None
	},
	"CREO_LOG_HIST": {
		"sql": None,
		"key_column": "LOG_KEY",
		"keys": "{'CONTAINER_KEY', 'RULE_KEY', 'CAMPAIGN_KEY', 'TEMPLATE_KEY', 'PACKAGE_KEY', 'COMMUNICATION_KEY', 'MESSAGE_KEY', 'LOG_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_MESSAGE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_KEY",
		"keys": "{'DELIVERY_STATUS_KEY', 'CONTAINER_KEY', 'SEND_AFTER_MESSAGE_KEY', 'COMMUNICATION_MAILING_KEY', 'IN_REPLY_TO_MESSAGE_KEY', 'TEMPLATE_KEY', 'DATASET_ROW_KEY', 'MESSAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_MESSAGECONTACT_HIST": {
		"sql": None,
		"key_column": "MESSAGE_CONTACT_KEY",
		"keys": "{'MESSAGE_CONTACT_KEY', 'MESSAGE_KEY', 'CONTACT_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGECONTACTTYPE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_CONTACT_TYPE_KEY",
		"keys": "{'MESSAGE_CONTACT_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGECONTACTV2_HIST": {
		"sql": None,
		"key_column": "MESSAGE_CONTACT_KEY",
		"keys": "{'MESSAGE_CONTACT_KEY', 'MESSAGE_KEY', 'CONTACT_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGEDELIVERYSTATUS_HIST": {
		"sql": None,
		"key_column": "MESSAGE_DELIVERY_STATUS_KEY",
		"keys": "{'MESSAGE_KEY', 'MESSAGE_DELIVERY_STATUS_KEY', 'DELIVERY_STATUS_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_MESSAGEPART_HIST": {
		"sql": None,
		"key_column": "MESSAGE_PART_KEY",
		"keys": "{'MESSAGE_KEY', 'MESSAGE_PART_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGEPARTV2_HIST": {
		"sql": None,
		"key_column": "MESSAGE_PART_KEY",
		"keys": "{'MESSAGE_KEY', 'MESSAGE_PART_KEY'}",
		"table_filtered_by": None
	},
	"CREO_MESSAGESTATUSQUEUE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_STATUS_QUEUE_KEY",
		"keys": "{'CONTAINER_KEY', 'MESSAGE_STATUS_QUEUE_KEY'}",
		"table_filtered_by": "ENTERED_AT"
	},
	"CREO_MESSAGETYPE_HIST": {
		"sql": None,
		"key_column": "MESSAGE_TYPE_KEY",
		"keys": "{'MESSAGE_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_PACKAGE_HIST": {
		"sql": None,
		"key_column": "PACKAGE_KEY",
		"keys": "{'CONTAINER_KEY', 'PACKAGE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_PACKAGETEMPLATE_HIST": {
		"sql": None,
		"key_column": "PACKAGE_KEY",
		"keys": "{'TEMPLATE_KEY', 'PACKAGE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_PARAMETER_HIST": {
		"sql": None,
		"key_column": "PARAMETER_KEY",
		"keys": "{'DATASOURCE_KEY', 'PARAMETER_KEY'}",
		"table_filtered_by": None
	},
	"CREO_RULE_HIST": {
		"sql": None,
		"key_column": "RULE_KEY",
		"keys": "{'RULE_KEY', 'CONTAINER_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_TEMPLATE_HIST": {
		"sql": None,
		"key_column": "TEMPLATE_KEY",
		"keys": "{'TEMPLATE_KEY', 'BASE_TEMPLATE_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_TEMPLATERULE_HIST": {
		"sql": None,
		"key_column": "TEMPLATE_KEY",
		"keys": "{'RULE_KEY', 'TEMPLATE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_TEMPLATETYPE_HIST": {
		"sql": None,
		"key_column": "TEMPLATE_TYPE_KEY",
		"keys": "{'TEMPLATE_TYPE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_TEMPMESSAGE_HIST": {
		"sql": None,
		"key_column": "TEMP_MESSAGE_KEY",
		"keys": "{'TEMP_MESSAGE_KEY'}",
		"table_filtered_by": None
	},
	"CREO_USER_HIST": {
		"sql": None,
		"key_column": "USER_KEY",
		"keys": "{'USER_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	},
	"CREO_WEBHOOK_HIST": {
		"sql": None,
		"key_column": "WEBHOOK_KEY",
		"keys": "{'CONTAINER_KEY', 'WEBHOOK_KEY'}",
		"table_filtered_by": "DATE_ENTERED"
	}
}
