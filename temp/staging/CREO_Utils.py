AS_OF_DATE = "2023-07-15"

fullTableList = {
  "APPROVALREQUEST": {
    "SQL": None,
    "KEY_COLUMN": "APPROVAL_REQUEST_KEY",
    "KEYS": [
      "PACKAGE_KEY",
      "APPROVAL_REQUEST_KEY"
    ],
    "TABLE_FILTERED_BY": "ENTERED_AT"
  },
  "APPROVALREQUESTITEM": {
    "SQL": None,
    "KEY_COLUMN": "APPROVAL_REQUEST_ITEM_KEY",
    "KEYS": [
      "TEMPLATE_KEY",
      "APPROVAL_REQUEST_KEY",
      "APPROVAL_REQUEST_ITEM_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "CAMPAIGN": {
    "SQL": None,
    "KEY_COLUMN": "CAMPAIGN_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "CAMPAIGN_TYPE_KEY",
      "CAMPAIGN_KEY",
      "SEED_LIST_DATASET_KEY",
      "DATASET_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "CAMPAIGNTYPE": {
    "SQL": None,
    "KEY_COLUMN": "CAMPAIGN_TYPE_KEY",
    "KEYS": [
      "CAMPAIGN_TYPE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "COMMUNICATION": {
    "SQL": None,
    "KEY_COLUMN": "COMMUNICATION_KEY",
    "KEYS": [
      "CAMPAIGN_KEY",
      "COMMUNICATION_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "COMMUNICATIONMAILING": {
    "SQL": None,
    "KEY_COLUMN": "COMMUNICATION_MAILING_KEY",
    "KEYS": [
      "COMMUNICATION_MAILING_KEY",
      "COMMUNICATION_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_COMPLETED"
  },
  "CONFIG": {
    "SQL": None,
    "KEY_COLUMN": "CONFIG_KEY",
    "KEYS": [
      "CONFIG_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "CONFIGHISTORY": {
    "SQL": None,
    "KEY_COLUMN": "CONFIG_HISTORY_KEY",
    "KEYS": [
      "CONFIG_HISTORY_KEY",
      "CONFIG_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "CONTACT": {
    "SQL": None,
    "KEY_COLUMN": "CONTACT_KEY",
    "KEYS": [
      "CONTACT_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "CONTACTTYPE": {
    "SQL": None,
    "KEY_COLUMN": "CONTACT_TYPE_KEY",
    "KEYS": [
      "CONTACT_TYPE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "CONTAINER": {
    "SQL": None,
    "KEY_COLUMN": "CONTAINER_KEY",
    "KEYS": [
      "CONTAINER_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "DATASET": {
    "SQL": None,
    "KEY_COLUMN": "DATASET_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "DATASET_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "DATASETCELL": {
    "SQL": None,
    "KEY_COLUMN": "DATASET_COLUMN_KEY",
    "KEYS": [
      "DATASET_VALUE_KEY",
      "DATASET_ROW_KEY",
      "DATASET_COLUMN_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "DATASETCOLUMN": {
    "SQL": None,
    "KEY_COLUMN": "DATASET_COLUMN_KEY",
    "KEYS": [
      "DATASET_COLUMN_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "DATASETROW": {
    "SQL": None,
    "KEY_COLUMN": "DATASET_ROW_KEY",
    "KEYS": [
      "DATASET_ROW_KEY",
      "DATASET_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  # "DATASETVALUE": {
  #   "SQL": None,
  #   "KEY_COLUMN": "DATASET_VALUE_KEY",
  #   "KEYS": [
  #     "DATASET_VALUE_KEY"
  #   ],
  #   "TABLE_FILTERED_BY": None
  # },
  "DATASOURCE": {
    "SQL": None,
    "KEY_COLUMN": "DATASOURCE_KEY",
    "KEYS": [
      "DATASOURCE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "DEADMESSAGES": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "TEMPLATE_KEY",
      "SEND_AFTER_MESSAGE_KEY",
      "DELIVERY_STATUS_KEY",
      "IN_REPLY_TO_MESSAGE_KEY",
      "COMMUNICATION_MAILING_KEY",
      "MESSAGE_KEY",
      "DATASET_ROW_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "DEADMESSAGES2": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "TEMPLATE_KEY",
      "SEND_AFTER_MESSAGE_KEY",
      "DELIVERY_STATUS_KEY",
      "IN_REPLY_TO_MESSAGE_KEY",
      "COMMUNICATION_MAILING_KEY",
      "MESSAGE_KEY",
      "DATASET_ROW_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "DELIVERYSTATUS": {
    "SQL": None,
    "KEY_COLUMN": "DELIVERY_STATUS_KEY",
    "KEYS": [
      "DELIVERY_STATUS_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "EMOJI": {
    "SQL": None,
    "KEY_COLUMN": "EMOJI_KEY",
    "KEYS": [
      "EMOJI_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "FOLDER": {
    "SQL": None,
    "KEY_COLUMN": "FOLDER_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "PARENT_FOLDER_KEY",
      "FOLDER_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "FOLDERCONTACT": {
    "SQL": None,
    "KEY_COLUMN": None,
    "KEYS": [
      "CONTACT_KEY",
      "FOLDER_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "FOLDERMESSAGE": {
    "SQL": None,
    "KEY_COLUMN": None,
    "KEYS": [
      "MESSAGE_KEY",
      "FOLDER_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "GLOBAL": {
    "SQL": None,
    "KEY_COLUMN": None,
    "KEYS": [
        "APP_VERSION"
      ],
    "TABLE_FILTERED_BY": None
  },
  "LOG": {
    "SQL": None,
    "KEY_COLUMN": "LOG_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "PACKAGE_KEY",
      "TEMPLATE_KEY",
      "CAMPAIGN_KEY",
      "COMMUNICATION_KEY",
      "MESSAGE_KEY",
      "LOG_KEY",
      "RULE_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "MESSAGE": {
    "SQL": "COPY_CREO_MESSAGE.sql",
    "KEY_COLUMN": "MESSAGE_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "TEMPLATE_KEY",
      "SEND_AFTER_MESSAGE_KEY",
      "DELIVERY_STATUS_KEY",
      "IN_REPLY_TO_MESSAGE_KEY",
      "COMMUNICATION_MAILING_KEY",
      "MESSAGE_KEY",
      "DATASET_ROW_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "MESSAGECONTACT": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_CONTACT_KEY",
    "KEYS": [
      "CONTACT_KEY",
      "MESSAGE_KEY",
      "MESSAGE_CONTACT_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "MESSAGECONTACTTYPE": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_CONTACT_TYPE_KEY",
    "KEYS": [
      "MESSAGE_CONTACT_TYPE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "MESSAGECONTACTV2": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_CONTACT_KEY",
    "KEYS": [
      "CONTACT_KEY",
      "MESSAGE_KEY",
      "MESSAGE_CONTACT_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "MESSAGEDELIVERYSTATUS": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_DELIVERY_STATUS_KEY",
    "KEYS": [
      "MESSAGE_KEY",
      "MESSAGE_DELIVERY_STATUS_KEY",
      "DELIVERY_STATUS_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "MESSAGEPART": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_PART_KEY",
    "KEYS": [
      "MESSAGE_PART_KEY",
      "MESSAGE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  # "MESSAGEPARTV2": {
  #   "SQL": None,
  #   "KEY_COLUMN": "MESSAGE_PART_KEY",
  #   "KEYS": [
  #     "MESSAGE_PART_KEY",
  #     "MESSAGE_KEY"
  #   ],
  #   "TABLE_FILTERED_BY": None
  # },
  "MESSAGESTATUSQUEUE": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_STATUS_QUEUE_KEY",
    "KEYS": [
      "MESSAGE_STATUS_QUEUE_KEY",
      "CONTAINER_KEY"
    ],
    "TABLE_FILTERED_BY": "ENTERED_AT"
  },
  "MESSAGETYPE": {
    "SQL": None,
    "KEY_COLUMN": "MESSAGE_TYPE_KEY",
    "KEYS": [
      "MESSAGE_TYPE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "PACKAGE": {
    "SQL": None,
    "KEY_COLUMN": "PACKAGE_KEY",
    "KEYS": [
      "PACKAGE_KEY",
      "CONTAINER_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "PACKAGETEMPLATE": {
    "SQL": None,
    "KEY_COLUMN": "PACKAGE_KEY",
    "KEYS": [
      "PACKAGE_KEY",
      "TEMPLATE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "PARAMETER": {
    "SQL": None,
    "KEY_COLUMN": "PARAMETER_KEY",
    "KEYS": [
      "PARAMETER_KEY",
      "DATASOURCE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "RULE": {
    "SQL": None,
    "KEY_COLUMN": "RULE_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "RULE_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "TEMPLATE": {
    "SQL": None,
    "KEY_COLUMN": "TEMPLATE_KEY",
    "KEYS": [
      "TEMPLATE_KEY",
      "BASE_TEMPLATE_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "TEMPLATERULE": {
    "SQL": None,
    "KEY_COLUMN": "RULE_KEY",
    "KEYS": [
      "TEMPLATE_KEY",
      "RULE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "TEMPLATETYPE": {
    "SQL": None,
    "KEY_COLUMN": "TEMPLATE_TYPE_KEY",
    "KEYS": [
      "TEMPLATE_TYPE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "TEMPMESSAGE": {
    "SQL": None,
    "KEY_COLUMN": "TEMP_MESSAGE_KEY",
    "KEYS": [
      "TEMP_MESSAGE_KEY"
    ],
    "TABLE_FILTERED_BY": None
  },
  "USER": {
    "SQL": None,
    "KEY_COLUMN": "USER_KEY",
    "KEYS": [
      "USER_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  },
  "WEBHOOK": {
    "SQL": None,
    "KEY_COLUMN": "WEBHOOK_KEY",
    "KEYS": [
      "CONTAINER_KEY",
      "WEBHOOK_KEY"
    ],
    "TABLE_FILTERED_BY": "DATE_ENTERED"
  }
}
