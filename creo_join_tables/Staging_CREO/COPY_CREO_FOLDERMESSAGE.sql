SELECT 
    fm.[MESSAGE_KEY]
    ,fm.[FOLDER_KEY]
    ,fm.[LOCKED_BY]
    ,fm.[IS_READ]
FROM [dbo].FOLDERMESSAGE fm
LEFT JOIN [dbo].[MESSAGE] m ON fm.MESSAGE_KEY = m.MESSAGE_KEY
WHERE CAST(m.DATE_ENTERED AS DATE) = CAST('{asOfDate}' AS DATE);
