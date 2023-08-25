SELECT tr.RULE_KEY, tr.TEMPLATE_KEY
FROM [dbo].TEMPLATERULE tr
LEFT JOIN [dbo].TEMPLATE t
    ON tr.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].[RULE] r
    ON tr.RULE_KEY = r.RULE_KEY
WHERE CAST(r.DATE_ENTERED AS DATE) = CAST('{asOfDate}' AS DATE)
OR CAST(t.DATE_ENTERED AS DATE) = CAST('{asOfDate}' AS DATE);