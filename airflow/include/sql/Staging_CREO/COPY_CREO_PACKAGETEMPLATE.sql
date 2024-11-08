/*
[dbo].PACKAGETEMPLATE
PACKAGE_KEY,TEMPLATE_KEY
Join PACKAGE on PACKAGE_KEY, get DATE_ENTERED
Join TEMPLATE on TEMPLATE_KEY, get DATE_ENTERED

SELECT COUNT(*) -- 2243377
    -- pt.PACKAGE_KEY, 
    -- pt.TEMPLATE_KEY,
    -- t.DATE_ENTERED AS template_date,
    -- p.DATE_ENTERED AS package_date
FROM [dbo].PACKAGETEMPLATE pt
LEFT JOIN [dbo].TEMPLATE t
    ON pt.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].PACKAGE p
    ON pt.PACKAGE_KEY = p.PACKAGE_KEY
WHERE pt.PACKAGE_KEY IS NOT NULL 
AND pt.TEMPLATE_KEY IS NOT NULL;
*/

SELECT pt.PACKAGE_KEY, pt.TEMPLATE_KEY
FROM [dbo].PACKAGETEMPLATE pt
LEFT JOIN [dbo].TEMPLATE t
    ON pt.TEMPLATE_KEY = t.TEMPLATE_KEY
LEFT JOIN [dbo].PACKAGE p
    ON pt.PACKAGE_KEY = p.PACKAGE_KEY
WHERE CAST([p.DATE_ENTERED] AS DATE) = CAST('{asOfDate}' AS DATE)
OR CAST([t.DATE_ENTERED] AS DATE) = CAST('{asOfDate}' AS DATE);
