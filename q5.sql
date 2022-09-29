/* Q5 a query that returns "how exactly the advertisers target the user; that is, you want to see the top ten advertisers and, for each of these ten advertisers, their top ten combinations of targeting type and targeting value".

The query should return a table with a two columns labeled "Advertiser" (string), "Criteria Type" (text), and "Criterion" (text).

The query should not modify the database and be a single SQL statement.  */

-- SELECT advertiserInfo AS Advertiser, count(*), targetingType AS "Criteria Type", targetingValue AS Criterion FROM impressions, matchedTargetingCriteria, TargetingCriteria
-- WHERE impressions.id = matchedTargetingCriteria.impression
--     AND TargetingCriteria.id = matchedTargetingCriteria.criteria
-- GROUP BY advertiserInfo
-- ORDER BY count(*) DESC
-- ;

SELECT advertiserInfo, targetingType, targetingValue, count(*) FROM impressions, TargetingCriteria, matchedTargetingCriteria
WHERE impressions.id = matchedTargetingCriteria.impression
    AND TargetingCriteria.id = matchedTargetingCriteria.criteria
    AND advertiserInfo IN 
        (SELECT advertiserInfo FROM impressions
        GROUP BY advertiserInfo
        ORDER BY count(*) DESC
        LIMIT 10)
GROUP BY advertiserInfo, TargetingCriteria.id
ORDER BY advertiserInfo, count(*) DESC
LIMIT 10
;

-- SELECT advertiserInfo FROM impressions
-- GROUP BY advertiserInfo
-- ORDER BY count(*) DESC
-- LIMIT 10
-- ;