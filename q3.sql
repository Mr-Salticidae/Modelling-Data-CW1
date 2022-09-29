/* Q3 is a query that returns "how the advertisers target the user; i.e., you want to see the top ten targeting types and the number of ads of that type".

The query should return a table with a two columns labeled "Criteria Category" (text) and "Ad Count" (integer).

The query should not modify the database and be a single SQL statement. */
   
SELECT targetingType AS "Criteria Category", count(*) AS "Ad Count" FROM matchedTargetingCriteria, TargetingCriteria 
WHERE matchedTargetingCriteria.criteria = TargetingCriteria.id
GROUP BY targetingType
ORDER BY "Ad Count" DESC
LIMIT 10
;