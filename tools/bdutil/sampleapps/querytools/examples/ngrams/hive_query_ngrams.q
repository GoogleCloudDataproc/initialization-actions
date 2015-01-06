--
-- Copyright 2013 Google Inc. All Rights Reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
--

--
-- This script is intended to be run from the Hive shell:
--
--   hive> source hive_query_ngrams.q;
--
-- or from the operating system shell:
--
--   $ hive -f hive_query_ngrams.q
--
-- The result of this query is a table of records indicating the count
-- of occurrences of the words "radio" and "television" in the Google
-- ngrams corpora for each year since 1920.
--
-- This query ensures that a record exists in the result for every year
-- since 1920, even if there were no instances of a given word.
-- In practice this is unnecessary as radio and television both occur
-- more than once in the data set for every year since 1920.
--
-- The structure of this query is to join three distinct subqueries (on year):
--    y: list of years since 1920 (implicitly ordered by the DISTINCT operation)
--    r: sum of instances of the word "radio" for each year since 1920
--    t: sum of instances of the word "television" for each year since 1920
--

SELECT y.year AS year,
       r.instance_count AS radio, t.instance_count AS television,
       CAST(r.instance_count AS DOUBLE)/(r.instance_count + t.instance_count)
        AS pct
FROM
 (SELECT DISTINCT year AS year FROM
    (SELECT distinct year from 1gram where prefix = 'r' and year >= 1920
     UNION ALL
     SELECT distinct year from 1gram where prefix = 't' and year >= 1920) y_all)
    y
JOIN
 (SELECT LOWER(word) AS ngram_col, year, SUM(instance_count) AS instance_count
  FROM 1gram
  WHERE LOWER(word) = 'radio' AND prefix='r' AND (year >= 1920)
  GROUP BY LOWER(word), year) r
ON y.year = r.year
JOIN
 (SELECT LOWER(word) AS ngram_col, year, SUM(instance_count) AS instance_count
  FROM 1gram
  WHERE LOWER(word) = 'television' AND prefix='t' AND (year >= 1920)
  GROUP BY LOWER(word), year) t
ON y.year = t.year
ORDER BY year;

EXIT;

--
-- This is a simplified version of the above which eliminates the explicit
-- generation of the "year" list.  It assumes (correctly) that the word
-- "television" appears every year that "radio" does.
-- This query is listed here for reference and educational purposes only.
--
-- SELECT a.year, a.instance_count, b.instance_count,
--        CAST(a.instance_count AS DOUBLE)/(a.instance_count + b.instance_count)
-- FROM
--  (SELECT LOWER(word) AS ngram_col, year, SUM(instance_count) AS instance_count
--   FROM 1gram
--   WHERE LOWER(word) = 'radio' AND prefix='r' AND (year >= 1920)
--   GROUP BY LOWER(word), year) a
-- JOIN
--  (SELECT LOWER(word) AS ngram_col, year, SUM(instance_count) AS instance_count
--   FROM 1gram
--   WHERE LOWER(word) = 'television' AND prefix='t' AND (year >= 1920)
--   GROUP BY LOWER(word), year) b
-- ON a.year = b.year
-- ORDER BY year;
--
