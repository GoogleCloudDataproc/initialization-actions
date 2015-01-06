/*
  Copyright 2013 Google Inc. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
*/

/*
  This script is intended to be run from the Pig shell (grunt):

    grunt> exec pig_query_ngrams.pig

  or from the operating system shell:

    $ pig -f pig_query_ngrams.q

  The result of this pipeline is a relation of tuples indicating the count
  of occurrences of the words "radio" and "television" in the Google
  ngrams corpora for each year since 1920.

  This pipeline ensures that a record exists in the result for every year
  since 1920, even if there were no instances of a given word.
  In practice this is unnecessary as radio and television both occur
  more than once in the data set for every year since 1920.
*/

/* Default directory should be /user/<hdpuser> */

/* Load the "r" records */
data1 = LOAD './ngrams/1gram/googlebooks-eng-all-1gram-20120701-r'
        USING PigStorage()
        AS (ngram:CHARARRAY, year:INT, instance_count:INT, book_count:INT);

/* Filter only records for "radio" */
flt1 = FILTER data1 BY LOWER(ngram) == 'radio' AND year >= 1920;

/* Group all instances of [Rr]adio by year */
grp1 = GROUP flt1 BY (LOWER(ngram), year);

/* Sum the count of occurrences (by year) */
res1 = FOREACH grp1 GENERATE FLATTEN(group),
                    SUM(flt1.instance_count) AS instance_count:LONG;


/* Load the "t" records */
data2 = LOAD './ngrams/1gram/googlebooks-eng-all-1gram-20120701-t'
        USING PigStorage()
        AS (ngram:CHARARRAY, year:INT, instance_count:INT, book_count:INT);

/* Filter only records for "television" */
flt2 = FILTER data2 BY LOWER(ngram) == 'television' AND year >= 1920;

/* Group all instances of [Tt]elevision */
grp2 = GROUP flt2 BY (LOWER(ngram), year);

/* Sum the count of occurrences (by year) */
res2 = FOREACH grp2 GENERATE FLATTEN(group),
                    SUM(flt2.instance_count) AS instance_count:LONG;

/*
   res1 and res2 contain the occurrences for radio and television
   respectively by year.  To generate the results in a form:

   year  radio  televison
   1920  15523  133
   1921  18688  28
   <etc>

   generate a relation containing all "years" and then OUTER JOIN
   back to it.  This isn't strictly necessary as the radio and television
   records exist for all years 1920-2008 (and so a simple JOIN of
   the two resultsets would suffice in practice).
*/

/* Ensure that we have all years represented. */
years1 = FOREACH res1 GENERATE year;
years2 = FOREACH res2 GENERATE year;
years_all = UNION years1, years2;

/* Filter unique year values - implicitly orders the results */
years = DISTINCT years_all;

/* Join radio records to the years relation */
j1 = JOIN years BY year LEFT OUTER, res1 BY year;
/* Join television records to the years/radio relation */
j2 = JOIN j1 BY years::group::year LEFT OUTER, res2 BY year;

/*
   Generate a simple relation showing
    year, radio_count, television_count, radio_pct
   where radio_pct is the percentage of the occurrences of
   "radio" and "television" that were "radio".
*/
res = FOREACH j2 GENERATE j1::years::group::year AS year:INT,
                          j1::res1::instance_count AS radio:INT,
                          res2::instance_count AS television:INT,
                          (double)j1::res1::instance_count/
                            ((double)j1::res1::instance_count +
                             (double)res2::instance_count) AS radio_pct:DOUBLE;

/* Dump it */
dump res;
