/*
Assume you are given the tables below about Facebook pages and page likes.
Write a query to return the page IDs of all the Facebook pages that don't have any likes.
The output should be in ascending order.
*/

SELECT pages.page_id
FROM pages
LEFT JOIN page_likes ON page_likes.page_id = pages.page_id
WHERE page_likes.page_id IS NULL
ORDER BY pages.page_id;
