-- Total Number of product, Avg. Current Price, Total Number of Reviews
select
    distinct count(product_title) as 'Total Number of Products',
    Round(AVG(product_price),2) as 'Average Current Price',
    sum(product_num_ratings) as 'Total number of reviews'
from product_by_category;

-- Average, Median, Max of Current Price and Original Price
select 
    ROUND(AVG(product_price),2) as 'Avg. Current Price',
    ROUND(AVG(product_original_price),2) as 'Avg. Original Price',
    ROUND(MAX(product_price),2) as 'Max Current Price',
    ROUND(MAX(product_original_price),2) as 'Max Original Price',
    (SELECT distinct PERCENTILE_CONT(0.5) 
        WITHIN GROUP (ORDER BY product_price) OVER ()
     FROM product_by_category) as 'Median of Current Price',
    (SELECT distinct PERCENTILE_CONT(0.5) 
        WITHIN GROUP (ORDER BY product_original_price) OVER () 
     FROM product_by_category) as 'Median of Original Price'
from product_by_category;

-- Top 10 products by price
select top 10
    product_title as 'Product Title',
    Max(product_price) as 'Current Price'
from product_by_category
group by product_title
order by 'Current Price' desc;

-- Amazon prime ratio
select 
    is_prime as 'Amazon Prime',
    count(is_prime) as 'Total Number of Product',
    count(is_prime) * 100 / sum(count(is_prime)) over() as 'Percentage'
from product_by_category
group by is_prime;

-- Climate Pledge Friendly ratio
select 
    climate_pledge_friendly as 'Climate Pledge Friendly',
    count(climate_pledge_friendly) as 'Total Number of Product',
    count(climate_pledge_friendly) * 100 / sum(count(climate_pledge_friendly)) over() as 'Percentage'
from product_by_category
group by climate_pledge_friendly;
