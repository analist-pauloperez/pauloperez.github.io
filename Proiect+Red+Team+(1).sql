                                                               /* PROJECT RED TEAM */


/*is the revenue/profitability seasonal?
(Choose what to examine: revenue or profitability or both.) */

/*Calculcating TotalRevenue and Totalprofit by season for year 2012 and 2013 */

WITH SeasonalSales AS (--aggregated sales data grouped by year, month, season, and product category
    SELECT
        YEAR(soh.OrderDate) AS ProductYear, --retrieving the year from date
        MONTH(soh.OrderDate) AS ProductMonth, -- retrieving the month from date
        CASE -- Categorize Data by Seasons, using case function in order to assing each month with their season
            WHEN MONTH(soh.OrderDate) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(soh.OrderDate) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(soh.OrderDate) IN (9, 10, 11) THEN 'Autumn'
            WHEN MONTH(soh.OrderDate) IN (12, 1, 2) THEN 'Winter'
        END AS Season,
        pc.Name AS ProductCategory, 
        SUM(sod.LineTotal) AS TotalRevenue, -- using sum function to calculate total sales
        SUM(sod.LineTotal - (p.StandardCost * sod.OrderQty)) AS TotalProfit -- using sum function to calculate totalprofit
FROM Sales.SalesOrderHeader soh -- Join was used to extract the information needed from each table
    JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN Production.Product p ON sod.ProductID = p.ProductID
    JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
	where YEAR(soh.OrderDate) in (2012,2013) --Filtering Data for Specific Years
	GROUP BY -- grouping the data by year,months, season and productcategory
        YEAR(soh.OrderDate),
       MONTH(soh.OrderDate),
        CASE
            WHEN MONTH(soh.OrderDate) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(soh.OrderDate) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(soh.OrderDate) IN (9, 10, 11) THEN 'Autumn'
            WHEN MONTH(soh.OrderDate) IN (12, 1, 2) THEN 'Winter'
        END,
        pc.Name 

)

SELECT -- in select we chose to show the totalrevenue and totalprofit for each season, calculated for years 2012-2013.
   
  Season,
  format(sum(TotalRevenue), 'N0') AS TotalRevenue, -- using format function to improve readability 
  format(sum(TotalProfit),'N0') AS TotalProfit -- using 'N0' format string ensures the numbers are displayed with commas(thousands separators)
                                               -- and no decimals.
FROM SeasonalSales
GROUP BY Season
ORDER BY  Season -- order data in ascending order


/*Calculating totalrevenue and total profit by season , category for years 2012 and 2013*/

WITH SeasonalSales AS ( 
    SELECT
        YEAR(soh.OrderDate) AS ProductYear,--retrieving the year from date
        MONTH(soh.OrderDate) AS ProductMonth,--retrieving the month from date
        CASE
            WHEN MONTH(soh.OrderDate) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(soh.OrderDate) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(soh.OrderDate) IN (9, 10, 11) THEN 'Autumn'
            WHEN MONTH(soh.OrderDate) IN (12, 1, 2) THEN 'Winter'
        END AS Season,
        pc.Name AS ProductCategory, 
        SUM(sod.LineTotal) AS TotalRevenue,
        SUM(sod.LineTotal - (p.StandardCost * sod.OrderQty)) AS TotalProfit
   FROM 
        Sales.SalesOrderHeader soh
    JOIN 
        Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN 
        Production.Product p ON sod.ProductID = p.ProductID
    JOIN 
        Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    JOIN 
        Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
	where YEAR(soh.OrderDate) in (2012,2013)
	GROUP BY 
        YEAR(soh.OrderDate),
        MONTH(soh.OrderDate),
        CASE
            WHEN MONTH(soh.OrderDate) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(soh.OrderDate) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(soh.OrderDate) IN (9, 10, 11) THEN 'Autumn'
            WHEN MONTH(soh.OrderDate) IN (12, 1, 2) THEN 'Winter'
        END,
        pc.Name 
)
SELECT --The final query aggregates the seasonal data from the SeasonalSales CTE to calculate total revenue, total profit, and their 
       --averages for each season and product category.
    Season,
    ProductCategory, 
    FORMAT(SUM(TotalRevenue), 'N0') AS TotalRevenue,--Sums up the TotalRevenue and TotalProfit for each product category across all months in the same season
    FORMAT(SUM(TotalProfit), 'N0') AS TotalProfit,
    FORMAT(AVG(TotalRevenue), 'N0') AS AvgRevenuePerSeason, -- average revenue and profit per season for each product category
    FORMAT(AVG(TotalProfit), 'N0') AS AvgProfitPerSeason 
FROM SeasonalSales
GROUP BY Season, ProductCategory
ORDER BY Season, ProductCategory;


/*Is there an upward or downward trend in the company's data over the
months and years?
(Choose what to examine: revenue or profitability or both.) */

WITH CompanyData AS (--aggredated sales data grouped by year, generating totalrevenue and totalprofit for years 2011-2014)
    SELECT 
        YEAR(soh.orderdate) AS ProductYear,
        SUM(sod.linetotal) AS TotalRevenue,
        SUM(sod.linetotal - (p.standardcost * sod.orderqty)) AS TotalProfit
    FROM 
        sales.salesorderheader soh
    JOIN 
        sales.SalesOrderDetail sod ON soh.salesorderid = sod.salesorderid
    JOIN 
        Production.Product p ON sod.ProductID = p.ProductID
    GROUP BY 
        YEAR(soh.orderdate)
),
Results AS ( -- calculating Year over Year (YOY) TotalProfit and TotalRevenuem using LAG function
    SELECT 
        ProductYear,
        TotalRevenue,
        TotalProfit, 
        -- YoY Revenue Growth
        LAG(TotalRevenue) OVER (ORDER BY ProductYear) AS PreviousYearRevenue,--The LAG() function retrieves the TotalRevenue value from the previous row (based on ProductYear ordering)
        CASE 
            WHEN LAG(TotalRevenue) OVER (ORDER BY ProductYear) IS NULL THEN 0 
            ELSE ((TotalRevenue - LAG(TotalRevenue) OVER (ORDER BY ProductYear)) * 100.0) / --Calculates the percentage change in revenue compared to the previous year.
                 LAG(TotalRevenue) OVER (ORDER BY ProductYear)
        END AS YoY_Revenue_Growth,
        -- YoY Profit Growth
        LAG(TotalProfit) OVER (ORDER BY ProductYear) AS PreviousYearProfit,
        CASE 
            WHEN LAG(TotalProfit) OVER (ORDER BY ProductYear) IS NULL THEN 0
            ELSE ((TotalProfit - LAG(TotalProfit) OVER (ORDER BY ProductYear)) * 100.0) / 
                 LAG(TotalProfit) OVER (ORDER BY ProductYear)
        END AS YoY_Profit_Growth
    FROM 
        CompanyData 
		),

YearStatus as(  -- CTE used to check if the data for a given year contains all 12 months or is incomplete
    select
        YEAR(orderdate) AS ProductYear,
        case
           when count(distinct month(OrderDate))= 12 then 'CompleteMonthYear' -- counts distinct months for each year, if we have all 12 months , the year is labeled as 'Complete Year'
            else 'IncompleteMonthYear'          -- otherwise, is labeled as 'Incompleteyear'
        end as YearType
    from Sales.SalesOrderHeader
    group by year(OrderDate) -- grouping the data by year to determine completeness for each year
       
)


SELECT 
    a.ProductYear,
	b.YearType,
    format(TotalRevenue, 'N0') as TotalRevenue,
    format(TotalProfit,'N0') as TotalProfit,
    format(PreviousYearRevenue, 'N0') as PreviousyearRevenue,
    format(YoY_Revenue_Growth, 'N2') + '%' as YoY_Revenue_Growth_Percentage, -- formatting data, shown in percentage, using +(concatenate)
    format(PreviousYearProfit, 'N0') as PreviousYearProfit,
    format(YoY_Profit_Growth, 'N2') + '%' as YoY_Profit_Growth_Percentage
FROM 
    Results a 
join YearStatus b on a.ProductYear = b.ProductYear
ORDER BY 
    ProductYear


/* PART 2 */

/*➢ What is the average of the discounts on a single item? */

with SalesCategory_CTE as (-- calculates TotalRevenue,TotalProfit and AvgDiscount, for each product category grouped by year and month.
select
	year(soh.orderdate) as YearSales,
	month(soh.orderdate) as MonthSales,
	pc.name as CategoryName,
	sum(sod.linetotal) as TotalRevenue,
	sum(sod.linetotal - (p.standardcost * sod.orderqty)) as TotalProfit,
	AVG((sod.UnitPrice - sod.LineTotal / sod.OrderQty) / sod.UnitPrice * 100.0) AS AvgDiscount

from sales.salesorderheader soh
   join sales.SalesOrderDetail sod
     on soh.salesorderid = sod.salesorderid
       join Production.Product p
         on sod.ProductID = p.ProductID
           join Production.ProductSubcategory psc
            on p.ProductSubcategoryID = psc.ProductSubcategoryID
               join Production.ProductCategory pc
                 on psc.ProductCategoryID = pc.ProductCategoryID
			
group by pc.name, year(soh.orderdate), month(soh.orderdate)
)

select
yearsales,
monthsales,
categoryname,
format(TotalRevenue,'N0') as TotalRevenue,
format(TotalProfit,'N0') as TotalProfit,
format(AvgDiscount, 'N2') + '%' as AvgDiscountPercentage
from SalesCategory_CTE
where yearsales in (2012,2013) and AvgDiscount > 0.01 -- filtering data only for years 2012 and 2013 , showing AvgDiscount that is higher than 0.01%
order by AvgDiscountPercentage desc --Sorts the results by average discount percentage in descending order, showing the categories with the highest discounts first



/*➢ What is the quantity of items purchased? */

-- Here we calculate the qty grouped by year , month and by category , it's easier to check by years and months which products 
-- are most ordered.

with SalesCategory as ( --calculating orderqty grouped by years , months and categoryname
select
	pc.name as CategoryName,
	year(soh.orderdate) as YearSales,
	month(soh.orderdate) as MonthSales,
	sum(sod.orderqty) as QtyOfItemPurchased
	

from sales.salesorderheader soh
   join sales.SalesOrderDetail sod
     on soh.salesorderid = sod.salesorderid
       join Production.Product p
         on sod.ProductID = p.ProductID
           join Production.ProductSubcategory psc
            on p.ProductSubcategoryID = psc.ProductSubcategoryID
               join Production.ProductCategory pc
                 on psc.ProductCategoryID = pc.ProductCategoryID

group by pc.name, year(soh.orderdate), month(soh.orderdate)

)
select -- The main query summarizes the data from the CTE, focusing on total quantities sold across product categories.
	YearSales,
	monthsales,
	sum(case when CategoryName = 'Bikes' then QtyOfItemPurchased else 0 end) 'BikesQty',
	sum(case when CategoryName = 'Clothing' then QtyOfItemPurchased else 0 end) 'ClothingQty',	
	sum(case when CategoryName = 'Accessories' then QtyOfItemPurchased else 0 end) 'AccesoriesQty',
	sum(case when CategoryName = 'Components' then QtyOfItemPurchased else 0 end) 'ComponentsQty',
    sum(QtyOfItemPurchased) TotalQtyOrdered
from SalesCategory
group by YearSales, MonthSales
order by  YearSales, MonthSales

-- this is an overview for the categories ordered within yeach year
-- here it's easier to check based on the seasonality which products performed best by the number of products ordered


with SalesCategory_CTE as (--calculates category-level sales metrics, grouped by year and month
select
	pc.name as CategoryName,
	year(soh.orderdate) as YearSales,
	month(soh.orderdate) as MonthSales,
	sum(sod.orderqty) as QtyOfItemPurchased


from sales.salesorderheader soh
   join sales.SalesOrderDetail sod
     on soh.salesorderid = sod.salesorderid
       join Production.Product p
         on sod.ProductID = p.ProductID
           join Production.ProductSubcategory psc
            on p.ProductSubcategoryID = psc.ProductSubcategoryID
               join Production.ProductCategory pc
                 on psc.ProductCategoryID = pc.ProductCategoryID
where year(soh.orderdate) in (2012,2013)
group by pc.name, year(soh.orderdate), month(soh.orderdate)
),

QuarterQTY_CTE as ( --This CTE categorizes the monthly data from SalesCategory_CTE into seasons (quarters)
select
    CategoryName,
	QtyOfItemPurchased,
    case 
		when MonthSales in (3,4,5) then  'Spring' 
		when MonthSales in (6,7,8) then 'Summer'
		when MonthSales in (9, 10, 11) then 'Autumn'
		when MonthSales in (12, 1, 2) then 'Winter'
	end as 'Quarter'
from SalesCategory_CTE
group by CategoryName,
	QtyOfItemPurchased,
    case
		when MonthSales in (3,4,5) then  'Spring' 
		when MonthSales in (6,7,8) then 'Summer'
		when MonthSales in (9, 10, 11) then 'Autumn'
		when MonthSales in (12, 1, 2) then 'Winter'
	end
)

select --The final query aggregates the seasonal sales quantities for each product category
	CategoryName,
	sum(case when [Quarter] = 'Spring' then QtyOfItemPurchased else 0 end) as Spring,
	sum(case when [Quarter] = 'Summer' then QtyOfItemPurchased else 0 end) as Summer,
	sum(case when [Quarter] = 'Autumn' then QtyOfItemPurchased else 0 end) as Autumn,
	sum(case when [Quarter] = 'Winter' then QtyOfItemPurchased else 0 end) as Iarna,
	SUM(QtyOfItemPurchased ) TOTALQTY
from QuarterQTY_CTE
group by 
         CategoryName
order by TOTALQTY DESC


/*➢ How much is the margin (sale price less cost)? */
/*➢ What is the average margin (sale price less cost)? */
/*➢ What are the monthly and quarterly rankings for the year according to the
margin (sale less cost)? */

-- v2 MonthlyRank


WITH SalesData AS (--The SalesData CTE prepares raw sales data with key calculations for each order line.
    SELECT
        YEAR(soh.OrderDate) AS ProductYear,
        MONTH(soh.OrderDate) AS ProductMonth,
        sod.OrderQty,
        sod.LineTotal,
        sod.UnitPrice,
        p.StandardCost,
        (sod.LineTotal - (p.StandardCost * sod.OrderQty)) AS Profit
    FROM
        Sales.SalesOrderHeader soh
    JOIN 
        Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN 
        Production.Product p ON sod.ProductID = p.ProductID
),
MonthlyData AS (--The MonthlyData CTE aggregates the sales data at the monthly level by grouping data by ProductYear and ProductMonth.
    SELECT 
        ProductYear,
        ProductMonth,
        SUM(LineTotal) AS TotalRevenue,
        SUM(Profit) AS TotalProfit,
        SUM((LineTotal / OrderQty - StandardCost) * OrderQty) AS TotalMargin,
        AVG(LineTotal / OrderQty - StandardCost) AS AverageMargin
    FROM 
        SalesData
    GROUP BY 
        ProductYear, ProductMonth
)

SELECT --The main query retrieves and ranks the monthly data, displaying metrics and rankings.
    ProductYear,
    ProductMonth,
    format(TotalMargin, 'N0') as TotalMargin,
    format(AverageMargin, 'N') as AverageMargin,
    RANK() OVER (PARTITION BY ProductYear ORDER BY TotalMargin DESC) AS MonthlyRank
    
FROM 
    MonthlyData



--QuarterlyRank

WITH SalesData AS (
        SELECT
        YEAR(soh.OrderDate) AS ProductYear,
        MONTH(soh.OrderDate) AS ProductMonth,
        sod.OrderQty,
        sod.LineTotal,
        sod.UnitPrice,
        p.StandardCost,
        (sod.LineTotal - (p.StandardCost * sod.OrderQty)) AS Profit
    FROM
        Sales.SalesOrderHeader soh
    JOIN 
        Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
    JOIN 
        Production.Product p ON sod.ProductID = p.ProductID
),

QuarterlyData AS ( --aggregates the sales data by quarter for each year, calculating quarterly margin and average margin.
    SELECT 
        ProductYear,
        CASE
            WHEN ProductMonth IN (1, 2, 3) THEN 'Q1'
            WHEN ProductMonth IN (4, 5, 6) THEN 'Q2'
            WHEN ProductMonth IN (7, 8, 9) THEN 'Q3'
            WHEN ProductMonth IN (10, 11, 12) THEN 'Q4'
        END AS Quarter,
        SUM((LineTotal / OrderQty - StandardCost) * OrderQty) AS QuarterlyMargin,
		AVG(LineTotal / OrderQty - StandardCost) AS AverageMargin
    FROM 
        SalesData
    GROUP BY 
        ProductYear,
        CASE
            WHEN ProductMonth IN (1, 2, 3) THEN 'Q1'
            WHEN ProductMonth IN (4, 5, 6) THEN 'Q2'
            WHEN ProductMonth IN (7, 8, 9) THEN 'Q3'
            WHEN ProductMonth IN (10, 11, 12) THEN 'Q4'
        END
)

SELECT --The main query formats and ranks the data from the QuarterlyData CTE
    ProductYear,
	Quarter,
	format(QuarterlyMargin, 'N0') as QuarterlyMargin,
    format(AverageMargin, 'N') as AverageMargin,
    RANK() OVER (PARTITION BY ProductYear ORDER BY QuarterlyMargin DESC) AS QuarterlyRank --Assigns a rank to each quarter within the same year
FROM 
    QuarterlyData 
