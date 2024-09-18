--1. DATA LOADING
create database world_layoffs_project

use world_layoffs_project
select * from layoffs

select * into layoffs_stagging from layoffs

select * from layoffs_stagging

--2. REMOVING DUPLICATES
---identifying duplicate records
with duplicate_cte as
(
select *, ROW_NUMBER() over(partition by company, location, industry, total_laid_off order by country) as row_num 
from layoffs_stagging
)
select * from duplicate_cte
where row_num > 1

---creating another staging table to improve data handling.
CREATE TABLE [dbo].[layoffs_stagging2](
	[company] [nvarchar](50) NOT NULL,
	[location] [nvarchar](50) NOT NULL,
	[industry] [nvarchar](50) NULL,
	[total_laid_off] [nvarchar](50) NULL,
	[percentage_laid_off] [nvarchar](50) NULL,
	[date] [nvarchar](50) NOT NULL,
	[stage] [nvarchar](50) NOT NULL,
	[country] [nvarchar](50) NOT NULL,
	[funds_raised_millions] [nvarchar](50) NULL,
	row_num int
)

insert into layoffs_stagging2
select *, 
row_number() over(partition by company, 'location', industry, total_laid_off order by country) as row_num
from layoffs_stagging

select * from layoffs_stagging2
where row_num > 1

---allows users to retrieve the table structure, providing information about columns, data types, constraints, and more.
exec sp_columns layoffs_stagging2

--Deleting duplicate records from layoffs_stagging2 table based on row_num values
delete from layoffs_stagging2
where row_num > 1

select * from layoffs_stagging2

--3. STANDARDIZING THE DATA(to improve data consistency)
--trim is used to clean up the data by removing these extra spaces
select company, trim(company)
from layoffs_stagging2

--retrieving and sorting unique values
--standardizing industry values for accurate exploratory of data analysis
--grouping variants of 'crypto' and 'crypto currency'
select distinct industry
from layoffs_stagging2
order by 1

select * from layoffs_stagging2
where industry like 'crypto%'

update layoffs_stagging2
set industry = 'Crypto'
where industry like 'crypto%'

--cleaning and updating 'country' field: removing trailing periods and standardizing entries
select * from layoffs_stagging2
where country like 'United States%'
order by 1

select distinct country, trim(trailing'.' from country)
from layoffs_stagging2
order by 1

update layoffs_stagging2
set country = trim(trailing'.' from country)
where country like 'United States%'

select * from layoffs_stagging2

--update the date column by converting the string date to a date format

-- Step 1: Handle invalid date strings
UPDATE layoffs_stagging2
SET date = '1900-01-01'
WHERE ISDATE(date) = 0
   OR TRY_CONVERT(DATE, date, 101) IS NULL;

-- Step 2: Convert valid NVARCHAR dates to DATE
UPDATE layoffs_stagging2
SET date = TRY_CONVERT(DATE, date, 101)
WHERE ISDATE(date) = 1
  AND TRY_CONVERT(DATE, date, 101) IS NOT NULL;

-- Step 3: Alter the column type to DATE
ALTER TABLE layoffs_stagging2
ALTER COLUMN date DATE;

--4. Dropping unnecessary columns
--Identifying records with missing or empty values in layoffs_stagging2 table

select * from layoffs_stagging2
where total_laid_off = 'null'

select * from layoffs_stagging2
where total_laid_off = 'null'
and percentage_laid_off = 'null'

select t1.industry, t2.industry
from layoffs_stagging2 t1
join layoffs_stagging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null

UPDATE t1
SET t1.industry = t2.industry
FROM layoffs_stagging2 t1
JOIN layoffs_stagging2 t2
  ON t1.company = t2.company
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

select * from layoffs_stagging2
where company like 'Bally%'

select * from layoffs_stagging2
where company = 'Airbnb'

delete from layoffs_stagging2
where total_laid_off = 'null'
and percentage_laid_off = 'null'

update layoffs_stagging2
set total_laid_off = 0
where total_laid_off = 'null'

update layoffs_stagging2
set percentage_laid_off = 0
where percentage_laid_off = 'null'

alter table layoffs_stagging2
drop column row_num

select * from layoffs_stagging2