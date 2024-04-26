-- DATA CLEANING
-- 1. Remove duplicate if there are any

-- Create a new table (staging) which has the same columns like the layoffs
select *
from layoffs_staging;

create table layoffs_staging
like layoffs;

-- And copy all the data from table layoffs (raw data) to the new one
insert into layoffs_staging
select *
from layoffs;

-- Check results
select *
from layoffs_staging;

-- We have to find the duplicates. As we don't have a unique column to help us, we have to create a new column in order to number the rows.
-- using the row_number() function we need partition by all columns in order to find the dupluicates
select *,
	row_number() over (
    partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- We need now to filter the row_num column where the value is greater than 1
-- One way is to use a CTE
with find_duplicates as
(
select *,
	row_number() over (
    partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *			-- DELETE doesn't work in CTEs
from find_duplicates
where row_num > 1;

-- As in CTEs we cannot manipulate the data, we can create a new table adding a new column with row_num
create table layoffs_staging2
like layoffs_staging;

alter table layoffs_staging2
add column row_num int;

-- Copy all the data including values for row_num column
insert into layoffs_staging2
select *,
	row_number() over (
    partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- Filter rows by removing the duplicates (row_num > 1)
select * 
from layoffs_staging2
where row_num > 1;

-- Delete these rows.
delete from layoffs_staging2
where row_num > 1;

-- Check the dataset
select *
from layoffs_staging2
where row_num > 1;	-- Cleaned

----------------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Standardize the data (issues for spelling etc)

select * 
from layoffs_staging2;

-- Use trim to remove the spaces before and after the strings
select
	company,
    trim(company)
from layoffs_staging2;

-- Update table layoffs_staging2 remainig the fixed values
update layoffs_staging2
set company = trim(company);

select * from layoffs_staging2;

-- Check for issues in industry column and remove them
select distinct industry
from layoffs_staging2;

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- Check and fix issues in country
select distinct country
from layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Change datatype of date
select 
	`date`,
    str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select * from layoffs_staging2;

-- Changing the datatype with str_to_date does not affect the datatype for date in table
-- So we'll alter the table and change the type.
alter table layoffs_staging2
modify column `date` date;

----------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. Check for Null and Blank values

select *
from layoffs_staging2
order by industry;

select *
from layoffs_staging2
where industry is null or industry = '';

-- Check the company Airbnb as example
select *
from layoffs_staging2
where company = 'Airbnb';

-- Using self join check the industry which appear in the one talbe and not in the other
select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- Populate the known industry to the unknown (null) for the same company
update layoffs_staging2 t1
	join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null or t1.industry = '';

-- In order to take place the above Update we need first to set Null value wherever there is ''(blank) for industry
update layoffs_staging2
set industry = NULL
where industry = '';

-- Repeat the previous Update step, removing first the OR condition = ''
update layoffs_staging2 t1
	join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- Check result
select *
from layoffs_staging2
where industry is null or industry = '';

----------------------------------------------------------------------------------------------------------------------------------------------------------

-- 4. Remove any unnecessary Rows and Columns

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- Regarding these two columns we cannot use them for analysis as they cannot provide us with sufficient data.
delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2;

-- Finally the column row_num is not useful anymore so we proceed on deletion.
alter table layoffs_staging2
drop column row_num;
