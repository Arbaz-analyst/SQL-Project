-- Removing duplicates

create table layoffs_staging like layoffs;

select * from layoffs_staging;

insert into layoffs_staging  select * from layoffs;

select * , row_number() over(partition by company, location, industry) as row_num 
from layoffs_staging;

-- using CTE first if we can delete duplicates

with duplicate_cte as (
select * , row_number() over(partition by company, location, industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num 
from layoffs_staging)
delete  from duplicate_cte
where row_num >1 ;

-- CTE dont work so i am using copy of that table 


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

-- row_number() window function is best if you want to del duplicates if primary key is not provided

insert into layoffs_staging2
select * , row_number() over(partition by company, location, industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num 
from layoffs_staging;

select * from layoffs_staging2 
where row_num >1 ;

-- now deleting duplicate 

delete from layoffs_staging2
where row_num >1;

select * from layoffs_staging2 where row_num >1;


-- standardize the data 

select company,trim(company) from layoffs_staging2;

-- updating the data with some strings like trim

update layoffs_staging2 
set company = trim(company);

select distinct industry from layoffs_staging2
order by 1;

select * from layoffs_staging2 
where industry like "crypto%";

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'crypto%';


update layoffs_staging2 
set country = trim(trailing '.' from country);


-- chage date into meaningful format so we can use that for date- time intelligence

update layoffs_staging2
set `date` =  str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2 
modify column `date` date;

-- Now Null and blank values


select *from layoffs_staging2 
where industry is null or industry = '';

select * from layoffs_staging2 
where company like 'airbn%';

update layoffs_staging2 
set industry = null 
where industry = '';

 --  using self join to fill in null values 
 
 
 update layoffs_staging2 t1
 join  layoffs_staging2 t2 
 on t1.company= t2.company 
 set t1.industry = t2.industry
 where t1.industry is null and t2.industry is not null;
 

delete from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null;

-- now dropping row_num column as needed 

alter table layoffs_staging2 
drop column row_num;

select * from layoffs_staging2;