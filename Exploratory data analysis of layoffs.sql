select * from layoffs_staging2
where percentage_laid_off =1 
order by funds_raised_millions desc;


select company,sum(total_laid_off)
from layoffs_staging2 
group by company 
order by 2 desc;


select min(`date`) ,MAX(`date`)
from layoffs_staging2;


select industry,sum(total_laid_off)
from layoffs_staging2 
group by industry 
order by 2 desc;


select country,sum(total_laid_off)
from layoffs_staging2 
group by country 
order by 2 desc;


select `date`,sum(total_laid_off)
from layoffs_staging2 
group by `date`
order by 1 desc;


select year(`date`),sum(total_laid_off)
from layoffs_staging2 
group by year(`date`)
order by 1 desc;


select stage,sum(total_laid_off)
from layoffs_staging2 
group by stage
order by 2 ;


select substring(`date`,6,2) as `month`,sum(total_laid_off)
from layoffs_staging2 
group by `month`
order by 1 ;


select substring(`date`,1,7) as `month`,sum(total_laid_off)
from layoffs_staging2 
where substring(`date`,1,7) is not null
group by `month`
order by 1 ;


with Rolling_total as (
select substring(`date`,1,7) as `month`,sum(total_laid_off) as total_off
from layoffs_staging2 
where substring(`date`,1,7) is not null
group by `month`
order by 1)
select `month`,total_off,sum(total_off) over(order by `month`) as rolling_total
from Rolling_total;


select company,year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
order by 3 desc;


with company_year(company,years,total_laid_off) as (
select company,year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
), company_yearly_rank as (
select * ,dense_rank() over(partition by years order by total_laid_off) as Ranking
from company_year
where years is not null
) 
select * from company_yearly_rank
where ranking>5;