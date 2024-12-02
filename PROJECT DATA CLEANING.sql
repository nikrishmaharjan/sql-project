SELECT * 
FROM world_layoffs.layoffs;

##creating raw tables to find out duplicates
create table layoffs_staged
like layoffs;

select*
from layoffs_staged;

##inserting data in the raw table
insert layoffs_staged
select *
from world_layoffs.layoffs;

## adding a coln named row_no to find out the repeating rows
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_no
from layoffs_staged;

##finding out the rows that are repeated, fro which we need to duplicte_cte to use where clause
with duplicate_cte as (
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_no
from layoffs_staged
)
select * 
from duplicate_cte 
where row_no > 1;

##delete the rows that are returned, for which a table with the row_no has to be created 
CREATE TABLE `layoffs_staged2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_no` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

##insert values in the new table
insert into layoffs_staged2
select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_no
from layoffs_staged;

##testing 
select * 
from layoffs_staged2
where row_no > 1;

##deteting the duplicates
delete
from layoffs_staged2
where row_no > 1;
 ##SUCCESS##
 
 ##STANDARDIZING THE DATA
 select * 
from layoffs_staged2;

##trimmed the unnecessary spaces 
 update layoffs_staged2
 set company = trim(company), location = trim(location);
 
 ##combining the same industries with different names

update layoffs_staged2
set industry = 'crypto'
where industry like 'crypto%';

update layoffs_staged2
set country = trim(trailing '.' from country)
where country like'united states%';

##changing the format string to date 
update layoffs_staged2
set `date` = str_to_date(`date` , '%m/%d/%Y');

## populating null values
select *
from layoffs_staged2
where company = 'carvana' or company = 'juul';


##filled the null value 
update layoffs_staged2
set industry = 'Travel'
where company = 'airbnb' and funds_raised_millions = 6400;

###testing
select t1.company, t1.industry, t2.industry, t2.company
from layoffs_staged2 as t1
join layoffs_staged2 as t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '') 
and t2.industry is not null;
####

##turning the blank values into null values
update layoffs_staged2
set industry = null 
where industry = '';

## filling the values in the null spaces by joining the same table with similar data
update layoffs_staged2 t1
join layoffs_staged2 t2
on t1.company = t2.company 
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

## remove any unneccesary columns
select*
from layoffs_staged2
;
 
 ## removing the unnecessary column ie:row_no as it not needed anymore
 alter table layoffs_staged2
 drop column row_no;
 
#########  DATA CLEANING SUCCESSFUL#########

