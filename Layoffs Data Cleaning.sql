-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates- to get rid of duplicate values
-- 2. Standardize Data- to check with spellings and etc
-- 3. Null values or Blank values- to manage or delete them
-- 4. Remove Any Columns- if necessary

-- Copy the raw values from layoff table to another table
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;

-- 1. REMOVING DUPLICATES

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging 
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

# Create a new table with extra attribute row_num so that it is used to delete duplicates

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER( PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`,stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging ;

SELECT * FROM layoffs_staging2;

DELETE 
FROM layoffs_staging2
WHERE row_num >1 ;

SELECT * FROM layoffs_staging2
WHERE row_num >1;


-- 2. STANDARDIZING DATA

# Trim all the blank spaces in the begining

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company= TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2;

# Change all crypto like industry to crypto

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'CRYPTO%';

UPDATE layoffs_staging2
SET industry= 'Crypto'
WHERE industry like 'CRYPTO%';

# change united states. to united states

SELECT country
FROM layoffs_staging2
GROUP BY country
ORDER BY country ;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country like 'United States%';

# Convert date to date format

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`= str_to_date(`date`, '%m/%d/%Y') ;

SELECT * 
FROM layoffs_staging2;

# change date column datatype to date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE ;

-- 3. NULL VALUES

# NULL values in industry

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry ='';

SELECT *
FROM layoffs_staging2          
WHERE company = 'Airbnb';         # you can change industry to TRAVEL

SELECT *
FROM layoffs_staging2
WHERE company = "Bally's Interactive";

SELECT *
FROM layoffs_staging2
WHERE company = 'Carvana';         # You can change the industry to TRANSPORTATION

SELECT *
FROM layoffs_staging2
WHERE company = 'Juul';      # You can change industry to CONSUMER

SELECT *
FROM layoffs_staging2 AS T1
JOIN layoffs_staging2 AS T2
	ON T1.company= T2.company
WHERE (T1.industry IS NULL OR T1.industry='')
AND T2.industry IS NOT NULL ;

# change all blank industry values to NULL before importing t2.industry to t1.industry

UPDATE layoffs_staging2 
SET industry= null
WHERE industry = '';

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company= T2.company
SET T1.industry= T2.industry
WHERE (T1.industry IS NULL)
AND T2.industry IS NOT NULL ;

-- 4. REMOVING UNWANTED ROWS AND COLUMNS

# remove all null values of total_laid_off and percentage_laid_off because they are of no use

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

# drop column row_num

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2 ;
