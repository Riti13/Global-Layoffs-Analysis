-- EXPLORATORY DATA ANALYSIS

SELECT *
FROM layoffs_staging2;

# Company having MAXIMUM total_layoffs

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

# Companies with max to min layoffs

SELECT company, SUM(total_laid_off)
from layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# Industries having total layoffs

SELECT industry, SUM(total_laid_off)
from layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

# starting and ending of layoffs

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

# Countries with total layoffs

SELECT country, SUM(total_laid_off)
from layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# Layoffs by years

SELECT `date`, SUM(total_laid_off)
from layoffs_staging2
GROUP BY `date`
ORDER BY 1 ASC;

SELECT YEAR(`date`), SUM(total_laid_off)
from layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;

# Companies having total funds raised

SELECT company, SUM(funds_raised_millions)
from layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

# Countries having total funds raised

SELECT country, SUM(funds_raised_millions)
from layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

# Rolling data layoffs by each month of year

SELECT SUBSTR(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTR(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

WITH rolling_total as
(
SELECT SUBSTR(`date`,1,7) AS `MONTH`, SUM(total_laid_off) as total
FROM layoffs_staging2
WHERE SUBSTR(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total, SUM(total) OVER(ORDER BY `MONTH`) AS ROLLING_TOTAL
FROM rolling_total ;

# Rolling total by company and year

SELECT company,YEAR(`date`), SUM(total_laid_off)
from layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 1;

WITH COMPANY_YEAR (COMPANY, YEARS, TOTAL_LAYOFFS) AS
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
from layoffs_staging2
GROUP BY company, YEAR(`date`)
), 
COMPANY_YEAR_RANK AS 
(
SELECT *, DENSE_RANK() OVER(PARTITION BY YEARS ORDER BY TOTAL_LAYOFFS DESC) AS RANKS
FROM  COMPANY_YEAR
WHERE YEARS IS NOT NULL
)
SELECT *
FROM COMPANY_YEAR_RANK
WHERE RANKS <= 5 ;