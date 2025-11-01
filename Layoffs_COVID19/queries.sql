USE project_1_portafolio;

-- verificamos la tabla con rawdataretail_s
SELECT *
FROM data_cleaning_portafolio;

-- creamos tabla de trabajo para evitar modificar la tabla original
CREATE TABLE layoffs_staging
LIKE data_cleaning_portafolio;

-- Verificamos que se haya creado la tabla, hasta el momento solo tendra las columnas
SELECT *
FROM layoffs_staging;

-- en esta parte insertamos los datos en las columnas de nuestra tabla creada en la consulta anterior
INSERT layoffs_staging
SELECT *
FROM data_cleaning_portafolio;

-- Buscaremos filas repetidas creando una columna nueva con una numeracion que cambiara si se repite una fila,
 -- para eso agregamos un window con todas las columnas, usando row number y partition by, para incrementar la numeracion solo si la fila es diferente
SELECT *,ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions ) AS row_num
FROM layoffs_staging;


-- creamos un CTE con la query de arriba para facilitar nuestra consulta
WITH duplicate_cte AS 
(
SELECT *,ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions ) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- Buscaremos el resultado del query anterior para ver las filas repetidas
SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';


-- Creamos una tabla para poder Borrar los duplicados
CREATE TABLE layoffs_staging2 AS 
SELECT *,ROW_NUMBER() OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,funds_raised_millions ) AS row_num
FROM layoffs_staging;


-- Borramos los numeros repetidos en la tabla creada.
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Comprobamos que se hayan borrado
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE company = 'Casper';

-- Standardizing Data, checamos si debemos usar TRIM
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Actualizamos la columna company, usando TRIM quitaremos espacios 
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Verificamos otra columna (encontramos que se repite una industria Crypto, pero con diferente nombre Crypto Currency)
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- Buscamos si hay mas variantes de crypto
SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; 

-- Actualizaremos el nombre de Crypto para que sea un mismo nombre para todas las variantes
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry Like 'Crypto%';

-- Verificamos que ya solo hay un nombre para la industri Crypto 
SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- verificamos otra columna
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

-- Encontramos que United States tiene dos nombres, ya que uno tiene punto final
SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United S%';

-- Podemos quitar ese punto final con TRIM agregando TRAILING
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'United S%';

-- Actualizaremos la tabla para borrar ese punto
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Verificamos que se haya borrado el punto, y solo debe aparecer un United States
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'United S%';

-- Cambiaremos el tipo de datos de Date, ya que esta en texto
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Actualizamos el tipo de dato de `date
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Verificamos el cambio 
SELECT `date`
FROM layoffs_staging2;

-- La columna sigue siendo tipo texto, debemos alterar la tabla
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Verificaremos valores NULL y Blanks, para eliminarlos de ser necesario
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

-- Ahora veremos si podemos cambiar esos valores vacios verificando otras filas que tengan similitudes en company
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2
WHERE company = 'Bally''s Interactive'; -- este tendremos que quitarlo por que es fila unica



-- Haremos una serie de pasos para pasar los valores de la columna industria, a las filas que tienen valores NULL o que estan vacios
-- Esta Query sirve para ver todas las filas que tienen Null o estan vacias
SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
 WHERE ( t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- En esta Query volvemos NULL a los valores vacios, para asi facilitar el llenado de las celdas 
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

-- Actualizaremos las celdas NULL con el valor correspondiente
UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Verificamos que los cambios se realizaron en Industry
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Borramos esta fila ya que no nos sera de utilidad al tener valor nulo y no tener otra fila que nos indique cual es su tipo de industria
DELETE
FROM layoffs_staging2
WHERE company = 'Bally''s Interactive';

-- Borramos las filas que tengan valor nulo en total_laidoff y en percentage_laid_off. La primer Query es para comprobar los datos y la segunda los borra
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Borraremos la columna que creamos llamada row_num, ya que no tendra ningun uso de ahora en adelante.

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2
LIMIT 2000;
