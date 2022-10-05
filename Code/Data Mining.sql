USE Datasets;

CREATE TABLE Challenger
(o_ring_failure INT,
launch_temperature INT,
leak_check_pressure VARCHAR(10)
);

LOAD DATA LOCAL INFILE '/Users/bryton/Desktop/Challenger.csv' INTO TABLE Challenger
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


SELECT * FROM Challenger;

-- RANGE 

SELECT CONCAT(MIN(launch_temperature), '-', MAX(launch_temperature)) AS R FROM Challenger;

DELIMITER $$
CREATE FUNCTION GetRange()
RETURNS VARCHAR(30)
BEGIN
DECLARE v INT;
DECLARE n INT;
SELECT MIN(launch_temperature), MAX(launch_temperature) FROM Challenger INTO v, n;
RETURN CONCAT(v, '-', n);
END$$

SELECT GetRange();

-- Median

SET @row_index := -1;
SELECT AVG(subq.launch_temperature) AS median
FROM (SELECT @row_index:= @row_index +1 AS row_index, launch_temperature
FROM Challenger
ORDER BY launch_temperature
		) AS subq
        WHERE subq.row_index 
        IN (FLOOR(@row_index / 2 ), CEIL(@row_index/2));

DELIMITER $$
CREATE FUNCTION GetMedian()
RETURNS FLOAT
BEGIN
SET @row_index := -1;
RETURN 
(SELECT AVG(subq.launch_temperature) AS median
FROM (SELECT @row_index:= @row_index +1 AS row_index, launch_temperature
FROM Challenger
ORDER BY launch_temperature
		) AS subq
        WHERE subq.row_index 
        IN (FLOOR(@row_index / 2 ), CEIL(@row_index/2)));
END$$

SELECT GetMedian();

-- MODE
SELECT launch_temperature as MODE
FROM Challenger
GROUP BY launch_temperature
ORDER BY COUNT(launch_temperature) DESC
LIMIT 1;

SELECT leak_check_pressure, COUNT(leak_check_pressure) as MODE
FROM Challenger
GROUP BY leak_check_pressure
ORDER BY COUNT(leak_check_pressure) DESC
LIMIT 1;

DELIMITER $$
CREATE FUNCTION GetMode()
RETURNS VARCHAR(10)
BEGIN
RETURN (SELECT launch_temperature as M
FROM Challenger
GROUP BY launch_temperature
ORDER BY COUNT(launch_temperature) DESC
LIMIT 1);
END$$

SELECT GetMode();

-- SKEWNESS
WITH SkewCTE AS
(
SELECT SUM(1.0*launch_temperature) AS rx,
 SUM(POWER(1.0*launch_temperature,2)) AS rx2,
 SUM(POWER(1.0*launch_temperature,3)) AS rx3,
 COUNT(1.0*launch_temperature) AS rn,
 STDDEV(1.0*launch_temperature) AS stdv,
 AVG(1.0*launch_temperature) AS av
FROM Challenger
)
SELECT
   (rx3 - 3*rx2*av + 3*rx*av*av - rn*av*av*av)
   / (stdv*stdv*stdv) * rn / (rn-1) / (rn-2) AS Skewness
FROM SkewCTE;

DELIMITER $$
CREATE FUNCTION GetSkewness()
RETURNS FLOAT
BEGIN
DECLARE res FLOAT;
WITH SkewCTE AS
(
SELECT SUM(1.0*launch_temperature) AS rx,
 SUM(POWER(1.0*launch_temperature,2)) AS rx2,
 SUM(POWER(1.0*launch_temperature,3)) AS rx3,
 COUNT(1.0*launch_temperature) AS rn,
 STDDEV(1.0*launch_temperature) AS stdv,
 AVG(1.0*launch_temperature) AS av
FROM Challenger
)
SELECT
   (rx3 - 3*rx2*av + 3*rx*av*av - rn*av*av*av)
   / (stdv*stdv*stdv) * rn / (rn-1) / (rn-2) AS Skewness
FROM SkewCTE INTO res;
RETURN res;
END$$

SELECT GetSkewness();

-- KURTOSIS
WITH KurtCTE AS
(
SELECT SUM(1.0*launch_temperature) AS rx,
 SUM(POWER(1.0*launch_temperature,2)) AS rx2,
 SUM(POWER(1.0*launch_temperature,3)) AS rx3,
 SUM(POWER(1.0*launch_temperature,4)) AS rx4,
 COUNT(1.0*launch_temperature) AS rn,
 STDDEV(1.0*launch_temperature) AS stdv,
 AVG(1.0*launch_temperature) AS av
FROM Challenger
)
SELECT
   (rx4 - 4*rx3*av + 6*rx2*av*av - 4*rx*av*av*av + rn*av*av*av*av)
   / (stdv*stdv*stdv*stdv) * rn * (rn+1) / (rn-1) / (rn-2) / (rn-3)
   - 3.0 * (rn-1) * (rn-1) / (rn-2) / (rn-3) AS Kurtosis
FROM KurtCTE;

DELIMITER $$
CREATE FUNCTION GetKurtosis()
RETURNS FLOAT
BEGIN
DECLARE res FLOAT;
WITH KurtCTE AS
(
SELECT SUM(1.0*launch_temperature) AS rx,
 SUM(POWER(1.0*launch_temperature,2)) AS rx2,
 SUM(POWER(1.0*launch_temperature,3)) AS rx3,
 SUM(POWER(1.0*launch_temperature,4)) AS rx4,
 COUNT(1.0*launch_temperature) AS rn,
 STDDEV(1.0*launch_temperature) AS stdv,
 AVG(1.0*launch_temperature) AS av
FROM Challenger
)
SELECT
   (rx4 - 4*rx3*av + 6*rx2*av*av - 4*rx*av*av*av + rn*av*av*av*av)
   / (stdv*stdv*stdv*stdv) * rn * (rn+1) / (rn-1) / (rn-2) / (rn-3)
   - 3.0 * (rn-1) * (rn-1) / (rn-2) / (rn-3) AS Kurtosis
FROM KurtCTE INTO res;
RETURN res;
END$$

SELECT GetKurtosis();

-- Z test

DELIMITER $$
CREATE FUNCTION Z() RETURNS FLOAT
BEGIN
declare res float;
declare av1 float;
declare av0 float;
declare d1 float;
declare d0 float;
declare num1 float;
declare num0 float;
select AVG(1.0*launch_temperature) from Challenger where o_ring_failure=1 into av1;
select AVG(1.0*launch_temperature) from Challenger where o_ring_failure=0 into av0;
select Power(STDDEV(1.0*launch_temperature),2) AS stdv FROM Challenger where o_ring_failure=1 into d1;
select Power(STDDEV(1.0*launch_temperature),2) AS stdv FROM Challenger where o_ring_failure=0 into d0;
select count(launch_temperature) from Challenger where o_ring_failure=1 into num1;
select count(launch_temperature) from Challenger where o_ring_failure=0 into num0;
select (av0-av1)/Power((d1/num1+d0/num0),0.5) into res;
RETURN res;
END$$

SELECT Z();

DELIMITER $$
CREATE PROCEDURE ZScore()
BEGIN
	DECLARE res FLOAT;
    DECLARE av1 FLOAT;
    DECLARE av0 FLOAT;
    DECLARE d1 FLOAT;
    DECLARE d0 FLOAT;
    DECLARE num1 FLOAT;
    DECLARE num0 FLOAT;
	SELECT AVG(1.0*launch_temperature) FROM Challenger WHERE o_ring_failure=1 INTO av1;
	SELECT AVG(1.0*launch_temperature) FROM Challenger WHERE o_ring_failure=0 INTO av0;
    SELECT POWER(STDDEV(1.0*launch_temperature),2) AS stdv FROM Challenger WHERE o_ring_failure=1 INTO d1;
    SELECT POWER(STDDEV(1.0*launch_temperature),2) AS stdv FROM Challenger WHERE o_ring_failure=0 INto d0;
    SELECT COUNT(launch_temperature) FROM Challenger WHERE o_ring_failure=1 INTO num1;
    SELECT COUNT(launch_temperature) FROM Challenger WHERE o_ring_failure=0 INTO num0;
    SELECT (av0-av1)/POWER((d1/num1+d0/num0),0.5) AS z;
END$$

CALL ZScore();

DELIMITER $$
CREATE PROCEDURE a(IN Tname VARCHAR(20), IN Categorical VARCHAR(20), IN Numerical VARCHAR(20))
BEGIN
DECLARE res FLOAT;
DECLARE av1 FLOAT;
DECLARE av0 FLOAT;
DECLARE d1 FLOAT;
DECLARE d0 FLOAT;
DECLARE num1 FLOAT;
DECLARE num0 FLOAT;
SET @sql1 = CONCAT('SELECT AVG(1.0*', Numerical,') FROM', Tname, 'WHERE ',Categorical, '=1 INTO av1');
-- SELECT AVG(1.0*Numerical) FROM Tname WHERE Categorical=0 INTO av0;
-- SELECT POWER(STDDEV(1.0*Numerical),2) AS stdv FROM Tname WHERE Categorical=1 INTO d1;
-- SELECT POWER(STDDEV(1.0*Numerical),2) AS stdv FROM Tname WHERE Categorical=0 INto d0;
-- SELECT COUNT(Numerical) FROM Tname WHERE Categorical=1 INTO num1;
-- SELECT COUNT(Numerical) FROM Tname WHERE Categorical=0 INTO num0;
-- SELECT (av0-av1)/POWER((d1/num1+d0/num0),0.5);
END$$

CALL a('Challenger', 'o_ring_failure', 'launch_temperature');

-- KNN
DELIMITER $$
CREATE PROCEDURE KNN(IN Tempareture INT, IN K INT)
BEGIN
SELECT launch_temperature
FROM Challenger
ORDER BY ABS((SELECT Tempareture) -launch_temperature)
LIMIT K;
END$$

DELIMITER $$
CREATE PROCEDURE KNN(IN Temperature INT, IN K INT)
SELECT o_ring_failure as Failure, launch_temperature, (SELECT COUNT(o_ring_failure)/K
FROM (SELECT o_ring_failure FROM Challenger
ORDER BY ABS((SELECT Temperature) - launch_temperature)
LIMIT K) as temp
WHERE temp.o_ring_failure = Failure) AS prob
FROM Challenger
ORDER BY ABS((SELECT Temperature)-launch_temperature)
LIMIT K;
END$$

CALL KNN(30, 3);

SELECT o_ring_failure as Failure, launch_temperature, (SELECT COUNT(o_ring_failure)/3
FROM (SELECT o_ring_failure FROM Challenger
ORDER BY ABS(30 - launch_temperature)
LIMIT 3) as temp
WHERE temp.o_ring_failure = Failure) AS prob
FROM Challenger
ORDER BY ABS(30-launch_temperature)
LIMIT 3;

SELECT COUNT(o_ring_failure)/3
FROM (SELECT o_ring_failure FROM Challenger
ORDER BY ABS(70 - launch_temperature)
LIMIT 3) as k
WHERE k.o_ring_failure = 1 ;
