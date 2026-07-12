USE demo;

-- Xóa view và bảng cũ nếu đã tồn tại
DROP VIEW IF EXISTS strong_beers_view;
DROP VIEW IF EXISTS beer_summary_view;
DROP TABLE IF EXISTS beers_partitioned;
DROP TABLE IF EXISTS beers;

-- Bảng đọc dữ liệu gốc từ HDFS
CREATE EXTERNAL TABLE beers (
    id INT,
    make STRING,
    type STRING,
    alcoholpercentage DOUBLE,
    brewery STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/input1'
TBLPROPERTIES (
    'skip.header.line.count'='1'
);

-- Bảng được phân vùng theo mức độ cồn
CREATE TABLE beers_partitioned (
    id INT,
    make STRING,
    type STRING,
    alcoholpercentage DOUBLE,
    brewery STRING
)
PARTITIONED BY (
    alcohol_level STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Cho phép tạo partition động
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Nạp dữ liệu vào các partition
INSERT OVERWRITE TABLE beers_partitioned
PARTITION (alcohol_level)
SELECT
    id,
    make,
    type,
    alcoholpercentage,
    brewery,
    CASE
        WHEN alcoholpercentage < 6 THEN 'low'
        WHEN alcoholpercentage < 7 THEN 'medium'
        ELSE 'high'
    END AS alcohol_level
FROM beer
WHERE id IS NOT NULL;

-- View hiển thị các loại bia mạnh từ 7% trở lên
CREATE VIEW strong_beers_view AS
SELECT
    id,
    make,
    type,
    alcoholpercentage,
    brewery
FROM beers_partitioned
WHERE alcohol_level = 'high';

-- View thống kê số lượng và độ cồn trung bình theo partition
CREATE VIEW beer_summary_view AS
SELECT
    alcohol_level,
    COUNT(*) AS total_beers,
    ROUND(AVG(alcoholpercentage), 2) AS average_alcohol
FROM beers_partitioned
GROUP BY alcohol_level;

-- Các lệnh kiểm tra kết quả
SHOW PARTITIONS beers_partitioned;

SELECT
    alcohol_level,
    COUNT(*) AS total_beers
FROM beers_partitioned
GROUP BY alcohol_level
ORDER BY alcohol_level;

SELECT *
FROM strong_beers_view
ORDER BY alcoholpercentage DESC;

SELECT *
FROM beer_summary_view
ORDER BY alcohol_level;

DESCRIBE FORMATTED strong_beers_view;