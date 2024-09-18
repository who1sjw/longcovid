-- Create Table
CREATE TABLE long_covid_symptoms (
    long_covid INT,
    age INT,
    BMI FLOAT,
    number_of_cigarettes INT,
    sex INT,
    ethnicity INT,
    smoking INT,
    smoking_dur INT,
    autoimmune_disease INT,
    Immunosuppression INT,
    history_stroke INT,
    heart_disease INT,
    hypertension INT,
    asthma INT,
    COPD INT,
    Seizure INT,
    Migraine INT,
    insomnia INT,
    diabetes_one INT,
    diabetes_two INT,
    GI_disease INT,
    Cancer INT,
    anemia INT,
    renal_disease INT,
    COVID_vaccine_received INT,
    doses INT,
    isolation INT,
    rash INT,
    Conjunctivitis INT,
    Anosmia INT,
    SOB INT,
    chest_pain INT,
    cough INT,
    sore_throat INT,
    runny_nose INT,
    dysguesia INT,
    GI_sx INT,
    muscle_joint INT,
    fatigue INT,
    fever INT,
    confusion INT,
    reinfection INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Data Loading into Hive
LOAD DATA INPATH 's3://bigdatacovid/mainCov.csv' OVERWRITE INTO TABLE long_covid_symptoms;

SELECT count(*) FROM long_covid_symptoms where long_covid is NULL;


-- Schema Design for Efficient Querying
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE long_covid_symptoms_optimized (
    long_covid BOOLEAN,
    age INT,
    BMI FLOAT,
    number_of_cigarettes INT,
    sex INT,
    ethnicity INT,
    smoking INT,
    smoking_dur INT,
    autoimmune_disease BOOLEAN,
    Immunosuppression BOOLEAN,
    history_stroke BOOLEAN,
    heart_disease BOOLEAN,
    hypertension BOOLEAN,
    asthma BOOLEAN,
    COPD BOOLEAN,
    Seizure BOOLEAN,
    Migraine BOOLEAN,
    insomnia BOOLEAN,
    diabetes_one BOOLEAN,
    diabetes_two BOOLEAN,
    GI_disease BOOLEAN,
    Cancer BOOLEAN,
    anemia BOOLEAN,
    renal_disease BOOLEAN,
    COVID_vaccine_received BOOLEAN,
    doses INT,
    isolation BOOLEAN,
    rash BOOLEAN,
    Conjunctivitis BOOLEAN,
    Anosmia BOOLEAN,
    SOB BOOLEAN,
    chest_pain BOOLEAN,
    cough BOOLEAN,
    sore_throat BOOLEAN,
    runny_nose BOOLEAN,
    dysguesia BOOLEAN,
    GI_sx BOOLEAN,
    muscle_joint BOOLEAN,
    fatigue BOOLEAN,
    fever BOOLEAN,
    confusion BOOLEAN,
    reinfection BOOLEAN
)
PARTITIONED BY (age_group STRING)
CLUSTERED BY (long_covid) INTO 4 BUCKETS
STORED AS ORC;


INSERT OVERWRITE TABLE long_covid_symptoms_optimized
PARTITION (age_group)
SELECT 
    CASE WHEN long_covid = 1 THEN TRUE ELSE FALSE END,
    age,
    BMI,
    number_of_cigarettes,
    sex,
    ethnicity,
    smoking,
    smoking_dur,
    CASE WHEN autoimmune_disease = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN Immunosuppression = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN history_stroke = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN heart_disease = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN hypertension = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN asthma = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN COPD = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN Seizure = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN Migraine = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN insomnia = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN diabetes_one = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN diabetes_two = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN GI_disease = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN Cancer = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN anemia = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN renal_disease = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN COVID_vaccine_received = 1 THEN TRUE ELSE FALSE END,
    doses,
    CASE WHEN isolation = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN rash = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN Conjunctivitis = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN Anosmia = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN SOB = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN chest_pain = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN cough = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN sore_throat = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN runny_nose = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN dysguesia = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN GI_sx = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN muscle_joint = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN fatigue = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN fever = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN confusion = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN reinfection = 1 THEN TRUE ELSE FALSE END,
    CASE 
        WHEN age < 30 THEN 'young'
        WHEN age BETWEEN 30 AND 60 THEN 'middle'
        ELSE 'senior'
    END AS age_group
FROM long_covid_symptoms;


SELECT * FROM long_covid_symptoms_optimized LIMIT 10;



-- Total cases and long COVID rate
SELECT
COUNT(*) as total_cases,
SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized;

-- Top 10 most common symptoms
SELECT symptom, count FROM (
    SELECT 'Anosmia' as symptom, SUM(CASE WHEN Anosmia THEN 1 ELSE 0 END) as count
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'SOB', SUM(CASE WHEN SOB THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'chest_pain', SUM(CASE WHEN chest_pain THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'cough', SUM(CASE WHEN cough THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'sore_throat', SUM(CASE WHEN sore_throat THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'runny_nose', SUM(CASE WHEN runny_nose THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'dysguesia', SUM(CASE WHEN dysguesia THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'GI_sx', SUM(CASE WHEN GI_sx THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'muscle_joint', SUM(CASE WHEN muscle_joint THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
    UNION ALL
    SELECT 'fatigue', SUM(CASE WHEN fatigue THEN 1 ELSE 0 END)
    FROM long_covid_symptoms_optimized
) symptoms
ORDER BY count DESC
LIMIT 10;

-- Average age and BMI
SELECT 
    AVG(age) as avg_age,
    AVG(BMI) as avg_bmi
FROM long_covid_symptoms_optimized;

-- Grouping Symptoms by Demographic Factors
SELECT 
    age_group,
    sex,
    ethnicity,
    AVG(CASE WHEN fatigue THEN 1 ELSE 0 END) as avg_fatigue,
    AVG(CASE WHEN SOB THEN 1 ELSE 0 END) as avg_sob,
    AVG(CASE WHEN Anosmia THEN 1 ELSE 0 END) as avg_anosmia
FROM long_covid_symptoms_optimized
GROUP BY age_group, sex, ethnicity;

-- Calculating Correlation between Symptoms
SELECT 
    corr(CASE WHEN fatigue THEN 1 ELSE 0 END, CASE WHEN SOB THEN 1 ELSE 0 END) as fatigue_sob_corr,
    corr(CASE WHEN fatigue THEN 1 ELSE 0 END, CASE WHEN Anosmia THEN 1 ELSE 0 END) as fatigue_anosmia_corr,
    corr(CASE WHEN SOB THEN 1 ELSE 0 END, CASE WHEN Anosmia THEN 1 ELSE 0 END) as sob_anosmia_corr
FROM long_covid_symptoms_optimized;

-- Most Common Symptom Combinations
SELECT 
    CASE WHEN fatigue THEN 1 ELSE 0 END as fatigue,
    CASE WHEN SOB THEN 1 ELSE 0 END as SOB,
    CASE WHEN Anosmia THEN 1 ELSE 0 END as Anosmia,
    COUNT(*) as combination_count
FROM long_covid_symptoms_optimized
GROUP BY 
    CASE WHEN fatigue THEN 1 ELSE 0 END,
    CASE WHEN SOB THEN 1 ELSE 0 END,
    CASE WHEN Anosmia THEN 1 ELSE 0 END
ORDER BY combination_count DESC
LIMIT 5;

-- Analyze the relationship between age groups and Long COVID
SELECT 
    age_group,
    COUNT(*) as total_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized
GROUP BY age_group
ORDER BY long_covid_rate DESC;

--Examine the impact of smoking on Long COVID
SELECT 
    CASE 
        WHEN smoking = 0 THEN 'Non-smoker'
        WHEN smoking = 1 THEN 'Former smoker'
        WHEN smoking = 2 THEN 'Current smoker'
        ELSE 'Unknown'
    END as smoking_status,
    COUNT(*) as total_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized
GROUP BY 
    CASE 
        WHEN smoking = 0 THEN 'Non-smoker'
        WHEN smoking = 1 THEN 'Former smoker'
        WHEN smoking = 2 THEN 'Current smoker'
        ELSE 'Unknown'
    END
ORDER BY long_covid_rate DESC;


--Analyze the effect of vaccination on Long COVID
SELECT 
    CASE 
        WHEN COVID_vaccine_received THEN 'Vaccinated'
        ELSE 'Not Vaccinated'
    END as vaccination_status,
    COUNT(*) as total_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized
GROUP BY COVID_vaccine_received
ORDER BY long_covid_rate DESC;

-- Examine the relationship between pre-existing conditions and Long COVID
SELECT 
    'Autoimmune Disease' as condition,
    SUM(CASE WHEN autoimmune_disease THEN 1 ELSE 0 END) as condition_count,
    SUM(CASE WHEN autoimmune_disease AND long_covid THEN 1 ELSE 0 END) as long_covid_count,
    SUM(CASE WHEN autoimmune_disease AND long_covid THEN 1 ELSE 0 END) / SUM(CASE WHEN autoimmune_disease THEN 1 ELSE 0 END) as long_covid_rate
FROM long_covid_symptoms_optimized
UNION ALL
SELECT 
    'Heart Disease' as condition,
    SUM(CASE WHEN heart_disease THEN 1 ELSE 0 END) as condition_count,
    SUM(CASE WHEN heart_disease AND long_covid THEN 1 ELSE 0 END) as long_covid_count,
    SUM(CASE WHEN heart_disease AND long_covid THEN 1 ELSE 0 END) / SUM(CASE WHEN heart_disease THEN 1 ELSE 0 END) as long_covid_rate
FROM long_covid_symptoms_optimized
UNION ALL
SELECT 
    'Hypertension' as condition,
    SUM(CASE WHEN hypertension THEN 1 ELSE 0 END) as condition_count,
    SUM(CASE WHEN hypertension AND long_covid THEN 1 ELSE 0 END) as long_covid_count,
    SUM(CASE WHEN hypertension AND long_covid THEN 1 ELSE 0 END) / SUM(CASE WHEN hypertension THEN 1 ELSE 0 END) as long_covid_rate
FROM long_covid_symptoms_optimized
ORDER BY long_covid_rate DESC;

-- Analyze the correlation between BMI and Long COVID
SELECT 
    CASE 
        WHEN BMI < 18.5 THEN 'Underweight'
        WHEN BMI BETWEEN 18.5 AND 24.9 THEN 'Normal weight'
        WHEN BMI BETWEEN 25 AND 29.9 THEN 'Overweight'
        WHEN BMI >= 30 THEN 'Obese'
        ELSE 'Unknown'
    END as bmi_category,
    COUNT(*) as total_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized
GROUP BY 
    CASE 
        WHEN BMI < 18.5 THEN 'Underweight'
        WHEN BMI BETWEEN 18.5 AND 24.9 THEN 'Normal weight'
        WHEN BMI BETWEEN 25 AND 29.9 THEN 'Overweight'
        WHEN BMI >= 30 THEN 'Obese'
        ELSE 'Unknown'
    END
ORDER BY long_covid_rate DESC;

-- Examine the relationship between isolation and Long COVID
SELECT 
    CASE WHEN isolation THEN 'Isolated' ELSE 'Not Isolated' END as isolation_status,
    COUNT(*) as total_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized
GROUP BY isolation
ORDER BY long_covid_rate DESC;


-- Analyze the impact of reinfection on Long COVID
SELECT 
    CASE WHEN reinfection THEN 'Reinfected' ELSE 'First Infection' END as infection_status,
    COUNT(*) as total_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) as long_covid_cases,
    SUM(CASE WHEN long_covid THEN 1 ELSE 0 END) / COUNT(*) as long_covid_rate
FROM long_covid_symptoms_optimized
GROUP BY reinfection
ORDER BY long_covid_rate DESC;
