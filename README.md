# longcovid
Big Data Analysis of Long COVID Symptoms

By incorporating temporal analysis and considering various factors such as demographics, pre-existing conditions, and lifestyle variables, this research seeks to provide a nuanced understanding of Long COVID symptom patterns and their progression over time. The integration of clinical and social media data allows for a unique perspective that captures both medical realities and public experiences of the condition.

The use of MapReduce algorithms for symptom co-occurrence analysis and sentiment trend analysis represents an innovative approach to processing large volumes of health-related data. This methodology has the potential to uncover hidden patterns and relationships that may not be apparent through traditional analysis methods, contributing to a more detailed understanding of Long COVID symptomatology.

## Description of the Long COVID Symptoms Dataset

**Source**

The dataset, titled "Long Covid Risk", was authored by Ahmed Shaheen and published on Figshare, a reputable online repository for research data. This comprehensive dataset was made available on April 14, 2024, providing researchers with valuable information for studying Long COVID risk factors and symptoms.
https://figshare.com/articles/dataset/Long_Covid_Risk/25599591

**Data Structure and Format**

The dataset is structured as a tabular format, presented in an Excel spreadsheet (mainCov.xlsx). It comprises 41 features, including demographic information, pre-existing medical conditions, COVID-19 vaccination status, and various symptoms associated with Long COVID. The data is primarily categorical, with binary indicators (0 or 1) for most variables, while some features like age and BMI are continuous. This format allows for efficient data manipulation and analysis using various statistical and machine learning techniques.


## Description of the Twitter Dataset

To complement structured clinical data on COVID-19, a Twitter dataset was employed to provide insights into public discourse and experiences related to the pandemic. This dataset serves as a contrasting source of information, offering real-time, unstructured data that can be compared and integrated with clinical findings.

The Twitter data was obtained from a publicly available dataset on Kaggle, titled "COVID19 Tweets"(https://doi.org/10.34740/KAGGLE/DSV/1451513). This dataset was downloaded as a CSV file (covid19_tweets.csv), providing a comprehensive collection of tweets related to the COVID-19 pandemic. The use of a pre-existing dataset aligns with big data principles, demonstrating the ability to leverage large volumes of data from social media platforms for research purposes.
