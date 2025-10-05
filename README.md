## 🏗️ Building a Data Warehouse Project

Hello and welcome to my first project!  
I am building a **data warehouse** using **SQL Server**, including *ETL processes* and *data modeling*.  
This project was created to review and test my SQL skills.

The information will be loaded into the data warehouse from CSV files extracted from CRM and ERP systems.  
The data warehouse will be built following the **Medallion Architecture**, which includes three layers: *bronze*, *silver*, and *gold*.

- **Bronze layer:** Contains the raw data with no transformations. The load method will be batch processing using a full load (truncate and insert).  
- **Silver layer:** This layer performs data transformations such as cleansing and standardization. The load method will be the same as in the bronze layer.  
- **Gold layer:** Here, data integrations, aggregations, and business logic are applied. The data model (star schema) includes two dimensions (*dim_customers* and *dim_products*) and one fact table (*fact_sales*).

<img width="624" height="420" alt="Datawarehouse project" src="https://github.com/user-attachments/assets/5a3c5cad-b1d5-47e7-aa05-27b1223692eb" />

Special thanks to [@DataWithBaraa](https://github.com/DataWithBaraa) for the incredible content and guidance: 
[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/watch?v=9GVqKuTVANE)

---

## 👩‍💻 About Me

Hi there! I’m **Valeria Yagui**, from Lima, Peru.  
I'm currently pursuing a **Master’s degree in Digital Business Management** at **Hochschule Pforzheim** in Germany.  
I enjoy working with data and learning new tools and technologies.  

Feel free to connect with me on LinkedIn:  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/valeria-yagui-nishii/)
