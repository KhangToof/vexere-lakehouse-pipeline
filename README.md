# 📌 Project: Improve the quality of bus ticket booking service through lakehouse platform
![image](https://github.com/user-attachments/assets/07560d48-52b4-44dd-aa78-f047f9fd69f3)

## 1. 🚀 Introduce Project
- The topic is built based on the data of bus trips from Ho Chi Minh City to 13 provinces and cities in the West of Vietnam.
- Design the Lakehouse model to simulate the big data poured in from data scraping.
- The output is charts showing parameters, fares, utilities, etc... to serve businesses.

## 2. 🗄️ Data Source
- Source data is scraped through python language selenium library.
- Collected through [vexere.com](https://vexere.com/) website.
- There are 3 main data sets collected: bus tickets, bus trip utilities, customer reviews with comments.
- Divided into 2 main data sets: CSV for bus tickets, JSON for utilities and comments.
- Sample ticket data: ![image](https://github.com/user-attachments/assets/e15d7176-66a4-470d-9580-09287b1c5351) ![image](https://github.com/user-attachments/assets/cafed35a-73a4-4427-a0b2-564415fb6a64)
- Sample data about ride facility: ![image](https://github.com/user-attachments/assets/76e1a1cb-df34-4597-920c-74b652e7cbc4)
- Sample data on customer comments: ![image](https://github.com/user-attachments/assets/43902afe-a166-4fa7-84c7-493b1ea5b72a)

## 3. 💡 System Design
![image](https://github.com/user-attachments/assets/973d86f1-8ece-4cae-b656-00883947f25c)
- The system is designed through 4 main items:
- First is the source data scraped from the website, then diversify the input data by deploying to save into 2 file formats: CSV and JSON
- Next is the storage layer that will use MinIO to store 3 data layers along with storing raw data on Ubuntu Server.
- The 3 data layers will be designed according to: bronze, silver, gold. In which, the bronze layer has the role of storing raw data, the silver layer has the role of storing pre-processed data as well as staging for the process into the gold layer. Finally, the gold layer is where the data model is designed according to the rules of the Data Warehouse (Galaxy Schema model). And all data in these 3 layers will be unified into 1 form through Delta Lake.
- Next, the model will apply NLP (natural language processing) to evaluate and analyze comment sentiment. Along with using Presto as a high performance query engine to MinIO via Hive Metastore.
- Finally visualized via Metabase.

## 4. 🏛️ Warehouse Model Design
![image](https://github.com/user-attachments/assets/26f8e129-be5d-445d-b0b8-aabd13935360)
- The data warehouse model is designed based on galaxy schema, with 2 fact tables and 5 dim tables.
- The 2 main Fact tables are about: attributes, measures related to bus tickets and attributes, measures related to customer comments.

## 5. 🖼️ Visualization
The problem analyzed here is related to improving the quality of bus ticket booking services, the target users here are bus companies. The main target of analysis is the bus companies and the customers using the service. From these issues, the following questions are raised:
- Statistics on the number of tickets sold daily, along with the average ticket price of each trip running on each route.
- Analysis of ticket booking trends by vehicle type and price.
- Analysis of customer satisfaction when using the service.
![image](https://github.com/user-attachments/assets/5566d4b7-02e6-4f50-acfd-8bbeb0dc66a2)
![image](https://github.com/user-attachments/assets/ce16d849-fdc8-4360-b87e-acb3e30324f9)
![image](https://github.com/user-attachments/assets/6b3b18f8-8961-4fda-a25b-db4234437bcc)


