# Data Storage and Structuring for CropCloudAnalytics: ETL Models

This document contains the Power Query M source code utilized in the CropCloudAnalytics model. It serves as a comprehensive repository for all queries related to data extraction, transformation, and loading processes within the application. The structured format allows for easy navigation and reference for developers and data analysts involved in maintaining and enhancing the CropCloudAnalytics system.

## Table of Contents
1. [Overview](#overview)
2. [Data Sources](#data-sources)
3. [Transformations](#transformations)
4. [Load Queries](#load-queries)
5. [Usage Examples](#usage-examples)
6. [Notes and Comments](#notes-and-comments)

## Overview of CropCloudAnalytics Data Storage and Structuring

At Abecera Agricultural Company, we utilize a star schema to analyze farming operations, enabling us to gain valuable insights from our data. Within this context, the key components of our star schema include:

- **Fact Table**: This central table contains quantitative data relevant to our farming operations, such as crop yields, irrigation costs, and labor hours. It serves as the primary source for analysis and includes keys that reference the associated dimension tables.
- **Dimension Tables**: These tables provide descriptive attributes related to the facts, including planting dates, crop varieties, farm locations, and weather conditions. Each dimension table connects to the fact table through a foreign key, allowing for detailed analysis from multiple perspectives.
- **Simplicity**: The star schema is designed for ease of use, making it straightforward for our team to understand and query the data. This simplified structure enhances query performance and accelerates data retrieval, empowering us to make timely decisions.
- **Denormalization**: To optimize performance and reduce query complexity, our dimension tables are often denormalized. This means they may contain redundant data, streamlining the querying process and enhancing overall efficiency in our data analysis.

The approach features six distinct clusters of queries, facilitating comprehensive analysis across various dimensions.

The fact table is **ANALITICA_ERP**, which corresponds to the analytical output table exported through SQL from the Hispatec ERP.Agro system [https://www.hispatec.com/productos/erpagro-software-agricultura/]. This step is programmed in SQL to export a table that is over 200 columns wide (each column corresponding to a different field) into the Power Query interface. **ANALITICA_ERP** includes all the main analytical accounting entries of the parent company, where a record is kept of all accounting entries following a specific structure that defines expenses and income by analytical project. An **analytical project** can be an investment account, a crop-parcel account, or an expense account for amortizations for instance. The concept of an analytical project is fundamental to the company's structure as it defines the basic integrated unit of all accounting events, referred to in BI terminology as facts. Therefore, this table is known in BI terms as the fact table; it is the center of the 'solar system' of data within the company, around which all other tables relate to one another.

One of the main advancements in agriculture promoted by the group is digitalization. Since 2019, the company has implemented an advanced ERP system (i.e., ERP.Agro), which has enabled the centralized digital storage of information on a large scale. This includes everything from field reports, which are records of operations specifying the resources used by each productive unit (farm, crop, parcel, sector), to billing, inventories, investments, machinery, subsidies, and other key aspects of the activity. Internally, the system is organized around this central concept called **analytical project (AP)**, which corresponds to the basic unit for allocating expenses and income, consumptions, and products produced. This concept allows for the assignment of operations related to cash flows (expenses and income) that influence the company's accounting and facilitates the productive use of information, both for describing processes and identifying behavioral patterns, as well as for defining operational control systems. The **AP** is essential for the functioning of the data system at Cortijo La Reina and constitutes the foundation upon which information related to activity management is organized. From an agronomic perspective, the use of this information has great potential, which is why its analysis and development has been defined as a key pillar in the company's strategy.

From the perspective of data storage and structuring, the group has benefited from the ERP.Agro solution by Hispatec. However, centralizing all data on a single platform has presented a critical challenge since 2019: promoting the ability to leverage this information operationally, in real-time, without incurring excessive costs related to data management and analysis, including extraction, cleaning, and formatting of reports.

Another crucial aspect is that analytics in a company like the Cortijo La Reina group must be treated as an analytical project in itself. This implies that it should have a clear margin of opportunity for value creation, which is measurable and objectively identifiable. In other words, analytics should be governed by a relationship between expenses and income, where the actual viability of the system is justified by revenue generation or cost reduction, thereby improving the company's performance. Otherwise, there is a risk of incurring significant efforts in documentation, storage, analysis, and reporting that do not translate into progress, but rather result in increased expenses and unnecessary time consumption. To address this issue, the integration of ERP data with a Business Intelligence (BI) tool that incorporates ETL (Extract, Transform, Load) solutions has been identified as a key strategy. This integration enables automation in handling information, reducing the need to allocate substantial resources in terms of time and specialized knowledge each time a control task is required for a specific project.

Furthermore, a fundamental aspect is the ability to hierarchize information, allowing for scalable analysis that addresses both specific and general aspects of our operations. For example, the ability to track and analyze data from the use of a phytosanitary product in a specific field to a broader level, such as an entire farm or even the entire company, provides a multiscalar perspective in decision-making. This enables detailed management of analytics, such as determining the exact dosage of treatment applied in previous campaigns, as well as evaluating which farm or entity optimizes the use of phytosanitary products and the implications on the operational results of the company.

----


## Data Sources
List and describe the various data sources from which the queries extract data, including any relevant connection details.

## Transformations
Document the different transformations applied to the data, including the Power Query M code snippets for each transformation.

## Load Queries
Include the queries responsible for loading the transformed data into the appropriate destinations (e.g., databases, data warehouses).

## Usage Examples
Provide examples of how to utilize the queries effectively within CropCloud Analytics, including any necessary parameters or configuration settings.

## Notes and Comments
Include any additional notes, comments, or considerations relevant to the Power Query M source code.


