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

The approach features five distinct clusters of queries, facilitating comprehensive analysis across various dimensions.

The fact table is **ANALITICA_ERP**, which corresponds to the analytical output table exported through SQL (SQL Server Management Studio by Microsoft) from the Hispatec ERP.Agro system [https://www.hispatec.com/productos/erpagro-software-agricultura/]. This step is programmed in SQL to export a table that is over 200 columns wide (each column corresponding to a different field) into the Power Query interface. **ANALITICA_ERP** includes all the main analytical accounting entries of the parent company, where a record is kept of all accounting entries following a specific structure that defines expenses and income by analytical project. An **analytical project** can be an investment account, a crop-parcel account, or an expense account for amortizations for instance. The concept of an analytical project is fundamental to the company's structure as it defines the basic integrated unit of all accounting events, referred to in BI terminology as facts. Therefore, this table is known in BI terms as the fact table; it is the center of the 'solar system' of data within the company, around which all other tables relate to one another.

One of the main advancements in agriculture promoted by the group is digitalization. Since 2019, the company has implemented an advanced ERP system (i.e., ERP.Agro), which has enabled the centralized digital storage of information on a large scale. This includes everything from field reports, which are records of operations specifying the resources used by each productive unit (farm, crop, parcel, sector), to billing, inventories, investments, machinery, subsidies, and other key aspects of the activity. Internally, the system is organized around this central concept called **analytical project (AP)**, which corresponds to the basic unit for allocating expenses and income, consumptions, and products produced. This concept allows for the assignment of operations related to cash flows (expenses and income) that influence the company's accounting and facilitates the productive use of information, both for describing processes and identifying behavioral patterns, as well as for defining operational control systems. The **AP** is essential for the functioning of the data system at Cortijo La Reina and constitutes the foundation upon which information related to activity management is organized. From an agronomic perspective, the use of this information has great potential, which is why its analysis and development has been defined as a key pillar in the company's strategy.

From the perspective of data storage and structuring, the group has benefited from the ERP.Agro solution by Hispatec. However, centralizing all data on a single platform has presented a critical challenge since 2019: promoting the ability to leverage this information operationally, in real-time, without incurring excessive costs related to data management and analysis, including extraction, cleaning, and formatting of reports.

Another crucial aspect is that analytics in a company like the Cortijo La Reina group must be treated as an analytical project in itself. This implies that it should have a clear margin of opportunity for value creation, which is measurable and objectively identifiable. In other words, analytics should be governed by a relationship between expenses and income, where the actual viability of the system is justified by revenue generation or cost reduction, thereby improving the company's performance. Otherwise, there is a risk of incurring significant efforts in documentation, storage, analysis, and reporting that do not translate into progress, but rather result in increased expenses and unnecessary time consumption. To address this issue, the integration of ERP data with a Business Intelligence (BI) tool that incorporates ETL (Extract, Transform, Load) solutions has been identified as a key strategy. This integration enables automation in handling information, reducing the need to allocate substantial resources in terms of time and specialized knowledge each time a control task is required for a specific project.

Furthermore, a fundamental aspect is the ability to hierarchize information, allowing for scalable analysis that addresses both specific and general aspects of our operations. For example, the ability to track and analyze data from the use of a phytosanitary product in a specific field to a broader level, such as an entire farm or even the entire company, provides a multiscalar perspective in decision-making. This enables detailed management of analytics, such as determining the exact dosage of treatment applied in previous campaigns, as well as evaluating which farm or entity optimizes the use of phytosanitary products and the implications on the operational results of the company.

## Data Sources

Apart from the first cluster, the **Fact Table**, where a single query is stored (i.e., **ANALITICA_ERP**), the system is subdivided into four additional clusters of tables that orbit around the fact table. These four clusters are as follows:

**ERP Tables Satellite [N=7]**
- PARTES_ERP
- Areas
- Sociedades
- Fincas
- Calendario
- PARTES_keys
- Cultivos

**Aguas-Riego [N=7]**
- DATOS-RIEGO
- AGUAS-RIEGO
- SALDO_RIEGO (farm ID)
- Riego_total
- Riego_areas
- Riego_reparto
- Riego_keys

**Wiseconn CropCloud [N=13]**
- Sampling zone
- API - Wiseconn Sensors
- Crop Water Functions
- API SWC (20 cm)
- API SWC (30 cm)
- API SWC (40 cm)
- API SWC (50 cm)
- API SWC (60 cm)
- API Temperature
- API Rain
- API ETo
- Field Validation Data (crop ID)
- HI (crop ID)

**Other Consults [N=3]**
- fCalendario 
- DAX
- Roles

## Transformations

The following M script implements an **ETL** (Extract, Transform, Load) process for the **ANALITICA_ERP** table within the SQL **REPORTING** database of the ANALITICA_ERP system. It begins by connecting to the SQL database and retrieving the relevant data while filtering out rows with null values in the 'EjercicioAnalitico' column (i.e., accounting period). The script then enriches the data by adding new columns, such as Campaña (i.e., the agricultural accounting period refering to the timeframe used for financial reporting and analysis, specifically aligned with the crop season) and ID.AREA (i.e., the elementary key is generated to link the fact table with the AREAS table. This elementary key is essential for connecting an analytical project to a specific crop field area, as it relates the analytical project - or crop - to the area by considering the year. Since analytical project areas change annually due to annual crops and crop rotations, this temporal aspect is crucial), which are derived from existing fields, facilitating easier analysis. 

Subsequent transformations include calculating the **IMPORTE.APLICADO** based on specified conditions and creating derived metrics like **Importe_ha** which determines the amount by area (this column records all facts associated with resource flows per area). The final output is a cleaned and structured table ready for reporting, with unnecessary columns removed and relevant types assigned for effective data analysis.

```m
let
    // Source: Connect to the SQL database
    Origen = Sql.Databases("WIN-19AB\ERP"),
    
    // Access the 'REPORTING' database
    REPORTING = Origen{[Name="REPORTING"]}[Data],
    
    // Access the 'ANALITICA_CULTIVO' table in the 'dbo' schema
    dbo_ANALITICA_CULTIVO = REPORTING{[Schema="dbo",Item="ANALITICA_CULTIVO"]}[Data],

    // Filter out rows with null values in 'EjercicioAnalitico'
    #"Reject NULL rows [EjercicioAnalitico]" = Table.SelectRows(dbo_ANALITICA_CULTIVO, each ([EjercicioAnalitico] <> null)),
    
    // Add a new column 'Campaña' that duplicates 'EjercicioAnalitico'
    #"Add a new column [Campaña]" = Table.AddColumn(#"Reject NULL rows [EjercicioAnalitico]", "Campaña", each [EjercicioAnalitico]),
    
    // Add a column with text before the first space in 'EjercicioAnalitico'
    #"Inserted Text Before Delimiter [EjercicioAnalitico -> Campaña]" = Table.AddColumn(#"Add a new column [Campaña]", "Text Before Delimiter", each Text.BeforeDelimiter([EjercicioAnalitico], " "), type text),
    
    // Remove the 'Campaña' column from the table
    #"Remove Column [Campaña]" = Table.RemoveColumns(#"Inserted Text Before Delimiter [EjercicioAnalitico -> Campaña]",{"Campaña"}),
    
    // Rename the 'Text Before Delimiter' column to 'Campaña'
    #"Rename Column [Text Before Delimiter -> Campaña]" = Table.RenameColumns(#"Remove Column [Campaña]",{{"Text Before Delimiter", "Campaña"}}),
    
    // Add a column with text before the delimiter "-" in 'PROYECTO'
    #"Texto insertado antes del delimitador2" = Table.AddColumn(#"Rename Column [Text Before Delimiter -> Campaña]", "Texto antes del delimitador", each Text.BeforeDelimiter([PROYECTO], " -"), type text),
    
    // Combine 'Texto antes del delimitador' and 'Campaña' into a new column 'ID.AREA'
    #"Create ID.AREA" = Table.CombineColumns(#"Texto insertado antes del delimitador2",{"Texto antes del delimitador", "Campaña"},Combiner.CombineTextByDelimiter(":", QuoteStyle.None),"ID.AREA"),
    
    // Extract text before the delimiter ":" from 'ID.AREA'
    #"Texto insertado antes del delimitador" = Table.AddColumn(#"Create ID.AREA", "Texto antes del delimitador", each Text.BeforeDelimiter([ID.AREA], ":"), type text),
    
    // Rename the column containing text before the delimiter to 'Proyecto_0'
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Texto insertado antes del delimitador",{{"Texto antes del delimitador", "Proyecto_0"}}),
    
    // Extract text after the delimiter ":" in 'ID.AREA'
    #"Texto insertado después del delimitador" = Table.AddColumn(#"Columnas con nombre cambiado", "Texto después del delimitador", each Text.AfterDelimiter([ID.AREA], ":"), type text),
    
    // Rename the extracted text column to 'Campaña_0'
    #"Columnas con nombre cambiado2" = Table.RenameColumns(#"Texto insertado después del delimitador",{{"Texto después del delimitador", "Campaña_0"}}),
    
    // Filter rows where 'CODIGOCENTRO' equals "001"
    #"Filas filtradas" = Table.SelectRows(#"Columnas con nombre cambiado2", each ([CODIGOCENTRO] = "001")),

    // Merge the filtered table with 'Areas' on 'ID.AREA'
    #"Merged Queries - AÑADIR AREAS" = Table.NestedJoin(#"Filas filtradas", {"ID.AREA"}, Areas, {"ID.AREA"}, "AREAS", JoinKind.LeftOuter),
    
    // Expand the merged 'AREAS' column to include 'Campaña' and 'Superficie_ha'
    #"Expand new merged table" = Table.ExpandTableColumn(#"Merged Queries - AÑADIR AREAS", "AREAS", {"Campaña", "Superficie_ha"}, {"Campaña", "Superficie_ha"}),

    // Rename columns for clarity
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Expand new merged table",{{"PROYECTO", "PROYECTO_1"},{"Campaña", "Campaña_1"}}),
    
    // Create a new column 'PROYECTO' based on null checks
    #"Columnas con nombre cambiado1.1"= Table.AddColumn(#"Columnas con nombre cambiado1", "PROYECTO", each if [PROYECTO_1] = null then [Proyecto_0] else [PROYECTO_1]),
    
    // Create a new column 'Campaña' based on null checks
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas con nombre cambiado1.1", "Campaña", each if [Campaña_1] = null then [Campaña_0] else [Campaña_1]),
    
    // Remove unnecessary columns
    #"Columnas quitadas" = Table.RemoveColumns(#"Columna condicional agregada",{"Proyecto_0", "PROYECTO_1", "Campaña_0", "Campaña_1"}),
    
    // Change the type of 'FECHAIMPUTACION' to date
    #"Changed Type [FECHAIMPUTACION] in es-ES" = Table.TransformColumnTypes(#"Columnas quitadas", {{"FECHAIMPUTACION", type date}}, "es-ES"),
    
    // Change the type of 'Superficie_ha' and 'IMPORTEAPLICADO' to number
    #"Changed Type [Superficie_ha + IMPORTEAPLICADO]" = Table.TransformColumnTypes(#"Changed Type [FECHAIMPUTACION] in es-ES",{{"Superficie_ha", type number}, {"IMPORTEAPLICADO", type number}}),
    
    // Create a new column 'IMPORTE.señal' to determine the signal type based on 'TIPOGRUPO'
    #"Columna IMPORTE.señal" = Table.AddColumn(#"Changed Type [Superficie_ha + IMPORTEAPLICADO]", "IMPORTE.señal", each if [TIPOGRUPO] = "Gastos" then -1 else 1),
    
    // Calculate 'IMPORTE.APLICADO' using the new signal column
    #"Include IMPORTE.señal -> IMPORTEAPLICADO" = Table.AddColumn(#"Columna IMPORTE.señal", "IMPORTE.APLICADO", each [IMPORTEAPLICADO] * [IMPORTE.señal], type number),
    
    // Remove the original 'IMPORTEAPLICADO' column
    #"Delete old IMPORTEAPLICADO" = Table.RemoveColumns(#"Include IMPORTE.señal -> IMPORTEAPLICADO",{"IMPORTEAPLICADO"}),
    
    // Rename 'IMPORTE.APLICADO' to 'IMPORTEAPLICADO'
    #"Rename new IMPORTEAPLICADO" = Table.RenameColumns(#"Delete old IMPORTEAPLICADO",{{"IMPORTE.APLICADO", "IMPORTEAPLICADO"}}),
    
    // Define 'Importe_ha' as the ratio of 'IMPORTEAPLICADO' to 'Superficie_ha'
    #"Define new column [Importe_ha]" = Table.AddColumn(#"Rename new IMPORTEAPLICADO", "Importe_ha", each [IMPORTEAPLICADO]/[Superficie_ha]),
    
    // Change the type of 'Importe_ha' to number
    #"Changed Type [Importe_ha] to ""number""" = Table.TransformColumnTypes(#"Define new column [Importe_ha]",{{"Importe_ha", type number}}),
    
    // Duplicate 'IMPORTEAPLICADO' column
    #"Duplicate Column [IMPORTEAPLICADO - Copy]" = Table.DuplicateColumn(#"Changed Type [Importe_ha] to ""number""", "IMPORTEAPLICADO", "IMPORTEAPLICADO - Copy"),
    
    // Rename the duplicated column to 'Importe_global'
    #"Rename Column [Importe_global]" = Table.RenameColumns(#"Duplicate Column [IMPORTEAPLICADO - Copy]",{{"IMPORTEAPLICADO - Copy", "Importe_global"}}),
    
    // Change the type of 'Importe_ha' to number in Spanish format
    #"Changed Type [Importe_ha] to ""number"" in es-ES" = Table.TransformColumnTypes(#"Rename Column [Importe_global]", {{"Importe_ha", type number}}, "es-ES"),
    
    // Change the type of 'Importe_global' to number in Spanish format
    #"Changed Type [Importe_global] to ""number"" in es-ES" = Table.TransformColumnTypes(#"Changed Type [Importe_ha] to ""number"" in es-ES", {{"Importe_global", type number}}, "es-ES"),
    
    // Rename 'NumeroParteProduccion' to 'NUMERO_PPARTE'
    #"Renamed Column [NUMERO_PPARTE]" = Table.RenameColumns(#"Changed Type [Importe_global] to ""number"" in es-ES",{{"NumeroParteProduccion", "NUMERO_PPARTE"}}),
    
    // Change the type of 'NUMERO_PPARTE' to text
    #"Changed Type [NUMERO_PPARTE] to text" = Table.TransformColumnTypes(#"Renamed Column [NUMERO_PPARTE]",{{"NUMERO_PPARTE", type text}}),
    
    // Change the type of 'ID.AREA' to text
    #"Changed Type [ID.AREA] to text" = Table.TransformColumnTypes(#"Changed Type [NUMERO_PPARTE] to text",{{"ID.AREA", type text}}),

    // Remove the 'TIPOGRUPO' column
    #"Removed Column [TIPOGRUPO]" = Table.RemoveColumns(#"Changed Type [ID.AREA] to text",{"TIPOGRUPO"})
in
    // Output the final table
    #"Removed Column [TIPOGRUPO]"


## Load Queries
Include the queries responsible for loading the transformed data into the appropriate destinations (e.g., databases, data warehouses).

## Usage Examples
Provide examples of how to utilize the queries effectively within CropCloud Analytics, including any necessary parameters or configuration settings.

## Notes and Comments
Include any additional notes, comments, or considerations relevant to the Power Query M source code.


