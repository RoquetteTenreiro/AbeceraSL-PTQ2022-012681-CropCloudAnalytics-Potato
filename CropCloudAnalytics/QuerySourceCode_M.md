# Data Storage and Structuring for CropCloudAnalytics: ETL Models

This document contains the Power Query M source code utilized in the CropCloudAnalytics model. It serves as a comprehensive repository for all queries related to data extraction, transformation, and loading processes within the application. The structured format allows for easy navigation and reference for developers and data analysts involved in maintaining and enhancing the CropCloudAnalytics system.

## Table of Contents
1. [Overview of CropCloudAnalytics](#overview-of-cropcloudanalytics)
2. [Data Sources](#data-sources)
3. [Data Transformations](#data-transformations)
4. [Other Queries](#other-queries)
5. [DAX code](#dax-code)
6. [Notes and Comments](#notes-and-comments)

## Overview of CropCloudAnalytics Data Storage and Structuring

At Abecera Agricultural Company, we utilize a star schema to analyze farming operations, enabling us to gain valuable insights from our data. Within this context, the key components of our star schema include:

- **Fact Table**: This central table contains quantitative data relevant to our farming operations, such as crop yields, irrigation costs, and labor hours. It serves as the primary source for analysis and includes keys that reference the associated dimension tables.
- **Dimension Tables**: These tables provide descriptive attributes related to the facts, including planting dates, crop varieties, farm locations, and weather conditions. Each dimension table connects to the fact table through a foreign key, allowing for detailed analysis from multiple perspectives.
- **Simplicity**: The star schema is designed for ease of use, making it straightforward for our team to understand and query the data. This simplified structure enhances query performance and accelerates data retrieval, empowering us to make timely decisions.
- **Denormalization**: To optimize performance and reduce query complexity, our dimension tables are often denormalized. This means they may contain redundant data, streamlining the querying process and enhancing overall efficiency in our data analysis.

The approach features five distinct clusters of queries, facilitating comprehensive analysis across various dimensions.

The fact table is **ANALITICA_ERP**, which corresponds to the analytical output table exported through SQL (SQL Server Management Studio by Microsoft) from the Hispatec ERP.Agro system [https://www.hispatec.com/productos/erpagro-software-agricultura/]. This step is programmed in SQL to export a table that is over 200 columns wide (each column corresponding to a different field) into the Power Query interface. **ANALITICA_ERP** includes all the main analytical accounting entries of the parent company, where a record is kept of all accounting entries following a specific structure that defines expenses and income by analytical project. An **analytical project** can be an investment account, a crop-parcel account, or an expense account for amortizations for instance. The concept of an analytical project is fundamental to the company's structure as it defines the basic integrated unit of all accounting events, referred to in BI terminology as facts. Therefore, this table is known in BI terms as the fact table; it is the center of the 'solar system' of data within the company, around which all other tables relate to one another.

One of the main advancements in agriculture promoted by the group is digitalization. Since 2019, the company has implemented an advanced ERP system (i.e., ERP.Agro), which has enabled the centralized digital storage of information on a large scale. This includes everything from field reports, which are records of operations specifying the resources used by each productive unit (farm, crop, parcel, sector), to billing, inventories, investments, machinery, subsidies, and other key aspects of the activity. Internally, the system is organized around this central concept called **analytical project**, which corresponds to the basic unit for allocating expenses and income, consumptions, and products produced. This concept allows for the assignment of operations related to cash flows (expenses and income) that influence the company's accounting and facilitates the productive use of information, both for describing processes and identifying behavioral patterns, as well as for defining operational control systems. The **analytical project** is essential for the functioning of the data system at Cortijo La Reina and constitutes the foundation upon which information related to activity management is organized. From an agronomic perspective, the use of this information has great potential, which is why its analysis and development has been defined as a key pillar in the company's strategy.

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

## Data Transformations

### ANALITICA_ERP

The following M script implements an **ETL** (Extract, Transform, Load) process for the **ANALITICA_ERP** table within the SQL **REPORTING** database of the ANALITICA_ERP system. It begins by connecting to the SQL database and retrieving the relevant data while filtering out rows with null values in the 'EjercicioAnalitico' column (i.e., accounting period). The script then enriches the data by adding new columns, such as Campaña (i.e., the agricultural accounting period refering to the timeframe used for financial reporting and analysis, specifically aligned with the crop season) and ID.AREA (i.e., the elementary key is generated to link the fact table with the AREAS table. This elementary key is essential for connecting an analytical project to a specific crop field area, as it relates the analytical project - or crop - to the area by considering the year. Since analytical project areas change annually due to annual crops and crop rotations, this temporal aspect is crucial), which are derived from existing fields, facilitating easier analysis. 

Subsequent transformations include calculating the **IMPORTE.APLICADO** based on specified conditions and creating derived metrics like **Importe_ha** which determines the amount by area (this column records all facts associated with resource flows per area). The final output is a cleaned and structured table ready for reporting, with unnecessary columns removed and relevant types assigned for effective data analysis.

```vs
let
    // Source
    Origen = Sql.Databases("WIN-19AB\ERP"),
    REPORTING = Origen{[Name="REPORTING"]}[Data],
    dbo_ANALITICA_CULTIVO = REPORTING{[Schema="dbo",Item="ANALITICA_CULTIVO"]}[Data],
```
This code connects to a SQL Server instance named **"WIN-19AB\ERP"** and retrieves data from the **"REPORTING"** database. Within this database, it specifically accesses a table called **"ANALITICA_CULTIVO"** under the "dbo" schema.

```vs
    // Filter and Transform Data
    /*#"PROYECTOS - FASE 1" = Table.SelectRows(dbo_ANALITICA_CULTIVO, each ([PROYECTO] = "F923000001 - ESPINACAS GELAGRI" or [PROYECTO] = "F925000006 - ALMENDROS 2016")),
    #"Reject NULL rows [EjercicioAnalitico]" = Table.SelectRows(#"PROYECTOS - FASE 1", each ([EjercicioAnalitico] <> null)),*/
    #"Reject NULL rows [EjercicioAnalitico]" = Table.SelectRows(dbo_ANALITICA_CULTIVO, each ([EjercicioAnalitico] <> null)),
    #"Add a new column [Campaña]" = Table.AddColumn(#"Reject NULL rows [EjercicioAnalitico]", "Campaña", each [EjercicioAnalitico]),
    #"Inserted Text Before Delimiter [EjercicioAnalitico -> Campaña]" = Table.AddColumn(#"Add a new column [Campaña]", "Text Before Delimiter", each Text.BeforeDelimiter([EjercicioAnalitico], " "), type text),
    #"Remove Column [Campaña]" = Table.RemoveColumns(#"Inserted Text Before Delimiter [EjercicioAnalitico -> Campaña]",{"Campaña"}),
    #"Rename Column [Text Before Delimiter -> Campaña]" = Table.RenameColumns(#"Remove Column [Campaña]",{{"Text Before Delimiter", "Campaña"}}),
    #"Texto insertado antes del delimitador2" = Table.AddColumn(#"Rename Column [Text Before Delimiter -> Campaña]", "Texto antes del delimitador", each Text.BeforeDelimiter([PROYECTO], " -"), type text),
    #"Create ID.AREA" = Table.CombineColumns(#"Texto insertado antes del delimitador2",{"Texto antes del delimitador", "Campaña"},Combiner.CombineTextByDelimiter(":", QuoteStyle.None),"ID.AREA"),
    #"Texto insertado antes del delimitador" = Table.AddColumn(#"Create ID.AREA", "Texto antes del delimitador", each Text.BeforeDelimiter([ID.AREA], ":"), type text),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Texto insertado antes del delimitador",{{"Texto antes del delimitador", "Proyecto_0"}}),
    #"Texto insertado después del delimitador" = Table.AddColumn(#"Columnas con nombre cambiado", "Texto después del delimitador", each Text.AfterDelimiter([ID.AREA], ":"), type text),
    #"Columnas con nombre cambiado2" = Table.RenameColumns(#"Texto insertado después del delimitador",{{"Texto después del delimitador", "Campaña_0"}}),
    #"Filas filtradas" = Table.SelectRows(#"Columnas con nombre cambiado2", each ([CODIGOCENTRO] = "001")),
```
This section of the code performs a series of filtering and transformation steps on the data from the **"ANALITICA_CULTIVO"** table. First, it filters out any rows where the EjercicioAnalitico column contains null values, ensuring that only valid data is processed. Following this, a new column named Campaña is added, which contains the same values as **EjercicioAnalitico**. The next step extracts text before the first space in the EjercicioAnalitico column and stores this in a temporary column. Subsequently, the temporary **Campaña** column is removed to streamline the dataset. The extracted column is then renamed to Campaña to reflect its intended use. Next, the code extracts text before the first " - " in the **PROYECTO** column, creating another temporary column. This extracted text, along with the **Campaña** values, is then combined into a new column called **ID.AREA**, using a colon (:) as the delimiter. The process continues with the extraction of text before the first colon in the **ID.AREA** column, which is stored in another temporary column. Finally, this column is renamed to **Proyecto_0**, thus completing the transformation sequence, while additional filtering is applied to retain only those rows where the **CODIGOCENTRO** column equals '001' (i.e., agricultural analytical projects, as '002' corresponds to the tourism business by Abecera, S.L.).
```vs
    // Filter and Merge Areas Data
    #"Merged Queries - AÑADIR AREAS" = Table.NestedJoin(#"Filas filtradas", {"ID.AREA"}, Areas, {"ID.AREA"}, "AREAS", JoinKind.LeftOuter),
    #"Expand new merged table" = Table.ExpandTableColumn(#"Merged Queries - AÑADIR AREAS", "AREAS", {"Campaña", "Superficie_ha"}, {"Campaña", "Superficie_ha"}),
```
The provided code performs data manipulation within a Power Query environment, specifically focusing on filtering and merging area-related data. Initially, it executes a left outer join between a filtered table, referenced as **#"Filas filtradas"**, and an external table named Areas, matching on the column **ID.AREA**. This operation results in a new column called "AREAS" that contains related records from the Areas table. Subsequently, the code expands this newly merged table by extracting two specific fields **"Campaña"** and **"Superficie_ha"** from the **"AREAS"** column, effectively incorporating these attributes into the main table structure. This process enhances the dataset with additional information relevant to each area, facilitating further analysis.
```vs
    // Transform Data
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Expand new merged table",{{"PROYECTO", "PROYECTO_1"},{"Campaña", "Campaña_1"}}),
    #"Columnas con nombre cambiado1.1"= Table.AddColumn(#"Columnas con nombre cambiado1", "PROYECTO", each if [PROYECTO_1] = null then [Proyecto_0] else [PROYECTO_1]),
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas con nombre cambiado1.1", "Campaña", each if [Campaña_1] = null then [Campaña_0] else [Campaña_1]),
    #"Columnas quitadas" = Table.RemoveColumns(#"Columna condicional agregada",{"Proyecto_0", "PROYECTO_1", "Campaña_0", "Campaña_1"}),
    #"Changed Type [FECHAIMPUTACION] in es-ES" = Table.TransformColumnTypes(#"Columnas quitadas", {{"FECHAIMPUTACION", type date}}, "es-ES"),
    #"Changed Type [Superficie_ha + IMPORTEAPLICADO]" = Table.TransformColumnTypes(#"Changed Type [FECHAIMPUTACION] in es-ES",{{"Superficie_ha", type number}, {"IMPORTEAPLICADO", type number}}),
    #"Columna IMPORTE.señal" = Table.AddColumn(#"Changed Type [Superficie_ha + IMPORTEAPLICADO]", "IMPORTE.señal", each if [TIPOGRUPO] = "Gastos" then -1 else 1),
    #"Include IMPORTE.señal -> IMPORTEAPLICADO" = Table.AddColumn(#"Columna IMPORTE.señal", "IMPORTE.APLICADO", each [IMPORTEAPLICADO] * [IMPORTE.señal], type number),
    #"Delete old IMPORTEAPLICADO" = Table.RemoveColumns(#"Include IMPORTE.señal -> IMPORTEAPLICADO",{"IMPORTEAPLICADO"}),
    #"Rename new IMPORTEAPLICADO" = Table.RenameColumns(#"Delete old IMPORTEAPLICADO",{{"IMPORTE.APLICADO", "IMPORTEAPLICADO"}}),
    #"Define new column [Importe_ha]" = Table.AddColumn(#"Rename new IMPORTEAPLICADO", "Importe_ha", each [IMPORTEAPLICADO]/[Superficie_ha]),
    #"Changed Type [Importe_ha] to ""number""" = Table.TransformColumnTypes(#"Define new column [Importe_ha]",{{"Importe_ha", type number}}),
    #"Duplicate Column [IMPORTEAPLICADO - Copy]" = Table.DuplicateColumn(#"Changed Type [Importe_ha] to ""number""", "IMPORTEAPLICADO", "IMPORTEAPLICADO - Copy"),
    #"Rename Column [Importe_global]" = Table.RenameColumns(#"Duplicate Column [IMPORTEAPLICADO - Copy]",{{"IMPORTEAPLICADO - Copy", "Importe_global"}}),
    #"Changed Type [Importe_ha] to ""number"" in es-ES" = Table.TransformColumnTypes(#"Rename Column [Importe_global]", {{"Importe_ha", type number}}, "es-ES"),
    #"Changed Type [Importe_ha] to ""number"" in es-ES 2" = Table.TransformColumnTypes(#"Changed Type [Importe_ha] to ""number"" in es-ES", {{"Importe_global", type number}}, "es-ES"),
    #"Duplicated Column" = Table.DuplicateColumn(#"Changed Type [Importe_ha] to ""number"" in es-ES 2", "NumeroParteProduccion", "NumeroParteProduccion - Copy"),
    #"Extracted Last Characters" = Table.TransformColumns(#"Duplicated Column", {{"NumeroParteProduccion - Copy", each Text.End(_, 6), type text}}),
    #"Rename Column [NumeroParteProduccion]" = Table.RenameColumns(#"Extracted Last Characters",{{"NumeroParteProduccion - Copy", "Numero"}}),
    #"Duplicate [NumeroParteProduccion]" = Table.DuplicateColumn(#"Rename Column [NumeroParteProduccion]", "NumeroParteProduccion", "NumeroParteProduccion - Copy"),
    #"Extract Text Before Delimiter [NumeroParteProduccion]" = Table.TransformColumns(#"Duplicate [NumeroParteProduccion]", {{"NumeroParteProduccion - Copy", each Text.BeforeDelimiter(_, " "), type text}}),
    #"Rename to Serie" = Table.RenameColumns(#"Extract Text Before Delimiter [NumeroParteProduccion]",{{"NumeroParteProduccion - Copy", "Serie"}}),
    #"Changed Type [Numero] to ""text""" = Table.TransformColumnTypes(#"Rename to Serie",{{"Numero", type text}}),
    #"Fill EMPTY values [SERIE]" = Table.ReplaceValue(#"Changed Type [Numero] to ""text""","","0",Replacer.ReplaceValue,{"Serie"}),
    #"Fill EMPTY values [NUMERO]" = Table.ReplaceValue(#"Fill EMPTY values [SERIE]","","0",Replacer.ReplaceValue,{"Numero"}),
    #"Fill NULL values [Numero]" = Table.ReplaceValue(#"Fill EMPTY values [NUMERO]",null,"0",Replacer.ReplaceValue,{"Numero"}),
    #"Changed Type [Numero]" = Table.TransformColumnTypes(#"Fill NULL values [Numero]",{{"Numero", Int64.Type}}),
    #"Select final columns" = Table.SelectColumns(#"Changed Type [Numero]",{"TIPOGRUPO", "SUBGRUPO", "PARTIDA", "FECHAIMPUTACION", "CONCEPTODOCUMENTO", "CONCEPTOAPUNTE", "NUMEROALBARANVENTA", "ARTICULOALBARANVENTA", "CANTIDADALBARANVENTA", "ID_AnaliticaImputacion", "EjercicioAnalitico", "Id_AreaResponsabilidad", "EjercicioContable", "CLI430_idParte", "CONCEPTO ANALITICO", "ID.AREA", "Superficie_ha", "PROYECTO", "Campaña", "IMPORTEAPLICADO", "Importe_ha", "Importe_global", "Numero", "Serie"}),
    #"Delete column [CONCEPTO]" = Table.RemoveColumns(#"Select final columns",{"CONCEPTO ANALITICO"}),
    #"Rename Column [id_PARTE]" = Table.RenameColumns(#"Delete column [CONCEPTO]",{{"CLI430_idParte", "id_PARTE"}}),
    #"Changed Type [id_PARTE]" = Table.TransformColumnTypes(#"Rename Column [id_PARTE]",{{"id_PARTE", Int64.Type}}),
    #"Inserted Text After Delimiter" = Table.AddColumn(#"Changed Type [id_PARTE]", "Text After Delimiter", each Text.AfterDelimiter([PARTIDA], "- "), type text),
    #"Removed Columns" = Table.RemoveColumns(#"Inserted Text After Delimiter",{"PARTIDA"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns",{{"Text After Delimiter", "PARTIDA"}}),
    #"Columnas reordenadas" = Table.ReorderColumns(#"Renamed Columns",{"PROYECTO", "TIPOGRUPO", "SUBGRUPO", "PARTIDA", "FECHAIMPUTACION", "IMPORTEAPLICADO", "CONCEPTODOCUMENTO", "CONCEPTOAPUNTE", "NUMEROALBARANVENTA", "ARTICULOALBARANVENTA", "ID_AnaliticaImputacion", "EjercicioAnalitico", "EjercicioContable", "id_PARTE", "ID.AREA", "Campaña", "Superficie_ha", "Importe_ha", "Importe_global", "Numero", "Serie"}),
    #"Columnas con nombre cambiado3" = Table.RenameColumns(#"Columnas reordenadas",{{"id_PARTE", "ID.PARTE"}}),
    #"Texto insertado antes del delimitador1" = Table.AddColumn(#"Columnas con nombre cambiado3", "ID.PROYECTO", each Text.BeforeDelimiter([PROYECTO], " -"), type text),
    #"Primeros caracteres insertados" = Table.AddColumn(#"Texto insertado antes del delimitador1", "Primeros caracteres", each Text.Start([ID.PROYECTO], 1), type text),
    #"Columnas con nombre cambiado4" = Table.RenameColumns(#"Primeros caracteres insertados",{{"Primeros caracteres", "ID.FINCA"}}),
    #"Texto extraído después del delimitador" = Table.TransformColumns(#"Columnas con nombre cambiado4", {{"SUBGRUPO", each Text.AfterDelimiter(_, "- "), type text}}),
    #"Columna duplicada20" = Table.DuplicateColumn(#"Texto extraído después del delimitador", "ID.PROYECTO", "ID.PROYECTO - Copia"),
```
The provided code implements a series of transformations to manipulate a table in Power Query, enhancing its structure and preparing it for further analysis. Initially, it renames columns for clarity, changing **"PROYECTO"** to **"PROYECTO_1"** and **"Campaña"** to **"Campaña_1"**. Following this, new columns are introduced: **"PROYECTO"** is conditionally filled based on the existence of values in **"PROYECTO_1"** or defaults to **"Proyecto_0"** if null, and a similar operation is performed for **"Campaña"**. The subsequent steps remove unnecessary columns that are no longer needed, such as **"Proyecto_0"** and the previously renamed columns, optimizing the dataset's size. Date formatting is adjusted for the **"FECHAIMPUTACION"** column, and numeric conversions are applied to relevant columns like **"Superficie_ha"** and **"IMPORTEAPLICADO"**. Additionally, a new column, **"IMPORTE.señal"**, is created to signal whether the type group is an expense or income, multiplying this value by **"IMPORTEAPLICADO"** to create a new adjusted column. The original **"IMPORTEAPLICADO"** is then removed, and its adjusted version is renamed accordingly. New calculations are introduced to derive **"Importe_ha"** as a per-area value, alongside duplicating and renaming columns to retain original data and extract specific segments of information. The process also includes cleaning and formatting operations, such as replacing empty and null values with zero, ensuring data integrity before selecting the final set of columns for analysis. Finally, the code reorders columns and performs additional renaming to finalize the structure, creating a well-organized dataset ready for reporting or further data processing tasks.
```vs
// Define AreaResponsabilidad
    #"Crear AREA RESPONSABILIDAD0" = Table.TransformColumns(#"Columna duplicada20", {{"ID.PROYECTO - Copia", each Text.Middle(_, 1, 3), type text}}),
    #"Columnas con nombre cambiado80" = Table.RenameColumns(#"Crear AREA RESPONSABILIDAD0",{{"ID.PROYECTO - Copia", "AreaResponsabilidad"}}),
    #"Tipo cambiado10" = Table.TransformColumnTypes(#"Columnas con nombre cambiado80",{{"AreaResponsabilidad", Int64.Type}}),
    #"Columna condicional agregada20" = Table.AddColumn(#"Tipo cambiado10", "Personalizado", each if [AreaResponsabilidad] = 400 then "ACREEDORES Y DEUDORES" else if [AreaResponsabilidad] = 401 then "VARIOS" else if [AreaResponsabilidad] = 410 then "EXPLOTACIONES" else if [AreaResponsabilidad] = 460 then "PERSONAL" else if [AreaResponsabilidad] = 480 then "CUENTAS DIVERSAS" else if [AreaResponsabilidad] = 482 then "INVERSIONES EN CURSO" else if [AreaResponsabilidad] = 901 then "CULTIVO AÑO ANTERIOR" else if [AreaResponsabilidad] = 905 then "PLANTACIONES AÑO ANTERIOR" else if [AreaResponsabilidad] = 906 then "GANADO AÑO ANTERIOR" else if [AreaResponsabilidad] = 921 then "CULTIVOS EXTENSIVOS" else if [AreaResponsabilidad] = 922 then "HORTICOLAS FRESCAS" else if [AreaResponsabilidad] = 923 then "HORTICOLAS INDUSTRIA" else if [AreaResponsabilidad] = 924 then "ECO" else if [AreaResponsabilidad] = 925 then "PLANTACIONES" else if [AreaResponsabilidad] = 926 then "GANADO" else if [AreaResponsabilidad] = 929 then "CULTIVOS DIVERSOS" else if [AreaResponsabilidad] = 940 then "AGUA RIEGO" else if [AreaResponsabilidad] = 941 then "OTRAS CUENTAS DE GASTOS" else if [AreaResponsabilidad] = 942 then "MARCA CLR" else if [AreaResponsabilidad] = 943 then "VISITAS TURISTICAS LA REINA" else if [AreaResponsabilidad] = 944 then "SUBVENCIONES" else if [AreaResponsabilidad] = 950 then "TRACTORES" else if [AreaResponsabilidad] = 951 then "TRACTORES ALQUILER" else if [AreaResponsabilidad] = 957 then "APEROS" else if [AreaResponsabilidad] = 958 then "MAQUINARIA CUENTA DE EXPLOTACION" else if [AreaResponsabilidad] = 959 then "TRACTORES CUENTA EXPLOTACION" else if [AreaResponsabilidad] = 960 then "GASTOS GENERALES" else if [AreaResponsabilidad] = 980 then "GASTOS GENERALES" else "na"),
    #"Columnas con nombre cambiado5" = Table.RenameColumns(#"Columna condicional agregada20",{{"AreaResponsabilidad", "ID.AreaResponsabilidad"}, {"Personalizado", "AreaResponsabilidad"}}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Columnas con nombre cambiado5",{{"AreaResponsabilidad", type text}}),
    #"Filas filtradas1" = Table.SelectRows(#"Tipo cambiado", each ([AreaResponsabilidad] = "AGUA RIEGO" or [AreaResponsabilidad] = "CULTIVOS DIVERSOS" or [AreaResponsabilidad] = "CULTIVOS EXTENSIVOS" or [AreaResponsabilidad] = "ECO" or [AreaResponsabilidad] = "GANADO" or [AreaResponsabilidad] = "HORTICOLAS FRESCAS" or [AreaResponsabilidad] = "HORTICOLAS INDUSTRIA" or [AreaResponsabilidad] = "PLANTACIONES")),
    #"Primeros caracteres insertados1" = Table.AddColumn(#"Filas filtradas1", "Primeros caracteres", each Text.Start([Campaña], 2), type text),
    #"Columnas con nombre cambiado6" = Table.RenameColumns(#"Primeros caracteres insertados1",{{"Primeros caracteres", "CampañaOrder"}}),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Columnas con nombre cambiado6",{{"CampañaOrder", Int64.Type}}),
    #"Columnas con nombre cambiado7" = Table.RenameColumns(#"Tipo cambiado1",{{"CANTIDADALBARANVENTA", "Cantidad_PRODUCTO"}})
in
    #"Columnas con nombre cambiado7"
```
This chunk of code performs a series of transformations to enhance a dataset related to area responsibilities. Initially, it extracts a substring from the **"ID.PROYECTO** - Copia" column, retaining the first three characters, and renames this column to **"AreaResponsabilidad"**. The type of this new column is then changed to Int64, indicating it will hold integer values. Subsequently, a conditional column named "Personalizado" is added, which categorizes the area responsibility based on predefined numerical codes into meaningful labels such as **"CULTIVOS EXTENSIVOS"** and **"AGUA RIEGO"** providing clearer context for analysis.

The code then renames the column **"AreaResponsabilidad"** to **"ID.AreaResponsabilidad"** and reverts the name back to **"AreaResponsabilidad"** for clarity. The data type for this column is changed to text to accommodate the new string values. The table is filtered to retain only those rows where **"AreaResponsabilidad"** matches specific categories related to agricultural activities. A new column, **"CampañaOrder"**, is created by extracting the first two characters from the **"Campaña"** column, which is subsequently converted to Int64 for numerical sorting purposes. Finally, the column **"CANTIDADALBARANVENTA"** is renamed to **"Cantidad_PRODUCTO"** to standardize terminology across the dataset. This structured approach not only improves data readability but also sets the stage for effective analysis in subsequent operations.

### ERP Tables Satellite

**PARTES_ERP**
```vs
let
    Origen = Sql.Databases("WIN-19AB\ERP"),
    REPORTING = Origen{[Name="REPORTING"]}[Data],
    dbo_PartesProduccion = REPORTING{[Schema="dbo",Item="PartesProduccion"]}[Data],
    #"Importe reemplazado" = Table.ReplaceValue(dbo_PartesProduccion,null,0,Replacer.ReplaceValue,{"Importe Transaccional"}),
    #"Personalizada agregada1" = Table.AddColumn(#"Importe reemplazado", "IMPORTE", each [ImportePlusFunc]+[Importe Transaccional]),
    #"Crear ID.PARTEProduccion" = Table.RenameColumns(#"Personalizada agregada1",{{"Id_ParteProduccion", "ID.PARTEProduccion"}}),
    #"Filas agrupadas" = Table.Group(#"Crear ID.PARTEProduccion", {"ID.PARTEProduccion", "Nombre Actividad Producción", "Nombre Artículo", "Cantidad", "Código Unidad Medida", "Precio el Artículo", "Nombre Unidad Medida", "Identificador Cultivo", "Descripción Cultivo", "Nombre Familia Artículo", "CampanyaParteProduccionCodigo"}, {{"Importe", each List.Sum([IMPORTE]), type number}}),
    #"Filas filtradas" = Table.SelectRows(#"Filas agrupadas", each true),
    #"Valor reemplazado1" = Table.ReplaceValue(#"Filas filtradas",null,0,Replacer.ReplaceValue,{"Importe"}),
    #"Personalizada agregada" = Table.AddColumn(#"Valor reemplazado1", "Valor", each [Precio el Artículo]*[Cantidad]),
    #"Valor reemplazado" = Table.ReplaceValue(#"Personalizada agregada",null,"MANO DE OBRA",Replacer.ReplaceValue,{"Nombre Artículo"}),
    #"Columna condicional agregada" = Table.AddColumn(#"Valor reemplazado", "Importe_PARTE", each if [Importe] = 0 then [Valor] else [Importe]),
    #"Columnas quitadas" = Table.RemoveColumns(#"Columna condicional agregada",{"Importe", "Valor"}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Columnas quitadas",{{"ID.PARTEProduccion", "ID.PARTE"}}),
    #"Columna combinada insertada" = Table.AddColumn(#"Columnas con nombre cambiado", "ID.PARTE.ARTICULO", each Text.Combine({Text.From([ID.PARTE], "es-ES"), [Nombre Artículo]}, ":"), type text),
    #"Columna condicional agregada2" = Table.AddColumn(#"Columna combinada insertada", "Unidad", each if [Código Unidad Medida] = "LIT" then "L" else if [Nombre Unidad Medida] = "UND" then "Un." else if [Código Unidad Medida] = "UNI" then "Un." else if [Código Unidad Medida] = "KG" then "Kg" else [Código Unidad Medida]),
    #"Columnas quitadas2" = Table.RemoveColumns(#"Columna condicional agregada2",{"Código Unidad Medida"}),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Columnas quitadas2",{{"Unidad", "Código Unidad Medida"}}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Columnas con nombre cambiado1",{{"Importe_PARTE", type number}, {"Código Unidad Medida", type text}}),
    #"Columna multiplicada" = Table.TransformColumns(#"Tipo cambiado", {{"Importe_PARTE", each _ * -1, type number}}),
    #"Valor reemplazado2" = Table.ReplaceValue(#"Columna multiplicada","GENERAL","ABONO GENERAL",Replacer.ReplaceText,{"Nombre Familia Artículo"})
in
    #"Valor reemplazado2"
```
The previous code snippet performs a series of data transformations on a database table named **"PartesProduccion"** from the **"REPORTING"** schema of the **"WIN-19AB\ERP"** SQL database. Initially, it retrieves the data and replaces null values in the "Importe Transaccional" column with zero. A new column, "IMPORTE," is added, summing the values of "ImportePlusFunc" and "Importe Transaccional." The script renames the "Id_ParteProduccion" column to "ID.PARTEProduccion" and groups the rows by various identifiers, summing the "IMPORTE." It replaces null values in the grouped data, adds a "Valor" column calculated from the product of "Precio el Artículo" and "Cantidad," and introduces a conditional column, "Importe_PARTE," which sets the value based on the "Importe" column's value. After removing unnecessary columns, it renames "ID.PARTEProduccion" to "ID.PARTE" and creates a combined identifier column, "ID.PARTE.ARTICULO," using text concatenation. It also adds a conditional column to translate unit codes and changes the data types of certain columns. Finally, it multiplies the "Importe_PARTE" values by -1 and replaces specific text in the "Nombre Familia Artículo" column. The final output is the transformed table.

**Areas**
```vs
let
    Origen = Sql.Databases("WIN-19AB\ERP"),
    REPORTING = Origen{[Name="REPORTING"]}[Data],
    dbo_ProyectosCultivos = REPORTING{[Schema="dbo",Item="ProyectosCultivos"]}[Data],
    #"Consultas combinadas" = Table.NestedJoin(dbo_ProyectosCultivos, {"Nombre"}, CULTIVOS_ERP, {"Descripcion"}, "CULTIVOS", JoinKind.LeftOuter),
    #"Se expandió CULTIVOS" = Table.ExpandTableColumn(#"Consultas combinadas", "CULTIVOS", {"SuperficieCultivada"}, {"SuperficieCultivada"}),
    /*#"Filas filtradas" = Table.SelectRows(#"Se expandió CULTIVOS", each ([NombreProyectoAnalitico] = "ALMENDROS 2016" or [NombreProyectoAnalitico] = "ESPINACAS GELAGRI")),
    #"Columnas quitadas" = Table.RemoveColumns(#"Filas filtradas",{"FechaAplicacion", "CodigoAlmacen", "NombreAlmacen", "CodigoProveedor", "NombreProveedor", "CodigoCliente", "NombreCliente", "fechaPlantacion", "fechaFinCultivo", "idPRoyecto", "id", "Id1", "Version", "Id2", "Version1", "Id3", "Version2", "Version3", "Version4"}),*/
    #"Columnas quitadas" = Table.RemoveColumns(#"Se expandió CULTIVOS",{"FechaAplicacion", "CodigoAlmacen", "NombreAlmacen", "CodigoProveedor", "NombreProveedor", "CodigoCliente", "NombreCliente", "fechaPlantacion", "fechaFinCultivo", "idPRoyecto", "id", "Id1", "Version", "Id2", "Version1", "Id3", "Version2", "Version3", "Version4"}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Columnas quitadas",{{"SuperficieCultivada", "Superficie_ha"}}),
    #"Columna combinada insertada" = Table.AddColumn(#"Columnas con nombre cambiado", "PROYECTO", each Text.Combine({[NombreProyectoAnalitico], [CodigoProyectoAnalitico]}, " - "), type text),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Columna combinada insertada",{"PROYECTO"}),
    #"Columna combinada insertada1" = Table.AddColumn(#"Columnas quitadas1", "PROYECTO", each Text.Combine({[CodigoProyectoAnalitico], [NombreProyectoAnalitico]}, " - "), type text),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Columna combinada insertada1",{{"Codigo", Int64.Type}}),
    #"Consultas combinadas1" = Table.NestedJoin(#"Tipo cambiado", {"Codigo", "Nombre"}, PARTES_ERP, {"Identificador Cultivo", "Descripción Cultivo"}, "PARTES", JoinKind.LeftOuter),
    #"Se expandió PARTES" = Table.ExpandTableColumn(#"Consultas combinadas1", "PARTES", {"CampanyaParteProduccionCodigo"}, {"CampanyaParteProduccionCodigo"}),
    #"Columna condicional agregada" = Table.AddColumn(#"Se expandió PARTES", "Personalizado", each if [Nombre] = "Olivar seto 2015" then "L925000001 - EL POZO - OLIVOS SETO 2015 TOCINA" else if [Nombre] = "Olivar seto 2016" then "L925000003 - OLIVAR SETO TOCINA 2016" else if [Nombre] = "Olivar seto 2017" then "L925000004 - OLIVAR SETO TOCINA 2017" else [PROYECTO]),
    #"Columnas con nombre cambiado2" = Table.RenameColumns(#"Columna condicional agregada",{{"PROYECTO", "PROYECTO_OLD"}, {"Personalizado", "PROYECTO"}}),
    #"Filas agrupadas" = Table.Group(#"Columnas con nombre cambiado2", {"CampanyaParteProduccionCodigo", "PROYECTO"}, {{"Superficie_ha", each List.Max([Superficie_ha]), type nullable number}}),
    #"Columna dividida" = Table.TransformColumns(#"Filas agrupadas", {{"Superficie_ha", each _ / 10000, type number}}),
    #"Texto insertado antes del delimitador" = Table.AddColumn(#"Columna dividida", "ID.PROYECTO", each Text.BeforeDelimiter([PROYECTO], " -"), type text),
    #"Columna combinada insertada2" = Table.AddColumn(#"Texto insertado antes del delimitador", "ID.AREA", each Text.Combine({[ID.PROYECTO], [CampanyaParteProduccionCodigo]}, ":"), type text),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Columna combinada insertada2",{{"CampanyaParteProduccionCodigo", "Campaña"}}),
    #"Evitar Areas 0" = Table.SelectRows(#"Columnas con nombre cambiado1", each [Superficie_ha] > 1)
in
    #"Evitar Areas 0"
```
This 'M' script extracts and transforms data from the **"ProyectosCultivos"** table within the **"REPORTING"** schema of the **"WIN-19AB\ERP"** SQL database. Initially, it establishes a nested join with the "CULTIVOS_ERP" table based on the project name, expanding the "SuperficieCultivada" column. Several columns, including those irrelevant to the analysis, are removed for clarity. The "SuperficieCultivada" column is renamed to "Superficie_ha," and a new combined column, "PROYECTO," is created by concatenating "NombreProyectoAnalitico" and "CodigoProyectoAnalitico." The script further transforms the data types as necessary and performs a second nested join with the "PARTES_ERP" table to include the "CampanyaParteProduccionCodigo" column. A conditional column named "Personalizado" is added to assign specific project identifiers based on project names. The data is then grouped by "CampanyaParteProduccionCodigo" and "PROYECTO," summarizing "Superficie_ha" while converting its values from square meters to hectares. Additional columns are created to extract and combine project identifiers, and the final output filters out entries with "Superficie_ha" values less than or equal to 1, resulting in a refined dataset ready for analysis.


**Sociedades**
```vs
let
    Origen = Csv.Document(File.Contents("C:\Users\usuario6\Desktop\Power BI - FASE 2\Agro\Proyectos Analíticos.csv"),[Delimiter=";", Columns=11, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Tipo cambiado" = Table.TransformColumnTypes(Origen,{{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Tipo cambiado",{"Column4", "Column5"}),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Columnas quitadas", [PromoteAllScalars=true]),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Encabezados promovidos",{"", "_1", "_2", "_3", "_4"}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Columnas quitadas1",{{"Nombre", "Nombre.PROYECTO"}, {"Código", "ID.PROYECTO"}}),
    #"Valor reemplazado" = Table.ReplaceValue(#"Columnas con nombre cambiado","""0""","""0000000000""",Replacer.ReplaceText,{"ID.PROYECTO"}),
    #"Texto insertado después del delimitador" = Table.AddColumn(#"Valor reemplazado", "Texto después del delimitador", each Text.AfterDelimiter([Empresa Principal], "- "), type text),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Texto insertado después del delimitador",{{"Texto después del delimitador", "Sociedad"}})
in
    #"Columnas con nombre cambiado1"
```
This Power Query M script in Power BI is designed to load, clean, and transform data from a CSV file, preparing it for further analysis. The process begins by loading the CSV file named "Proyectos Analíticos.csv" from a specific directory. The `Csv.Document` function is employed to interpret the file, specifying a semicolon as the delimiter, identifying that the file contains 11 columns, and setting the encoding to `1252`, commonly used for Western European characters. No special quoting style is applied, ensuring that all content is processed as it appears in the CSV. Once the file is loaded, the script transforms the data types of all columns to text. The `Table.TransformColumnTypes` function is used for this task, ensuring that each column, from Column1 to Column11, is treated uniformly as text, which might be necessary if the original file contained a mixture of data types.

Following this, the script proceeds to remove two columns, specifically `Column4` and `Column5`, which are deemed unnecessary for the analysis. This step reduces the dataset to relevant information only, simplifying further processing. The table's first row, which contains the actual field names, is then promoted to serve as the header. This is done with the `Table.PromoteHeaders` function, which replaces the default generic column names (e.g., Column1, Column2) with meaningful headers extracted from the first row. Further cleaning is carried out by removing additional columns that contain either empty names or placeholder names (such as `_1`, `_2`). This step ensures the dataset remains streamlined and excludes any columns that do not contribute useful data. 

In the next phase, two key columns are renamed for clarity. The column originally named `"Nombre"` is renamed to `"Nombre.PROYECTO"`, indicating that it holds the name of the project. Similarly, the `"Código"` column is renamed to `"ID.PROYECTO"`, signifying that it contains the unique project identifier. These renaming operations are essential for making the dataset easier to interpret. The script then performs a value replacement within the `"ID.PROYECTO"` column. Using `Table.ReplaceValue`, any occurrence of the value `"0"` is replaced with `"0000000000"`, likely to ensure that all project IDs follow a standardized format with a fixed length, possibly to maintain consistency or meet formatting requirements for subsequent data handling. Following this, the script adds a new column to the dataset. The new column, initially named `"Texto después del delimitador"`, extracts text that appears after a specific delimiter (`"- "`) from the `"Empresa Principal"` column. This is done using the `Text.AfterDelimiter` function, which pulls the part of the text that follows the hyphen and space, likely isolating a specific section of the company name or another related identifier.

Finally, this newly added column is renamed to `"Sociedad"`, giving it a clearer, contextually relevant title. This transformation concludes the script, which outputs a table named `"Columnas con nombre cambiado1"`. By this point, the data has been fully transformed: unnecessary columns have been removed, headers have been promoted, key columns have been renamed for clarity, and a new column has been added to enhance the dataset. The resulting table is now well-structured and prepared for analysis in Power BI.

**Fincas**
```vs
let
    Origen = Csv.Document(File.Contents("C:\Users\usuario6\Desktop\Power BI - FASE 2\Agro\Fincas.csv"),[Delimiter=";", Columns=2, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Tipo cambiado" = Table.TransformColumnTypes(Origen,{{"Column1", type text}, {"Column2", type text}}),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Tipo cambiado", [PromoteAllScalars=true]),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Letter", type text}, {"Finca", type text}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo cambiado1",{{"Letter", "ID.FINCA"}})
in
    #"Columnas con nombre cambiado"
```
This M script loads a CSV file named "Fincas.csv" containing two columns, changes their data types to text, and promotes the first row to headers. It then ensures the column types remain text and renames the column "Letter" to "ID.FINCA" for clarity, preparing the data for analysis in Power BI. The process involves cleaning and structuring the dataset to make it more readable and organized.

**Calendario**
```vs
let
    Origen = fCalendario(#date(2005, 1, 1), #date(2030, 12, 31)),
    #"Fecha insertada" = Table.AddColumn(Origen, "Fecha.1", each DateTime.Date([Fecha]), type date),
    #"Columnas quitadas" = Table.RemoveColumns(#"Fecha insertada",{"Fecha"}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Columnas quitadas",{{"Fecha.1", "FECHA"}})
in
    #"Columnas con nombre cambiado"
```
In this context, we define a calendar table, also referred to as a date dimension table, as a crucial component of a star schema model in Power BI for several compelling reasons. Firstly, it enhances time-based analysis by enabling the implementation of time intelligence calculations, such as year-to-date, month-to-date, and quarter-over-quarter assessments. This capability allows us to perform standardized analyses across various time periods—be it days, months, quarters, or years—spanning multiple fact tables. Secondly, a calendar table significantly boosts performance by streamlining queries; it acts as a central element for establishing relationships with diverse fact tables, thereby optimizing data retrieval and simplifying the overall structure.

Moreover, a calendar table offers a wealth of date attributes, including week numbers, holidays, fiscal periods, and custom categorizations (e.g., weekdays versus weekends). This richness enhances analytical capabilities while avoiding data duplication across multiple tables, ensuring consistency and minimizing discrepancies. It also improves filtering and slicing functionalities, enabling users to intuitively filter reports and dashboards by specific dates or time frames, which ultimately leads to more insightful analyses. Finally, a calendar table promotes compatibility with various data sources by bridging datasets that may utilize different date formats or systems, facilitating a more uniform approach to analysis. The calendar table functions as a key elementary table, allowing for one-to-many relationships that support time-dependent DAX calculations. This has proven essential in a model of this kind, which incorporates time-dependent analysis.


**PARTES_keys**
```vs
let
    Origen = PARTES_ERP,
    #"Filas agrupadas" = Table.Group(Origen, {"ID.PARTE", "ID.PARTE.ARTICULO"}, {{"recuento", each Table.RowCount(_), Int64.Type}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Filas agrupadas",{"recuento", "ID.PARTE.ARTICULO"}),
    #"Filas agrupadas1" = Table.Group(#"Columnas quitadas", {"ID.PARTE"}, {{"Recuento", each Table.RowCount(_), Int64.Type}}),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Filas agrupadas1",{"Recuento"})
in
    #"Columnas quitadas1"
```
This section establishes a single key identifier table, referred to as an elementary key table, which supports one-to-many relationships for selecting information related to PARTES de campo from the PARTES table in the ERP system.

**Cultivos**
```vs
let
    Origen = Areas,
    #"Columnas quitadas" = Table.RemoveColumns(Origen,{"Campaña", "Superficie_ha", "ID.AREA"}),
    #"Texto insertado después del delimitador" = Table.AddColumn(#"Columnas quitadas", "Texto después del delimitador", each Text.AfterDelimiter([PROYECTO], "- "), type text),
    #"Texto extraído antes del delimitador" = Table.TransformColumns(#"Texto insertado después del delimitador", {{"Texto después del delimitador", each Text.BeforeDelimiter(_, " "), type text}}),
    #"Valor reemplazado" = Table.ReplaceValue(#"Texto extraído antes del delimitador","CAMPITO","CAMPITO LIMAGRAIN",Replacer.ReplaceText,{"Texto después del delimitador"}),
    #"Valor reemplazado1" = Table.ReplaceValue(#"Valor reemplazado","EL","OLIVAR",Replacer.ReplaceText,{"Texto después del delimitador"}),
    #"Columna condicional agregada" = Table.AddColumn(#"Valor reemplazado1", "CULTIVO", each if [ID.PROYECTO] = "J921000100" then "OLIVAR ECOLOGICO" else if [ID.PROYECTO] = "F410000012" then "OLIVAR" else if [ID.PROYECTO] = "F410000011" then "OLIVAR" else if [ID.PROYECTO] = "F410000010" then "ALMENDROS" else if [ID.PROYECTO] = "F923000002" then "GRELOS" else if [ID.PROYECTO] = "F410000014" then "TRIGO" else if [ID.PROYECTO] = "F410000013" then "OLIVAR" else if [ID.PROYECTO] = "F410000006" then "FUENREAL" else [Texto después del delimitador]),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Columna condicional agregada",{"Texto después del delimitador"}),
    #"Texto insertado después del delimitador1" = Table.AddColumn(#"Columnas quitadas1", "Texto después del delimitador", each Text.AfterDelimiter([PROYECTO], "- "), type text),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Texto insertado después del delimitador1",{{"Texto después del delimitador", "PROYECTO_Riego_keys"}})
in
    #"Columnas con nombre cambiado"
```
This script processes a dataset represented by the variable "Areas". The initial step removes unnecessary columns, specifically "Campaña", "Superficie_ha" and "ID.AREA" which helps streamline the dataset to focus on relevant information. Subsequently, the script adds a new column that extracts text appearing after the delimiter "- " from the "PROYECTO" column, creating a column named "Texto después del delimitador". This operation is useful for isolating specific segments of the project names. The script then refines this new column by transforming its values to keep only the text before the next space, further clarifying the content.

Following this, the script employs a series of value replacements to standardize the text. The term "CAMPITO" is replaced with "CAMPITO LIMAGRAIN", ensuring consistency, while "EL" is replaced with "OLIVAR," aligning with specific naming conventions. Next, a conditional column is added, labeled "CULTIVO". This column assigns specific crop types based on the "ID.PROYECTO" values. For instance, projects with the ID "J921000100" are classified as "OLIVAR ECOLOGICO," while others are categorized into "OLIVAR," "ALMENDROS," "GRELOS," or "TRIGO." This step enhances the dataset by providing meaningful context about each crop related project.

Afterwards, the script removes the "Texto después del delimitador" column, as its purpose has been served. It then repeats the process of extracting text after the delimiter from the "PROYECTO" column to create another column with the same name, essentially reinstating that aspect of the data for further use. Finally, the script renames this new column to "PROYECTO_Riego_keys" clarifying its intended use related to irrigation keys. The script concludes by outputting this well-structured dataset, which has undergone several transformations to enhance its clarity and usability for analysis.

### Aguas-Riego

Aguas-Riego is a comprehensive irrigation monitoring and control approach that has been implemented. The Aguas-Riego system represents a sophisticated monitoring and control approach that utilizes data collection via Google Forms to generate a well-parameterized Google Sheets document. This document is tailored according to specific variables, including the irrigated parcel, the installed irrigation system, the crop type, and water usage. For each parcel, we categorize the irrigation system based on the methods employed—pivot versus drip irrigation. In the case of pivot irrigation, we calculate the flow as a function of the pivot's speed of advancement and the area covered. Conversely, for drip irrigation, we consider the flow rate of the installed emitters and the pumping characteristics to determine the flow applied per crop per parcel for each irrigation event. The system mandates manual data entry for essential parameters, such as initial and final irrigation times per event. For central pivots, we also monitor the pivot's speed, allowing us to adjust the application flow linearly. Each time an irrigation event occurs on the farm, the responsible individual is tasked with recording the relevant data in the Google Forms, which automatically updates the Excel sheet.

Within the scope of this research project, we have developed a script designed to correct data, execute control measures, and automatically compute the total water applied per crop per parcel segment. This information is subsequently linked to analytical projects, enabling the projection of costs associated with irrigation practices. These analytical projects encompass all costs incurred throughout the season. At the season's end, the total budget for irrigation management and water use is divided by crop or analytical project, adhering to the following rules:

- Fixed costs are allocated based on the surface area irrigated by each analytical project.
- Variable costs are distributed according to a weighting factor determined by the amount of water used, as recorded in the Aguas-Riego system.
- Some other costs are also allocated based on the quantity of irrigation events attributed to each crop, employing count functions applied to the registries.

Until 2023, this allocation process was carried out manually by the farm management team at the end of the irrigation season using Excel sheets, which was time-consuming and retrospective. My research has provided the company with an automated solution for real-time updates. Over the past few months, we have conducted a thorough analysis and investigation of this issue, reviewing an M code solution implemented in Power Query for data extraction from Google Sheets. We have made substantial progress in correcting the errors identified in the initial scripts and Google Sheets tables concerning the allocation of irrigation water.

We have studied the allocation criteria employed in farms such as La Reina and Villaseca. Our findings confirm that our model exhibits differences of approximately 5-6% compared to the manual data processing and allocation undertaken by the Aguas-Riego system. Our solution not only enhances efficiency and reduces resource demands but also provides real-time year-to-date splits, eliminating the need to wait until the end of the irrigation season to initiate the analytical procedures for assessing water use from both technical and economic perspectives. This is critical from a management standpoint, as it facilitates pivoting interventions and informed water management decisions. We have verified that the allocation is primarily based on two criteria: by cubic meter (m³) used per analytical project and by surface irrigated ha. Previously, our first solution for the allocation conducted in Power BI relied solely on cubic meters, leading to significant discrepancies. However, we have reformulated the code and corrected various details, resulting in minimal differences—approximately 5%. This entire procedure is iterated across seven tables, and below we describe in detail the significance of each table and the computational procedures implemented with M to address this critical need.

**DATOS-RIEGO**
```vs
let
    Origen = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1u7LYecQ7W-ffxf0UpujMYrShQ5kBqXH9RPNlwyzN0jk/edit?usp=sharing"),
    DATOS_Table = Origen{[name="DATOS",ItemKind="Table"]}[Data],
    #"Columnas quitadas" = Table.RemoveColumns(DATOS_Table,{"Column1"}),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Columnas quitadas", [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"ID", Int64.Type}, {"PARCELA", type text}, {"CULTIVO", type text}, {"CULTIVO/PARCELA", type text}, {"HAS", type number}, {"TIPO", type text}, {"COEFICIENTE(L/M2)", type number}, {"PROYECTO ANALÍTICO", type text}}),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Tipo cambiado",{"PROYECTO ANALÍTICO"}),
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas quitadas1", "PROYECTO", each if [CULTIVO] = "ALMENDROS PIRINEO 2016" then "F925000006 - ALMENDROS 2016" else if [CULTIVO] = "ESPINACAS" then "F923000001 - ESPINACAS GELAGRI" else if [#"CULTIVO/PARCELA"] = "12-AJOS-P10T_CORDOBA" then "F922000001 - AJOS CHINOS" else if [#"CULTIVO/PARCELA"] = "19-GUISANTES-P11T_ALMODOVAR" then "F923000003 - GUISANTES" else if [#"CULTIVO/PARCELA"] = "22-GUISANTES-PTUMBAITO_ALMODOVAR" then "F923000014 - GUISANTES SPS" else if [#"CULTIVO/PARCELA"] = "14-GUISANTES-P13T_ALMODOVAR_NORTE" then "F923000014 - GUISANTES SPS" else if [#"CULTIVO/PARCELA"] = "17-GUISANTES-P13T_ALMODOVAR_SUR" then "F923000014 - GUISANTES SPS" else if [#"CULTIVO/PARCELA"] = "20-TRIGO LIMAGRAIN-P11T_CORDOBA" then "F941000101 - CAMPITO LIMAGRAIN" else if [#"CULTIVO/PARCELA"] = "21-TRIGO DURO-PTUMBAITO_CORDOBA" then "F921000001 - TRIGO DURO" else if [#"CULTIVO/PARCELA"] = "13-ALMENDROS MAJOLETO 2020-ALMENDROS MAJOLETO 2020" then "F925000020 - ALMENDROS BELONA" else if [#"CULTIVO/PARCELA"] = "23-ALMENDROS SECANILLOS 2019-ALMENDROS SECANILLOS 2019" then "F925000010 - ALMENDROS 2019 SECANILLOS" else if [#"CULTIVO/PARCELA"] = "24-ALMENDROS SETO P.4-5 2019-ALMENDROS SETO P.4-5 2019" then "F925000011 - ALMENDROS 2019 SETO" else if [#"CULTIVO/PARCELA"] = "25-ALMENDROS PIRINEO 2014-ALMENDROS PIRINEO 2014" then "F925000001 - ALMENDROS 2014" else if [#"CULTIVO/PARCELA"] = "26-ALMENDROS PIRINEO 2016-ALMENDROS PIRINEO 2016" then "F925000006 - ALMENDROS 2016" else if [#"CULTIVO/PARCELA"] = "27-ALMENDROS PIRINEO NUEVO19-ALMENDROS PIRINEO NUEVO19" then "F925000018 - ALMENDROS 2020 CARACOL (FASE I) Y PIRINEO NUEVO" else if [#"CULTIVO/PARCELA"] = "30-ALMENDROS CARACOL 2020-ALMENDROS CARACOL 2020" then "F925000018 - ALMENDROS 2020 CARACOL (FASE I) Y PIRINEO NUEVO" else if [#"CULTIVO/PARCELA"] = "34-ALMENDROS CARACOL 2021-ALMENDROS CARACOL 2021" then "F925000019 - ALMENDROS 2021 CARACOL (FASE II)" else if [#"CULTIVO/PARCELA"] = "41-ALMENDROS LA BARCA 2014-ALMENDROS LA BARCA 2014" then "F925000001 - ALMENDROS 2014" else if [#"CULTIVO/PARCELA"] = "6-ALMENDROS ZAHURDAS 2018-ALMENDROS ZAHURDAS 2018" then "F925000020 - ALMENDROS BELONA" else if [#"CULTIVO/PARCELA"] = "7-PISTACHOS ZAHURDAS 2018-PISTACHOS ZAHURDAS 2018" then "F925000014 - PISTACHOS 2017 ZAHURDA" else if [#"CULTIVO/PARCELA"] = "9-PISTACHOS P.23-24 2020-PISTACHOS P.23-24 2020" then "F925000015 - PISTACHOS 2020 MEANDRO (P23-24)" else if [#"CULTIVO/PARCELA"] = "10-OLIVAR ECO SOTO-OLIVAR ECO SOTO" then "F925000017 - OLIVAR ECOLOGICO" else if [#"CULTIVO/PARCELA"] = "18-TRIGO DURO-P13T_CORDOBA_SUR" then "F921000001 - TRIGO DURO" else [CULTIVO])
in
    #"Columna condicional agregada"
```
This M code snippet retrieves data from a specified Google Sheets document. It begins by accessing the contents of the Google Sheets file using the GoogleSheets.Contents function, pointing to a shared URL. The variable "Origen" stores this data. From this dataset, it extracts the table named "DATOS" by indexing into the Origen variable, assigning the resulting data to "DATOS_Table". In the following step, the script removes the column labeled "Column1" from "DATOS_Table," which is likely unnecessary for subsequent analysis. Once this column is eliminated, the headers of the remaining data are promoted to the first row of the dataset through the Table.PromoteHeaders function, ensuring that each column is correctly identified by its relevant title.

Next, the script transforms the column types to match their intended formats. It specifies that the "ID" column should be of type Int64, while other columns like "PARCELA", "CULTIVO", "CULTIVO/PARCELA", "HAS", "TIPO", and "COEFICIENTE(L/M2)" are designated as either text or number types. This step is crucial for ensuring that data is interpreted correctly in later operations.

After the type transformation, the code removes the "PROYECTO ANALÍTICO" column, as it may not be relevant to the analysis being performed. Subsequently, the script adds a new column named "PROYECTO". This column is populated using a conditional expression, where various project identifiers are assigned based on specific criteria found in the "CULTIVO" and "CULTIVO/PARCELA" columns. Each condition checks the values in these columns, allowing for a systematic assignment of project labels for a wide range of crops such as "ALMENDROS", "PISTACHOS", and "OLIVAR ECOLOGICO". The logic for populating the "PROYECTO" column relies heavily on a series of if statements, enabling the script to return the correct project identifier based on the specific crop or parcel type listed in the respective columns. If none of the conditions are met, the original value from the "CULTIVO" column is retained. This flexibility enhances the usability of the dataset, providing context that is critical for further analysis.

The final output of the script is the dataset with the newly added "PROYECTO" column, which enriches the data for subsequent analysis or reporting tasks. Overall, this code snippet effectively transforms and organizes the data from Google Sheets, making it more accessible for agricultural project assessments.

**AGUAS-RIEGO**
```vs
let
    Origen = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1u7LYecQ7W-ffxf0UpujMYrShQ5kBqXH9RPNlwyzN0jk/edit?usp=sharing"),
    #"Respuestas de formulario 1_Table" = Origen{[name="Respuestas de formulario 1",ItemKind="Table"]}[Data],
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Respuestas de formulario 1_Table", [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Marca temporal", type datetime}, {"FECHA", type date}, {"CULTIVO/PARCELA", type text}, {"PASES", Int64.Type}, {"PORCENTAJE%", type number}, {"HORAS", type any}, {"OBSERVACIONES", type any}, {"COEFICIENTE", type number}, {"HAS", type number}, {"TIPO", type text}, {"PASES 100%", type number}, {"M3", type number}, {"M3/HA", type number}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo cambiado",{{"PORCENTAJE%", "PORCENTAJE"}}),
    #"Filas filtradas1" = Table.SelectRows(#"Columnas con nombre cambiado", each ([Marca temporal] <> null)),
    #"Columnas quitadas" = Table.RemoveColumns(#"Filas filtradas1",{"Marca temporal", "COEFICIENTE", "HAS", "TIPO", "PASES 100%", "M3", "M3/HA"}),
    #"Consultas combinadas" = Table.NestedJoin(#"Columnas quitadas", {"CULTIVO/PARCELA"}, #"DATOS-RIEGO", {"CULTIVO/PARCELA"}, "DATOS", JoinKind.LeftOuter),
    #"Se expandió DATOS" = Table.ExpandTableColumn(#"Consultas combinadas", "DATOS", {"HAS", "TIPO", "COEFICIENTE(L/M2)", "PROYECTO"}, {"HAS", "TIPO", "COEFICIENTE(L/M2)", "PROYECTO"}),
    #"Filas ordenadas" = Table.Sort(#"Se expandió DATOS",{{"FECHA", Order.Ascending}}),
    #"Personalizada agregada" = Table.AddColumn(#"Filas ordenadas", "PASES_100", each [PASES]*(1/[PORCENTAJE])),
    #"Personalizada agregada1" = Table.AddColumn(#"Personalizada agregada", "M3", each if [TIPO]="PIVOT" then [PASES_100]*[#"COEFICIENTE(L/M2)"]*[HAS]*10 else [HORAS]*[#"COEFICIENTE(L/M2)"]*[HAS]*10),
    #"Personalizada agregada2" = Table.AddColumn(#"Personalizada agregada1", "M3/HA", each [M3]/[HAS]),
    #"Año insertado" = Table.AddColumn(#"Personalizada agregada2", "Año", each Date.Year([FECHA]), Int64.Type),
    #"Filas filtradas2" = Table.SelectRows(#"Año insertado", each ([Año] = 2023)),
    #"Consultas combinadas1" = Table.NestedJoin(#"Filas filtradas2", {"Año"}, #"SALDO_RIEGO La Reina", {"Año"}, "SALDO_RIEGO", JoinKind.LeftOuter),
    #"Se expandió SALDO_RIEGO" = Table.ExpandTableColumn(#"Consultas combinadas1", "SALDO_RIEGO", {"TIPO.GASTO", "SALDO"}, {"TIPO.GASTO", "SALDO"}),
    #"Filas agrupadas" = Table.Group(#"Se expandió SALDO_RIEGO", {"PROYECTO", "TIPO.GASTO"}, {{"M3", each List.Sum([M3]), type number}, {"SALDO", each List.Median([SALDO]), type nullable number}}),
    #"Consultas combinadas2" = Table.NestedJoin(#"Filas agrupadas", {"SALDO"}, Riego_total, {"SALDO"}, "Riego_total", JoinKind.LeftOuter),
    #"Se expandió Riego_total" = Table.ExpandTableColumn(#"Consultas combinadas2", "Riego_total", {"M3"}, {"Riego_total.M3"}),
    #"Porcentaje insertado de" = Table.AddColumn(#"Se expandió Riego_total", "Porcentaje de", each [M3] / [Riego_total.M3] * 100, type number),
    #"División insertada" = Table.AddColumn(#"Porcentaje insertado de", "División", each [Porcentaje de] / 100, type number),
    #"Columnas quitadas1" = Table.RemoveColumns(#"División insertada",{"Porcentaje de"}),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Columnas quitadas1",{{"División", "Peso_relativo"}}),
    #"Personalizada agregada3" = Table.AddColumn(#"Columnas con nombre cambiado1", "Importe", each [SALDO]*[Peso_relativo]),
    #"Multiplicación insertada" = Table.AddColumn(#"Personalizada agregada3", "Multiplicación", each [Importe] * -1, type number),
    #"Columnas quitadas2" = Table.RemoveColumns(#"Multiplicación insertada",{"Importe"}),
    #"Columnas con nombre cambiado2" = Table.RenameColumns(#"Columnas quitadas2",{{"Multiplicación", "Importe_global"}}),
    /* #"Filas filtradas" = Table.SelectRows(#"Columnas con nombre cambiado2", each ([PROYECTO] = "F923000001 - ESPINACAS GELAGRI")), */
    #"Personalizada agregada4" = Table.AddColumn(#"Columnas con nombre cambiado2", "PARTIDA", each "RIEGOS"),
    #"Personalizada agregada5" = Table.AddColumn(#"Personalizada agregada4", "Campaña", each "22/23"),
    #"Personalizada agregada6" = Table.AddColumn(#"Personalizada agregada5", "Nombre Actividad Producción", each "RIEGO"),
    #"Personalizada agregada7" = Table.AddColumn(#"Personalizada agregada6", "Nombre Artículo", each "Riego"),
    #"Consultas combinadas3" = Table.NestedJoin(#"Personalizada agregada7", {"PROYECTO"}, Riego_areas, {"PROYECTO"}, "Riego_areas", JoinKind.LeftOuter),
    #"Se expandió Riego_areas" = Table.ExpandTableColumn(#"Consultas combinadas3", "Riego_areas", {"Superficie_ha"}, {"Superficie_ha"}),
    #"Personalizada agregada8" = Table.AddColumn(#"Se expandió Riego_areas", "TIPOGRUPO", each "Gastos"),
    #"Personalizada agregada9" = Table.AddColumn(#"Personalizada agregada8", "Importe_ha", each [Importe_global]/[Superficie_ha]),
    #"Columnas con nombre cambiado3" = Table.RenameColumns(#"Personalizada agregada9",{{"M3", "Cantidad_global"}}),
    #"Personalizada agregada10" = Table.AddColumn(#"Columnas con nombre cambiado3", "Cantidad_ha", each [Cantidad_global]/[Superficie_ha]),
    #"Columnas quitadas3" = Table.RemoveColumns(#"Personalizada agregada10",{"SALDO", "Riego_total.M3", "Peso_relativo"}),
    #"Personalizada agregada11" = Table.AddColumn(#"Columnas quitadas3", "Nombre Unidad Medida", each "M3"),
    #"Columnas quitadas4" = Table.RemoveColumns(#"Personalizada agregada11",{"Nombre Artículo"}),
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas quitadas4", "Nombre Artículo", each if [TIPO.GASTO] = "Variable" then "Riego - Gastos variables" else "Riego - Gastos fijos"),
    #"Columnas quitadas5" = Table.RemoveColumns(#"Columna condicional agregada",{"TIPO.GASTO"}),
    #"Filas filtradas" = Table.SelectRows(#"Columnas quitadas5", each ([PROYECTO] <> null and [PROYECTO] <> "-"))
in
    #"Filas filtradas"
```

With this M code, we aim to process and transform data from a Google Sheets document. It begins by importing data from the sheet, specifically selecting the table titled "Respuestas de formulario 1", which contains all the customized data from the form filled in by the person responsible for irrigation. The headers are promoted, and the data types for various columns are defined, such as dates, numbers, and text. It then filters out rows with null values in the "Marca temporal" column and removes unnecessary columns, retaining only the relevant data for the analysis.

Next, the code performs a join operation, merging the filtered data with another dataset called "DATOS-RIEGO", based on the "CULTIVO/PARCELA" column. After expanding the merged columns, the rows are sorted by date. New calculated columns are added to compute values such as adjusted passes ("PASES_100"), water volume ("M3"), and water volume per hectare ("M3/HA"). "PASES_100" defines the relative speed of the pivot in percentage terms compared to its maximum designed speed, requiring proper model calibration. The data is filtered to include only records from a specific year, as the allocation of Aguas-Riego data is season-specific and should always be calculated on a year-to-date (YTD) basis. At the start of a new year, the metrics reset and need to restart from zero.

A second join merges the data with another dataset, "SALDO_RIEGO La Reina", based on the "Año" column. After expanding these columns, the data is grouped by "PROYECTO" and "TIPO.GASTO", summing the water volume and calculating the median of the "SALDO" column. The code then joins this grouped data with a "Riego_total" dataset and adds a column that calculates the relative percentage of water use based on the total water ("M3"). This percentage is transformed into a relative weight for further calculations. The "SALDO_RIEGO" data is critical, as it includes the total irrigation costs, which are split by analytical projects and dates. Here, we segmented the data according to "TIPO.GASTO" (variable vs. fixed irrigation costs), as they follow different allocation criteria: variable costs are based on the water volume applied or irrigation events, while fixed costs are based on the irrigated surface area per project.

Several additional columns are computed, including a global amount ("Importe_global") and water volume per hectare. The code further refines the dataset by adding labels and project details such as "PARTIDA," "Campaña," and "Nombre Actividad Producción." "PARTIDA" is a key component of the analytical system, grouping expenses and income according to major interventions in crop production (e.g., land preparation, fertilization, sowing, crop protection, irrigation, pruning, harvest, etc.). It also merges data with "Riego_areas" to include field area information, and calculates the water amount per hectare ("Cantidad_ha"). This is critical from an analytical standpoint: in farming, all unitary cash flows (or resources like water and energy) are structured per hectare, allowing us to disconnect from absolute scale and enabling comparative analysis.

Finally, unnecessary columns are removed, and the dataset is filtered to exclude null or incomplete projects before outputting the transformed data. This table is the output from the Aguas-Riego cluster query. As can be observed, it includes "Cantidad" (water volume in m³) expressed in both absolute terms and per hectare for each project, as well as expenses (absolute and relative per hectare and per project). It should be noted that the M code references multiple other transformed tables that require prior data processing. Let’s now retroactively review the input tables that feed into the AGUAS-RIEGO table.

**SALDO_RIEGO (farm ID)**
```vs
let
    Origen = Sql.Databases("WIN-19AB\ERP"),
    REPORTING = Origen{[Name="REPORTING"]}[Data],
    dbo_ANALITICA_CULTIVO = REPORTING{[Schema="dbo",Item="ANALITICA_CULTIVO"]}[Data],
    /*Aqui filtramos por area de responsabilidad - el codigo 46 del area de responsabilidad corresponde a AGUAS-RIEGOS*/
    #"Filas filtradas2" = Table.SelectRows(dbo_ANALITICA_CULTIVO, each ([Id_AreaResponsabilidad] = 46)),
    #"Texto insertado después del delimitador" = Table.AddColumn(#"Filas filtradas2", "Texto después del delimitador", each Text.AfterDelimiter([PROYECTO], "- "), type text),
    #"Columna condicional agregada" = Table.AddColumn(#"Texto insertado después del delimitador", "TIPO.GASTO", each if [Texto después del delimitador] = "FUERZA" then "Variable" else if [Texto después del delimitador] = "AGUA RIEGO TRACTORES" then "Variable" else if [Texto después del delimitador] = "MOTOR CAMPEON" then "Variable" else if [Texto después del delimitador] = "JORNALES AGUA RIEGO" then "Variable" else if [Texto después del delimitador] = "OBRAS" then "Variable" else if [Texto después del delimitador] = "REPARACIONES ELECTRICAS" then "Variable" else if [Texto después del delimitador] = "REPARACIONES MECANICAS" then "Variable" else if [Texto después del delimitador] = "VARIOS" then "Variable" else if [Texto después del delimitador] = "VEHICULO " then "Variable" else "Fijo"),
    #"Año insertado" = Table.AddColumn(#"Columna condicional agregada", "Año", each Date.Year([FECHAIMPUTACION]), Int64.Type),
    #"CAMPAÑA VIGENTE" = Table.SelectRows(#"Año insertado", each ([EjercicioAnalitico] = "22/23 - CAMPAÑA 2022/2023" or [EjercicioAnalitico] = "23/24 - CAMPAÑA 2023/2024")),
    #"Trimestre insertado" = Table.AddColumn(#"CAMPAÑA VIGENTE", "Trimestre", each Date.QuarterOfYear([FECHAIMPUTACION]), Int64.Type),
    #"Filas filtradas3" = Table.SelectRows(#"Trimestre insertado", each ([SUBGRUPO] = "GAS - GASTOS")),
    #"Columna condicional agregada1" = Table.AddColumn(#"Filas filtradas3", "coeficiente", each if [IMPORTEAPLICADO] < 0 then 1 else -1),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Columna condicional agregada1",{{"coeficiente", Int64.Type}}),
    #"Multiplicación insertada" = Table.AddColumn(#"Tipo cambiado", "Multiplicación", each [coeficiente] * [IMPORTEAPLICADO], type number),
    #"Filas agrupadas" = Table.Group(#"Multiplicación insertada", {"TIPOGRUPO", "TIPO.GASTO", "EjercicioAnalitico", "Año", "Trimestre"}, {{"SALDO", each List.Sum([Multiplicación]), type number}}),
    #"Filas filtradas1" = Table.SelectRows(#"Filas agrupadas", each ([TIPOGRUPO] = "Gastos")),
    Order_key = Table.AddColumn(#"Filas filtradas1", "Order", each [Año] * [Año] + [Trimestre], Int64.Type),
    #"Filas ordenadas" = Table.Sort(Order_key,{{"Order", Order.Ascending}}),
    #"Conservar las últimas filas" = Table.LastN(#"Filas ordenadas", 9),
    #"Filas agrupadas1" = Table.Group(#"Conservar las últimas filas", {"TIPOGRUPO", "TIPO.GASTO", "Año"}, {{"SALDO", each List.Sum([SALDO]), type nullable number}}),
    #"Filas filtradas" = Table.SelectRows(#"Filas agrupadas1", each ([Año] <> 2024))
in
    #"Filas filtradas"
```
The SALDO_RIEGO table (identified by farm ID) is a fundamental table that consolidates the total year-to-date (YTD) irrigation expenses, breaking them down by TIPO.GASTO into variable and fixed costs. This provides the cumulative expense summary, which serves as the total "pie" that will be divided across various analytical projects in the AGUAS-RIEGO table, as described earlier. The SALDO_RIEGO table is populated by financial information from the ERP, reflecting cash flows that are recorded in the ANALITICA table.

The code connects to a SQL Server database ("WIN-19AB\ERP") and retrieves data from the ANALITICA_CULTIVO table within the REPORTING database, focusing specifically on records where the area of responsibility (Id_AreaResponsabilidad) is 46, corresponding to the "AGUAS-RIEGOS" section. It creates a new column that extracts text after the hyphen in the PROYECTO column and classifies the expenses into either "Variable" or "Fijo" in the TIPO.GASTO column based on specific keywords. The code also extracts the year from the FECHAIMPUTACION column, filtering the data to include only records from the 2022/2023 and 2023/2024 campaigns. It adds a column to determine the quarter of the year based on FECHAIMPUTACION and further filters the data to include only rows categorized under the SUBGRUPO "GAS - GASTOS". A new conditional column called coeficiente is created to differentiate between positive and negative values in the IMPORTEAPLICADO field, and this coeficiente is multiplied by the applied amount. The code groups the data by various fields, including TIPOGRUPO, TIPO.GASTO, and year, summing the relevant values. The dataset is then sorted by a calculated key based on the year and quarter, and only the last nine rows are retained. Finally, the data is grouped again by expense type and year, with a sum of the SALDO column, and any data from 2024 is excluded before the transformed dataset is returned.

**Riego_total**
```vs
let
    Origen = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1u7LYecQ7W-ffxf0UpujMYrShQ5kBqXH9RPNlwyzN0jk/edit?usp=sharing"),
    #"Respuestas de formulario 1_Table" = Origen{[name="Respuestas de formulario 1",ItemKind="Table"]}[Data],
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Respuestas de formulario 1_Table", [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Marca temporal", type datetime}, {"FECHA", type date}, {"CULTIVO/PARCELA", type text}, {"PASES", Int64.Type}, {"PORCENTAJE%", type number}, {"HORAS", type any}, {"OBSERVACIONES", type any}, {"COEFICIENTE", type number}, {"HAS", type number}, {"TIPO", type text}, {"PASES 100%", type number}, {"M3", type number}, {"M3/HA", type number}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo cambiado",{{"PORCENTAJE%", "PORCENTAJE"}}),
    #"Filas filtradas1" = Table.SelectRows(#"Columnas con nombre cambiado", each ([Marca temporal] <> null)),
    #"Columnas quitadas" = Table.RemoveColumns(#"Filas filtradas1",{"Marca temporal", "COEFICIENTE", "HAS", "TIPO", "PASES 100%", "M3", "M3/HA"}),
    #"Consultas combinadas" = Table.NestedJoin(#"Columnas quitadas", {"CULTIVO/PARCELA"}, #"DATOS-RIEGO", {"CULTIVO/PARCELA"}, "DATOS", JoinKind.LeftOuter),
    #"Se expandió DATOS" = Table.ExpandTableColumn(#"Consultas combinadas", "DATOS", {"HAS", "TIPO", "COEFICIENTE(L/M2)", "PROYECTO"}, {"HAS", "TIPO", "COEFICIENTE(L/M2)", "PROYECTO"}),
    #"Filas ordenadas" = Table.Sort(#"Se expandió DATOS",{{"FECHA", Order.Ascending}}),
    #"Personalizada agregada" = Table.AddColumn(#"Filas ordenadas", "PASES_100", each [PASES]*(1/[PORCENTAJE])),
    #"Personalizada agregada1" = Table.AddColumn(#"Personalizada agregada", "M3", each if [TIPO]="PIVOT" then [PASES_100]*[#"COEFICIENTE(L/M2)"]*[HAS]*10 else [HORAS]*[#"COEFICIENTE(L/M2)"]*[HAS]*10),
    #"Personalizada agregada2" = Table.AddColumn(#"Personalizada agregada1", "M3/HA", each [M3]/[HAS]),
    #"Año insertado" = Table.AddColumn(#"Personalizada agregada2", "Año", each Date.Year([FECHA]), Int64.Type),
    #"Valor reemplazado" = Table.ReplaceValue(#"Año insertado",2022,2022,Replacer.ReplaceValue,{"Año"}),
    #"Filas filtradas2" = Table.SelectRows(#"Valor reemplazado", each ([Año] = 2023) and ([M3] <> null)),
    #"Consultas combinadas1" = Table.NestedJoin(#"Filas filtradas2", {"Año"}, #"SALDO_RIEGO La Reina", {"Año"}, "SALDO_RIEGO", JoinKind.LeftOuter),
    #"Se expandió SALDO_RIEGO" = Table.ExpandTableColumn(#"Consultas combinadas1", "SALDO_RIEGO", {"TIPO.GASTO", "SALDO"}, {"TIPO.GASTO", "SALDO"}),
    #"Filas agrupadas" = Table.Group(#"Se expandió SALDO_RIEGO", {"PROYECTO", "TIPO.GASTO"}, {{"M3", each List.Sum([M3]), type number}, {"M3/ha", each List.Average([#"M3/HA"]), type number}, {"SALDO", each List.Median([SALDO]), type nullable number}}),
    #"Filas filtradas" = Table.SelectRows(#"Filas agrupadas", each ([PROYECTO] <> null)),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Filas filtradas",{"PROYECTO", "M3/ha"}),
    #"Filas agrupadas1" = Table.Group(#"Columnas quitadas1", {"SALDO"}, {{"M3", each List.Sum([M3]), type number}})
in
    #"Filas agrupadas1"
```
This table essentially replicates the computation procedure followed earlier, but in this case, it calculates the total volume of water used year-to-date. The code imports data from the same Google Sheets document, processes it by transforming data types, adding calculated columns, and joining it with another table ("DATOS-RIEGO"). It filters the data for the year of analysis, calculates water usage ("M3" and "M3/HA"), and merges it with the "SALDO_RIEGO" table. Finally, it groups and sums the results based on specific fields, producing a summarized dataset of overall water usage and expenses.

**Riego_areas**
```vs
let
    Origen = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1u7LYecQ7W-ffxf0UpujMYrShQ5kBqXH9RPNlwyzN0jk/edit?usp=sharing"),
    DATOS_Table = Origen{[name="DATOS",ItemKind="Table"]}[Data],
    #"Columnas quitadas" = Table.RemoveColumns(DATOS_Table,{"Column1"}),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Columnas quitadas", [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"ID", Int64.Type}, {"PARCELA", type text}, {"CULTIVO", type text}, {"CULTIVO/PARCELA", type text}, {"HAS", type number}, {"TIPO", type text}, {"COEFICIENTE(L/M2)", type number}, {"PROYECTO ANALÍTICO", type text}}),
    #"Columnas quitadas1" = Table.RemoveColumns(#"Tipo cambiado",{"PROYECTO ANALÍTICO"}),
    #"CREAR PROYECTOS" = Table.AddColumn(#"Columnas quitadas1", "PROYECTO", each if [CULTIVO] = "ALMENDROS PIRINEO 2016" then "F925000006 - ALMENDROS 2016" else if [CULTIVO] = "ESPINACAS" then "F923000001 - ESPINACAS GELAGRI" else if [#"CULTIVO/PARCELA"] = "12-AJOS-P10T_CORDOBA" then "F922000001 - AJOS CHINOS" else if [#"CULTIVO/PARCELA"] = "19-GUISANTES-P11T_ALMODOVAR" then "F923000003 - GUISANTES" else if [#"CULTIVO/PARCELA"] = "22-GUISANTES-PTUMBAITO_ALMODOVAR" then "F923000014 - GUISANTES SPS" else if [#"CULTIVO/PARCELA"] = "14-GUISANTES-P13T_ALMODOVAR_NORTE" then "F923000014 - GUISANTES SPS" else if [#"CULTIVO/PARCELA"] = "17-GUISANTES-P13T_ALMODOVAR_SUR" then "F923000014 - GUISANTES SPS" else if [#"CULTIVO/PARCELA"] = "20-TRIGO LIMAGRAIN-P11T_CORDOBA" then "F941000101 - CAMPITO LIMAGRAIN" else if [#"CULTIVO/PARCELA"] = "21-TRIGO DURO-PTUMBAITO_CORDOBA" then "F921000001 - TRIGO DURO" else if [#"CULTIVO/PARCELA"] = "13-ALMENDROS MAJOLETO 2020-ALMENDROS MAJOLETO 2020" then "F925000020 - ALMENDROS BELONA" else if [#"CULTIVO/PARCELA"] = "23-ALMENDROS SECANILLOS 2019-ALMENDROS SECANILLOS 2019" then "F925000010 - ALMENDROS 2019 SECANILLOS" else if [#"CULTIVO/PARCELA"] = "24-ALMENDROS SETO P.4-5 2019-ALMENDROS SETO P.4-5 2019" then "F925000011 - ALMENDROS 2019 SETO" else if [#"CULTIVO/PARCELA"] = "25-ALMENDROS PIRINEO 2014-ALMENDROS PIRINEO 2014" then "F925000001 - ALMENDROS 2014" else if [#"CULTIVO/PARCELA"] = "26-ALMENDROS PIRINEO 2016-ALMENDROS PIRINEO 2016" then "F925000006 - ALMENDROS 2016" else if [#"CULTIVO/PARCELA"] = "27-ALMENDROS PIRINEO NUEVO19-ALMENDROS PIRINEO NUEVO19" then "F925000018 - ALMENDROS 2020 CARACOL (FASE I) Y PIRINEO NUEVO" else if [#"CULTIVO/PARCELA"] = "30-ALMENDROS CARACOL 2020-ALMENDROS CARACOL 2020" then "F925000018 - ALMENDROS 2020 CARACOL (FASE I) Y PIRINEO NUEVO" else if [#"CULTIVO/PARCELA"] = "34-ALMENDROS CARACOL 2021-ALMENDROS CARACOL 2021" then "F925000019 - ALMENDROS 2021 CARACOL (FASE II)" else if [#"CULTIVO/PARCELA"] = "41-ALMENDROS LA BARCA 2014-ALMENDROS LA BARCA 2014" then "F925000001 - ALMENDROS 2014" else if [#"CULTIVO/PARCELA"] = "6-ALMENDROS ZAHURDAS 2018-ALMENDROS ZAHURDAS 2018" then "F925000020 - ALMENDROS BELONA" else if [#"CULTIVO/PARCELA"] = "7-PISTACHOS ZAHURDAS 2018-PISTACHOS ZAHURDAS 2018" then "F925000014 - PISTACHOS 2017 ZAHURDA" else if [#"CULTIVO/PARCELA"] = "9-PISTACHOS P.23-24 2020-PISTACHOS P.23-24 2020" then "F925000015 - PISTACHOS 2020 MEANDRO (P23-24)" else if [#"CULTIVO/PARCELA"] = "10-OLIVAR ECO SOTO-OLIVAR ECO SOTO" then "F925000017 - OLIVAR ECOLOGICO" else if [#"CULTIVO/PARCELA"] = "18-TRIGO DURO-P13T_CORDOBA_SUR" then "F921000001 - TRIGO DURO" else [CULTIVO]),
    #"Filas agrupadas" = Table.Group(#"CREAR PROYECTOS", {"PROYECTO"}, {{"Superficie_ha", each List.Sum([HAS]), type nullable number}})
in
    #"Filas agrupadas"
```
Riego_areas is an important table for the allocation of Aguas-Riego data. Irrigation is not always applied to the same surface area of an analytical project. Some analytical projects, such as autumn-sown vegetables or irrigated maize, can physically occupy more than one irrigated parcel. For example, maize might be grown in three different pivots simultaneously, following the same agronomic itinerary and planned for the same commercial gateway. Analytically, all these maize fields are considered a single analytical project, where the total sum of areas would represent the PROJECT area. However, in such cases, irrigation is managed at a finer scale, specifically at the level of individual irrigated parcels. This table computes these areas, as they require a different procedure than the total analytical project area. While the total sum of irrigation is treated at the analytical project level, irrigation management necessitates a more detailed scale of analysis. This is the purpose of this table: to summarize project areas by their individual irrigated parts.

This code snippet is designed for data transformation in Power Query, focusing on processing data from the same Google Sheets document. It begins by importing the contents of the specified Google Sheet and extracting a table named "DATOS". The subsequent steps involve cleaning and organizing the data: unnecessary columns are removed, the first row is promoted to headers, and the data types of various columns are specified to ensure accurate processing. This includes converting the "ID" column to an integer and setting other columns to text or number formats as appropriate. A specific column, "PROYECTO ANALÍTICO", is also discarded to streamline the dataset. The code then adds a new column titled "PROYECTO", which uses conditional logic to assign project codes based on the values present in the "CULTIVO" or "CULTIVO/PARCELA" columns, ensuring that each cultivation is linked to its respective project. Finally, the data is grouped by the newly created "PROYECTO" column, calculating the total surface area in hectares for each project and resulting in a comprehensive summary of project allocations based on cultivation areas.

**Riego_reparto**
```vs
let
    Origen = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1u7LYecQ7W-ffxf0UpujMYrShQ5kBqXH9RPNlwyzN0jk/edit?usp=sharing"),
    #"Respuestas de formulario 1_Table" = Origen{[name="Respuestas de formulario 1",ItemKind="Table"]}[Data],
    #"Encabezados promovidos" = Table.PromoteHeaders(#"Respuestas de formulario 1_Table", [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Marca temporal", type datetime}, {"FECHA", type date}, {"CULTIVO/PARCELA", type text}, {"PASES", Int64.Type}, {"PORCENTAJE%", type number}, {"HORAS", type any}, {"OBSERVACIONES", type any}, {"COEFICIENTE", type number}, {"HAS", type number}, {"TIPO", type text}, {"PASES 100%", type number}, {"M3", type number}, {"M3/HA", type number}}),
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo cambiado",{{"PORCENTAJE%", "PORCENTAJE"}}),
    #"Columnas quitadas" = Table.RemoveColumns(#"Columnas con nombre cambiado",{"Marca temporal", "COEFICIENTE", "HAS", "TIPO", "PASES 100%", "M3", "M3/HA"}),
    #"Filas filtradas3" = Table.SelectRows(#"Columnas quitadas", each ([FECHA] <> null)),
    #"Consultas combinadas" = Table.NestedJoin(#"Filas filtradas3", {"CULTIVO/PARCELA"}, #"DATOS-RIEGO", {"CULTIVO/PARCELA"}, "DATOS", JoinKind.LeftOuter),
    #"Se expandió DATOS" = Table.ExpandTableColumn(#"Consultas combinadas", "DATOS", {"HAS", "TIPO", "COEFICIENTE(L/M2)", "PROYECTO"}, {"HAS", "TIPO", "COEFICIENTE(L/M2)", "PROYECTO"}),
    #"Filas ordenadas" = Table.Sort(#"Se expandió DATOS",{{"FECHA", Order.Ascending}}),
    #"Tipo cambiado2" = Table.TransformColumnTypes(#"Filas ordenadas",{{"HORAS", type number}}),
    #"Personalizada agregada" = Table.AddColumn(#"Tipo cambiado2", "PASES_100", each [PASES]*(1/[PORCENTAJE])),
    #"Personalizada agregada1" = Table.AddColumn(#"Personalizada agregada", "M3", each if [TIPO]="PIVOT" then [PASES_100]*[#"COEFICIENTE(L/M2)"]*[HAS]*10 else [HORAS]*[#"COEFICIENTE(L/M2)"]*[HAS]*10),
    #"Tipo cambiado3" = Table.TransformColumnTypes(#"Personalizada agregada1",{{"PASES_100", type number}, {"M3", type number}}),
    #"Personalizada agregada2" = Table.AddColumn(#"Tipo cambiado3", "M3/HA", each [M3]/[HAS]),
    #"Tipo cambiado4" = Table.TransformColumnTypes(#"Personalizada agregada2",{{"M3/HA", type number}}),
    #"Año insertado" = Table.AddColumn(#"Tipo cambiado4", "Año", each Date.Year([FECHA]), Int64.Type),
    #"Valor reemplazado" = Table.ReplaceValue(#"Año insertado",2022,2022,Replacer.ReplaceValue,{"Año"}),
    #"Filas filtradas2" = Table.SelectRows(#"Valor reemplazado", each ([FECHA] <> null)),

    #"Consultas combinadas1" = Table.NestedJoin(#"Filas filtradas2", {"Año"}, #"SALDO_RIEGO La Reina", {"Año"}, "SALDO_RIEGO", JoinKind.LeftOuter),
    #"Se expandió SALDO_RIEGO" = Table.ExpandTableColumn(#"Consultas combinadas1", "SALDO_RIEGO", {"TIPO.GASTO", "SALDO"}, {"TIPO.GASTO", "SALDO"}),
    #"Filas filtradas1" = Table.SelectRows(#"Se expandió SALDO_RIEGO", each ([SALDO] <> null)),
    #"Filas agrupadas" = Table.Group(#"Filas filtradas1", {"PROYECTO", "TIPO.GASTO"}, {{"M3", each List.Sum([M3]), type number}, {"SALDO", each List.Median([SALDO]), type nullable number}}),
    #"Consultas combinadas2" = Table.NestedJoin(#"Filas agrupadas", {"SALDO"}, Riego_total, {"SALDO"}, "Riego_total", JoinKind.LeftOuter),
    #"Se expandió Riego_total" = Table.ExpandTableColumn(#"Consultas combinadas2", "Riego_total", {"M3"}, {"Riego_total.M3"}),
    #"Porcentaje insertado de" = Table.AddColumn(#"Se expandió Riego_total", "Porcentaje de", each [M3] / [Riego_total.M3] * 100, type number),
    #"División insertada" = Table.AddColumn(#"Porcentaje insertado de", "División", each [Porcentaje de] / 100, type number),
    #"Columnas quitadas1" = Table.RemoveColumns(#"División insertada",{"Porcentaje de"}),
    #"Columnas con nombre cambiado1" = Table.RenameColumns(#"Columnas quitadas1",{{"División", "Peso_relativo"}}),
    #"Personalizada agregada3" = Table.AddColumn(#"Columnas con nombre cambiado1", "Importe", each [SALDO]*[Peso_relativo]),
    #"Multiplicación insertada" = Table.AddColumn(#"Personalizada agregada3", "Multiplicación", each [Importe] * -1, type number),
    #"Columnas quitadas2" = Table.RemoveColumns(#"Multiplicación insertada",{"Importe"}),
    #"Columnas con nombre cambiado2" = Table.RenameColumns(#"Columnas quitadas2",{{"Multiplicación", "Importe_global"}}),
    #"Personalizada agregada4" = Table.AddColumn(#"Columnas con nombre cambiado2", "PARTIDA", each "RIEGOS"),
    #"Personalizada agregada5" = Table.AddColumn(#"Personalizada agregada4", "Campaña", each "22/23"),
    #"Personalizada agregada6" = Table.AddColumn(#"Personalizada agregada5", "Nombre Actividad Producción", each "RIEGO"),
    #"Personalizada agregada7" = Table.AddColumn(#"Personalizada agregada6", "Nombre Artículo", each "Riego"),
    #"Consultas combinadas3" = Table.NestedJoin(#"Personalizada agregada7", {"PROYECTO"}, Riego_areas, {"PROYECTO"}, "Riego_areas", JoinKind.LeftOuter),
    #"Se expandió Riego_areas" = Table.ExpandTableColumn(#"Consultas combinadas3", "Riego_areas", {"Superficie_ha"}, {"Superficie_ha"}),
    #"Personalizada agregada8" = Table.AddColumn(#"Se expandió Riego_areas", "TIPOGRUPO", each "Gastos"),
    #"Personalizada agregada9" = Table.AddColumn(#"Personalizada agregada8", "Importe_ha", each [Importe_global]/[Superficie_ha]),
    #"Columnas con nombre cambiado3" = Table.RenameColumns(#"Personalizada agregada9",{{"M3", "Cantidad_global"}}),
    #"Personalizada agregada10" = Table.AddColumn(#"Columnas con nombre cambiado3", "Cantidad_ha", each [Cantidad_global]/[Superficie_ha]),
    #"Columnas quitadas3" = Table.RemoveColumns(#"Personalizada agregada10",{"SALDO", "Riego_total.M3", "Peso_relativo"}),
    #"Personalizada agregada11" = Table.AddColumn(#"Columnas quitadas3", "Nombre Unidad Medida", each "M3"),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Personalizada agregada11",{{"Importe_ha", type number}, {"Cantidad_ha", type number}}),
    #"Texto extraído después del delimitador" = Table.TransformColumns(#"Tipo cambiado1", {{"PROYECTO", each Text.AfterDelimiter(_, "- "), type text}}),
    #"Errores reemplazados" = Table.ReplaceErrorValues(#"Texto extraído después del delimitador", {{"Cantidad_global", 0}}),
    #"Filas filtradas" = Table.SelectRows(#"Errores reemplazados", each ([Cantidad_global] <> 0)),
    #"Columnas quitadas4" = Table.RemoveColumns(#"Filas filtradas",{"Nombre Artículo"}),
    #"Columna condicional agregada" = Table.AddColumn(#"Columnas quitadas4", "Nombre Artículo", each if [TIPO.GASTO] = "Variable" then "RIEGOS - GASTOS VARIABLES" else "RIEGOS - GASTOS FIJOS"),
    #"Columnas quitadas5" = Table.RemoveColumns(#"Columna condicional agregada",{"TIPO.GASTO"}),
    #"Columnas reordenadas" = Table.ReorderColumns(#"Columnas quitadas5",{"PROYECTO", "PARTIDA", "Cantidad_global", "Importe_global", "Campaña", "Nombre Actividad Producción", "Nombre Artículo", "Superficie_ha", "TIPOGRUPO", "Importe_ha", "Cantidad_ha", "Nombre Unidad Medida"}),
    #"Columna multiplicada" = Table.TransformColumns(#"Columnas reordenadas", {{"Importe_global", each _ * -1, type number}}),
    #"Columna multiplicada1" = Table.TransformColumns(#"Columna multiplicada", {{"Importe_ha", each _ * -1, type number}}),
    #"Valor reemplazado1" = Table.ReplaceValue(#"Columna multiplicada1","RIEGOS - GASTOS FIJOS","Gastos fijos",Replacer.ReplaceText,{"Nombre Artículo"}),
    #"Valor reemplazado2" = Table.ReplaceValue(#"Valor reemplazado1","RIEGOS - GASTOS VARIABLES","Gastos variables",Replacer.ReplaceText,{"Nombre Artículo"}),
    #"Consulta anexada" = Table.Combine({#"Valor reemplazado2", #"Riego Villaseca"}),
    #"Columnas quitadas6" = Table.RemoveColumns(#"Consulta anexada",{"Año"}),
    #"Tipo cambiado6" = Table.TransformColumnTypes(#"Columnas quitadas6",{{"Nombre Unidad Medida", type text}}),
    #"Filas filtradas4" = Table.SelectRows(#"Tipo cambiado6", each ([Cantidad_global] <> null)),
    #"Tipo cambiado5" = Table.TransformColumnTypes(#"Filas filtradas4",{{"Nombre Unidad Medida", type text}})
in
    #"Tipo cambiado5"
```
This is my favorite table in the Aguas-Riego allocation process, as it applies the methodology followed by CLR to compute the pie-splitting, which is ultimately the primary objective of this entire table cluster. As previously described, the idea is to guarantee a systematic splitting method that adheres to three major criteria: splitting expenses in the financial overview of irrigation based on total water applied per analytical project (i.e., variable costs), by total surface area irrigated (i.e., fixed costs), and by irrigation events (specific types of expenses that are generally variable).

The script is designed to process data from a Google Sheets spreadsheet. It begins by retrieving content from a specified Google Sheets URL and extracting a specific table named "Respuestas de formulario 1." The script promotes the first row of this table to headers and transforms column types to ensure they are appropriately defined (e.g., date, datetime, text, number). Subsequently, it renames certain columns, removes unnecessary ones, and filters out rows where the "FECHA" (date) column is null.

The code then performs a series of nested joins to combine data with other tables, expanding and renaming columns as needed. It sorts the data by date and adds calculated columns to determine the total passes and cubic meters (M3) based on specific conditions related to the type of irrigation. Additional calculations include determining the percentage of total water used and generating new columns for financial analysis, such as global amounts and costs per hectare. The script also incorporates error handling, replaces specific values, and structures the final output to ensure clarity and consistency, culminating in a cleaned and organized dataset ready for further analysis or reporting.


**Riego_keys**
```vs
let
    Origen = Riego_reparto,
    #"Filas agrupadas" = Table.Group(Origen, {"PROYECTO", "PARTIDA"}, {{"Recuento", each Table.RowCount(_), Int64.Type}})
in
    #"Filas agrupadas"
```
This is a very simple script designed to create a checking table to control whether the distribution is elementary at the PROYECTO and PARTIDA levels. This is key because this table will serve as the foundation for integrating that information into the analytical model. If the distribution is not elementary-based, we will not establish a one-to-many relationship, which could result in repeated and duplicated rows of information, ultimately leading to an overestimation of final costs and water registries. It is fundamental to build control tables in a process of this nature to verify when one-to-many relationships can be preserved by using one table to feed another. This is of major importance in query programming.


### Wiseconn CropCloud Tables

The Wiseconn CropCloud Tables comprise a cluster of 13 distinct tables that store data collected from Wiseconn sensors during field sensing trials. Each table serves a specific purpose in the data management process.

The first table, named SamplingZone, has a unique role as it is not directly linked to the sensors. Instead, it contains geospatial data related to the sampling zone where the sensors are located. This information is essential as it defines the shapefile of the monitored plot. The data from the SamplingZone is crucial for conducting "mask" and "clip" functions on satellite imagery, which helps isolate the field of interest. This step is critical as it enables the scaling of analysis from a point-based perspective to a broader field level, thereby enhancing the overall understanding of the entire agricultural plot.

Two additional tables provide calibration parameters and are not harvested from the sensors: the Crop Water Function and the Water Dynamics Table. The Crop Water Function offers theoretical calibration of water productivity functions based on the conservative behavior of the biomass-water relationship, as described in the following key references:

- Steduto, P., Hsiao, T. C., & Fereres, E. (2007). "On the conservative behavior of biomass water productivity." Water Use Efficiency and Water Productivity, 59(2).

- Raes, D., Steduto, P., Hsiao, T. C., & Fereres, E. (2009). "AquaCrop—The FAO's crop model for water productivity." Water Use Efficiency and Water Productivity, 39(5), 437-447.

The remaining tables in the cluster are dedicated to storing data collected from the Wiseconn sensors installed in the field. Each of these tables provides data on various parameters monitored during the sensing trials.

**Sampling zone**
```vs
let
    Origen = R.Execute("library(sf)#(lf)#(lf)# Load spatial data#(lf)SamplingZone <- read_sf(""C:/DATOS/Proyecto Power BI/PROYECTOS_POWER BI/15_PROYECTO_WISECONN_SISTAGRO/Spatial Data/GFAM-2024-02-22 14_40.shp"")#(lf)#(lf)F.dots <- st_centroid(SamplingZone)#(lf)#(lf)st_crs(F.dots) <- st_crs(""+proj=longlat +datum=WGS84 +no_defs"")"),
    #"Filas filtradas" = Table.SelectRows(Origen, each ([Name] = "F.dots")),
    #"Se expandió Value" = Table.ExpandTableColumn(#"Filas filtradas", "Value", {"Name", "geometry"}, {"Name.1", "geometry"})
in
    #"Se expandió Value"
```

**API - Wiseconn Sensors**
```vs
let
    Origen = Json.Document(Web.Contents("https://api.wiseconn.com/farms/3615/measures", [Headers=[api_key="vv4XnMTH6DQA4KJlDeIJ"]])),
    #"Convertida en tabla" = Table.FromList(Origen, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Se expandió Column1" = Table.ExpandRecordColumn(#"Convertida en tabla", "Column1", {"id", "farmId", "zoneId", "name", "unit", "lastData", "lastDataDate", "monitoringTime", "sensorDepth", "depthUnit", "fieldCapacity", "readilyAvailableMoisture", "soilMostureSensorType", "brand", "nodeId", "physicalConnection", "sensorType", "createdAt"}, {"id", "farmId", "zoneId", "name", "unit", "lastData", "lastDataDate", "monitoringTime", "sensorDepth", "depthUnit", "fieldCapacity", "readilyAvailableMoisture", "soilMostureSensorType", "brand", "nodeId", "physicalConnection", "sensorType", "createdAt"}),
    #"Se expandió physicalConnection" = Table.ExpandRecordColumn(#"Se expandió Column1", "physicalConnection", {"expansionPort", "expansionBoard", "nodePort"}, {"physicalConnection.expansionPort", "physicalConnection.expansionBoard", "physicalConnection.nodePort"}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Se expandió physicalConnection",{{"id", type text}, {"farmId", Int64.Type}, {"zoneId", Int64.Type}, {"name", type text}, {"unit", type text}, {"lastData", type number}, {"lastDataDate", type datetime}, {"monitoringTime", Int64.Type}, {"sensorDepth", Int64.Type}, {"depthUnit", type text}, {"fieldCapacity", type number}, {"readilyAvailableMoisture", type number}, {"soilMostureSensorType", type text}, {"brand", type text}, {"nodeId", Int64.Type}, {"physicalConnection.expansionPort", Int64.Type}, {"physicalConnection.expansionBoard", type text}, {"physicalConnection.nodePort", Int64.Type}, {"sensorType", type text}, {"createdAt", type datetime}})
in
    #"Tipo cambiado"
```
The Wiseconn Sensors Tables API serves as the primary table for accessing keys related to each installed sensor. This information is based on the API key contained within the JSON document, which can be accessed at Wiseconn Developer Portal. The first line of the data specifies the origin through an API key, such as “vv4XnMTH6DQA4KJlDeIJ”, which is obtained from the developers' website. This key enables access to the following endpoint: https://api.wiseconn.com/farms/3615/measures

In this URL, "3615" represents the ID of the farm (or sampling unit), and the API key is georeferenced to pinpoint the exact location from which data is being collected. 

An API key is a unique identifier used to authenticate a user, developer, or application when accessing an API (Application Programming Interface). In the context of cloud computing, API keys play a crucial role in ensuring secure and controlled access to services and data. They help track usage and limit access to authorized users, safeguarding the integrity of the system and its data. Additionally, this license allows the App to make queries to the API to receive Customer data (the “Content”) hosted on the Service, to write data to the Customer’s account, and to generate irrigation schedules or commands for the Customer’s farm via the Service. The Licensee acknowledges that all access to and use of Customer data is contingent upon the Customer's consent, which may be given or withdrawn at any time at their discretion. These are critical points of interest regarding data privacy, legal protection, and accessibility, which are essential for building trustworthy networks of data among organizations in the farming sector.

**Crop Water Functions**
```vs
let
    Origen = Excel.Workbook(File.Contents("C:\DATOS\Proyecto Power BI\PROYECTOS_POWER BI\15_PROYECTO_WISECONN_SISTAGRO\Potato WP.xlsx"), null, true),
    Folha1_Sheet = Origen{[Item="Folha1",Kind="Sheet"]}[Data],
    #"Encabezados promovidos" = Table.PromoteHeaders(Folha1_Sheet, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Y = f (T) [ton/ha/mm]", type number}, {"Source", type text}, {"Water (mm)", Int64.Type}, {"Yield (ton/ha)", type number}, {"N - level", type text}}),
    #"Filas filtradas" = Table.SelectRows(#"Tipo cambiado", each ([#"Y = f (T) [ton/ha/mm]"] <> null))
in
    #"Filas filtradas"
```

**API SWC (depth)**
```vs
let
    Origen = Json.Document(Web.Contents("https://api.wiseconn.com/measures/1-396644/data?initTime=2024-05-01T00:00:00&endTime=2024-07-10T00:00:00", [Headers=[api_key="vv4XnMTH6DQA4KJlDeIJ"]])),
    #"Convertida en tabla" = Table.FromList(Origen, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Se expandió Column1" = Table.ExpandRecordColumn(#"Convertida en tabla", "Column1", {"time", "value"}, {"time", "value"}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Se expandió Column1",{{"time", type datetime}, {"value", type number}}),
    #"Fecha insertada" = Table.AddColumn(#"Tipo cambiado", "Fecha", each DateTime.Date([time]), type date),
    #"Filas agrupadas" = Table.Group(#"Fecha insertada", {"Fecha"}, {{"SWC_20", each List.Average([value]), type nullable number}})
in
    #"Filas agrupadas"
```

The SWC tables are all structurally identical; they begin by calling the API key corresponding to the specific measure ID (in this case, 1-396644 for the soil volumetric water content measured at 20 cm depth sensor) for a given time window (no more than 3 months: data?initTime=2024-05-01T00:00:00&endTime=2024-07-10T00:00:00). This aspect represents a limitation of the current system, as the Wiseconn API solution does not allow us to retrieve data for periods longer than 3 months. However, I have devised a workaround for this issue, which involves replicating the same data in 3-month intervals. We can call successive time periods on different tables and perform an 'annex-query' operation to insert these tables into a single table, ultimately consolidating all the consulted time periods.

The code executes some basic data transformation functions to correct column names and properly order the variables. Then, we use table group functions to transform data from a 15-minute time resolution into daily values, as our water production model operates on daily time steps.

**API Temperature**
```vs
let
    Origen = Json.Document(Web.Contents("https://api.wiseconn.com/measures/1-396664/data?initTime=2024-05-01T00:00:00&endTime=2024-07-10T00:00:00", [Headers=[api_key="vv4XnMTH6DQA4KJlDeIJ"]])),
    #"Convertida en tabla" = Table.FromList(Origen, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Se expandió Column1" = Table.ExpandRecordColumn(#"Convertida en tabla", "Column1", {"time", "value"}, {"time", "value"}),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Se expandió Column1",{{"time", type datetime}, {"value", type number}}),
    #"Fecha insertada" = Table.AddColumn(#"Tipo cambiado", "Fecha", each DateTime.Date([time]), type date),
    #"Filas agrupadas" = Table.Group(#"Fecha insertada", {"Fecha"}, {{"T", each List.Average([value]), type nullable number}})
in
    #"Filas agrupadas"
```
The same process is replicated here by adjusting the measure IDs, as well as for the Rain API from the pluviometer and the ETo API provided by the weather station.

**Field Validation Data (crop ID)**
```vs
let
    Origen = Csv.Document(File.Contents("C:\DATOS\Proyecto Power BI\PROYECTOS_POWER BI\15_PROYECTO_WISECONN_SISTAGRO\Potato - Rojas Farm\Patatas_Rojas_Validation_Dataset.txt"),[Delimiter="	", Columns=8, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(Origen, [PromoteAllScalars=true]),
    #"Tipo cambiado1" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Value", type number}}),
    #"Tipo cambiado con configuración regional" = Table.TransformColumnTypes(#"Tipo cambiado1", {{"Date", type date}}, "es-ES"),
    #"Filas filtradas 20.6" = Table.SelectRows(#"Tipo cambiado con configuración regional", each not ([Date] = #date(2024, 6, 20) and [Parameter_code] = "TW" and ([Value] < 100))),
    #"Filas filtradas 04.7" = Table.SelectRows(#"Filas filtradas 20.6", each not ([Date] = #date(2024, 7, 4) and [Parameter_code] = "TW" and ([Value] < 120))),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Filas filtradas 04.7",{{"iD", Int64.Type}, {"Sample iD", type text}, {"Parameter_code", type text}, {"Date", type date}, {"Value", type number}, {"Parameter", type text}, {"Method", type text}, {"Unit", type text}}),
    #"Filas agrupadas" = Table.Group(#"Tipo cambiado", {"Parameter_code", "Date", "Parameter", "Unit"}, {{"Value", each List.Average([Value]), type nullable number}, {"TQ", each Table.RowCount(_), Int64.Type}}),
    #"Valor reemplazado" = Table.ReplaceValue(#"Filas agrupadas",#date(2024, 7, 4),#date(2024, 7, 2),Replacer.ReplaceValue,{"Date"})
in
    #"Valor reemplazado"
```
The Field Validation Data is a crucial dataset that must be populated with field records of biomass samples. This dataset is built according to a sampling scheme governed by one of two objectives: either to select visually comparable zones for sampling or to sample areas in close proximity that do not differ significantly from the sensor points.

The data was collected on a weekly to biweekly basis, following a long-row structure table with variable columns. A code ID was used to distinguish the crop traits sampled (e.g., AB for aerial biomass, RW for root weight, TW for tuber weight).

In our analysis, the M code applies outlier exclusion rules. This became necessary when we observed, in the case of potatoes, a second tuber initiation occurring around 60-80 days after sowing. This event negatively affected the validation dataset because we used a count function to determine the number of tubers, which is essential for computing individual tuber weight. As second tuberization occurred, the total tuber count increased more than the relative contribution of additional weight to the total yield, resulting in an underestimation of individual tuber weight. We began rejecting tubers with very low weight and size after the middle of the growth cycle to improve accuracy.

The sampling method involved field sampling, sample weighing, fresh and dry mass estimation, and recording the data in an Excel file, which is stored on the company's main server, as indicated in the first step of this code chunk: C:\DATOS\Proyecto Power BI\PROYECTOS_POWER BI\15_PROYECTO_WISECONN_SISTAGRO\Potato - Rojas Farm\Patatas_Rojas_Validation_Dataset.txt.

**HI (crop ID)**
```vs
let
    Origen = Excel.Workbook(File.Contents("C:\DATOS\Proyecto Power BI\PROYECTOS_POWER BI\15_PROYECTO_WISECONN_SISTAGRO\Potato WP.xlsx"), null, true),
    HI_Sheet = Origen{[Item="HI",Kind="Sheet"]}[Data],
    #"Encabezados promovidos" = Table.PromoteHeaders(HI_Sheet, [PromoteAllScalars=true]),
    #"Tipo cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos",{{"Source", type text}, {"HI Value", type number}, {"Water level", type text}})
in
    #"Tipo cambiado"
```
We identify a key step for yield estimation in tuber crops, such as potatoes: the transition from estimated biomass to tuber weight, which relies on the harvest index. The final tuber yield is not solely determined by the total accumulated biomass of the plant at the end of the growing cycle; it also depends on the partitioning of biomass from vegetative aerial organs to underground tubers. This can be mathematically simplified by the concept of the harvest index, which corresponds to the ratio of the plant's harvestable product weight (i.e., tubers) to the total plant biomass, including both aerial and underground organs.

However, this classical rule is not static, as partitioning is a process influenced by the crop’s stress status, which can be affected by nutrient and water levels. Since our model is water-driven and focused on plant-water dynamics, our HI crop table provides the necessary details to adjust the daily harvest index (HI) value based on the mean volumetric water content of the soil at any given time.

To address this, we conducted a focused literature review to build a generic Excel database that links the expected harvest index to water levels. The model uses DAX to compute a conditional function that applies a daily harvest index value based on specific soil moisture levels. These DAX operations will be explained later, as we are still reviewing the ETL phase.

## Other Queries

**fCalendario**
```vs
let
    Origen = (FI as date, FF as date)=>
    let
        Duracion =  Duration.TotalDays(FF-FI) + 1,
        ListaFechas = List.Dates(FI,Duracion,#duration(1,0,0,0)),
        TablaFechas = Table.FromList(ListaFechas, Splitter.SplitByNothing(), type table [Fecha = date], null, ExtraValues.Error),
        ColumnaAño = Table.AddColumn(TablaFechas,"Año", each Date.Year([Fecha]), Int64.Type),
        ColumnaNroMes = Table.AddColumn(ColumnaAño,"Nro. Mes", each Date.Month([Fecha]), Int64.Type),
        ColumnaMes = Table.AddColumn(ColumnaNroMes,"Mes", each Date.MonthName([Fecha]), type text),
        ColumnaTrimestre = Table.AddColumn(ColumnaMes,"Trimestre", each Text.From(Date.QuarterOfYear([Fecha])) & "T", type text),
        ColumnaSemana = Table.AddColumn(ColumnaTrimestre,"Semana", each Date.WeekOfYear([Fecha],Day.Monday), Int64.Type),
        ColumnaDiaSemana = Table.AddColumn(ColumnaSemana,"Día Semana", each if Date.DayOfWeek([Fecha],Day.Monday) = 0 then 7 else Date.DayOfWeek([Fecha],Day.Monday),Int64.Type),
        ColumnaYYMM = Table.AddColumn(ColumnaDiaSemana, "YYMM", each [Año]*100 + [Nro. Mes], Int64.Type)  
    in
       ColumnaYYMM
in 
    Origen
```
This code generates a detailed date table for a specified date range. The function accepts two parameters: FI (the start date) and FF (the end date). It begins by calculating the duration between these dates in days, adding one to include the end date. Then, it creates a list of consecutive dates from FI to FF with a one-day increment. This list is converted into a table with a single column named Fecha, containing all dates in the specified range. The function proceeds to add columns to enrich the table with additional date attributes. The "Año" column extracts the year from each date, while the "Nro. Mes" and "Mes" columns provide the numeric month and month name, respectively. The "Trimestre" column represents the quarter of the year with a suffix "T" (e.g., "1T" for Q1). For weekly tracking, the "Semana" column calculates the week of the year, assuming weeks start on Monday, and "Día Semana" indicates the day of the week numerically, setting Monday as 1 and Sunday as 7. Finally, a "YYMM" column is added, which formats the year and month in a YYMM format (e.g., "202301" for January 2023), combining the year and numeric month into a single value. Once all columns are added, the table is outputted, providing a date table enriched with detailed time-related fields, which can be highly useful for time-based data analysis and reporting tasks.

We opted to set a calendar date dimension table in this context because, in a star schema model in Power BI, a calendar date dimension is essential for organizing and analyzing time-series data, particularly within agricultural data science applications. This structure supports efficient DAX computations, allowing consistent filtering, aggregation, and comparison across various date attributes, such as year, month, and season—critical for tracking crop cycles, weather patterns, and yield trends. Using a calendar date table enables us to align agricultural events like planting and harvest dates with external variables such as rainfall or temperature, improving predictive modeling. Ultimately, this unified date reference enhances the accuracy of insights, empowering data-driven decisions in agriculture.

**DAX**

In Power Query, the DAX measures table is a designated space for storing custom DAX (Data Analysis Expressions) calculations within a Power BI data model. Typically created as an empty table using the "Enter Data" feature, it is labeled with a clear name like "Measures" or "Calculations" to differentiate it from data tables. Users can add individual DAX measures via the "New Measure" option, with each measure having a unique name and DAX formula. This measures table serves as a centralized reference for calculations, streamlining the data model and simplifying the process of finding and modifying measures. The DAX measures are dynamic, context-sensitive, and recalibrate based on slicers, filters, and user interactions, enabling real-time insights in Power BI reports. By centralizing measures, Power BI models become cleaner and more intuitive, especially useful in complex analytical projects requiring a consistent calculation structure.

**Roles**

The Roles table in Power BI is essential for implementing row-level security, segmenting data access, enhancing collaboration, testing security configurations, and creating dynamic security solutions. This functionality ensures that sensitive data is protected and that users only have access to the information relevant to their specific roles.

In this context, we specify all Abecera SL employees who have direct access to Power BI services for reports and dashboard visualization. Additionally, we define which email address or employee has access to each component of the reports and dashboards.

Once the entire ETL process is completed in the Query section, we load all the tables into the data model, and the phase of DAX coding and reporting visual setup begins.

## DAX code

**DAX Measures for CropCloudAnalytics**

The following DAX scripts contain various functions developed for CropCloudAnalytics. These measures encompass aspects such as biomass production, water consumption attributed to transpiration as inferred by soil water probes, soil water dynamics modeling including hydraulic properties parameterization, field sampling data cleaning and usage, harvest index modeling as a function of soil water content, and accounting analytics including FRC-, TCB-, YTD-metrics, income estimation, cost tracking, and margin decomposition. Additionally, plant counting measures and averaging measures, as well as model error assessments are also included. The following script serves as a reference for these functionalities.

**1. Accumulated Biomass (ton/ha)**

Calculates the total accumulated biomass over time, adjusting for an initial state.
```dax
Accumulated Biomass (ton/ha) = 
VAR InitialState = 2250
VAR CurrentDate = MAX('Wiseconn Data'[Date])
RETURN
CALCULATE (
    SUMX(
        FILTER(
            ALL('Wiseconn Data'),
            'Wiseconn Data'[Date] <= CurrentDate
        ),
        [Biomass]
    ) + InitialState
)
```

**2. YTD Income**

Estimates year-to-date income based on the yield and a fixed price per unit.

```dax
YTD Income = 
VAR precio = 0.28
RETURN
    [Tuber Yield (Kg/ha)] * precio
```

**3. YTD Costs**

Calculates year-to-date costs, converting import to a negative value for cost tracking.

```dax
YTD Costs = [SUMX Importe / ha] * -1
```

**4. YTD Margin**
Computes the year-to-date margin by subtracting costs from income.

```dax
YTD Margin = [YTD Income]-[YTD Costs]
```

**5. YTD Margin-to-Income Ratio**
Determines the ratio of margin to income, providing insight into financial performance.

```dax
YTD Margin-to-Income Ratio = ([FRC Income] - [FRC Cost]) / [FRC Income]
```

6. Tuber Yield (Kg/ha)

Calculates the tuber yield per hectare, factoring in biomass and harvest index.

```dax
Tuber Yield (Kg/ha) = 
VAR InitialState = 2250
VAR CurrentDate = MAX('Wiseconn Data'[Date])
RETURN
CALCULATE (
    SUMX(
        FILTER(
            ALL('Wiseconn Data'),
            'Wiseconn Data'[Date] <= CurrentDate
        ),
        [Biomass]* [HI]
    ) + InitialState
)
```

**7. Model Error**
Assesses the model error by comparing accumulated biomass to field sample biomass.

```dax
Model Error = 
AVERAGEX(
    'Field Validation Data Patatas', 
    IF(
        NOT(ISBLANK([Field Sample Biomass FM (kg/ha)])),
        ([Accumulated Biomass (ton/ha)] - [Field Sample Biomass FM (kg/ha)]) / [Field Sample Biomass FM (kg/ha)],
        BLANK()
    )
)
```

**8. Average Model Error Tuber**

Calculates the average error for tubers based on field samples.

```dax
Average Model Error Tuber = 
AVERAGEX(
    'Wiseconn Data',
    [Model Error Tuber]
)
```

**9. Plant Quantity (#)**

Counts the number of plants based on field validation data.

```dax
Plant Quantity (#) = 
SUMX(
    FILTER(
        'Field Validation Data Patatas',
        'Field Validation Data Patatas'[Parameter_code] = "AB"
    ),
    'Field Validation Data Patatas'[TQ]
)
```

**10. Crop Water Function**
    
Models the crop's primary production based on transpiration and other factors.

```dax
Crop Water Function = 
AVERAGEX('Crop Water Function', 
'Crop Water Function'[Y = f (T) [ton/ha/mm]]
```

**11. Total SUMX Importe / ha**

Calculates the total import per hectare, filtered to exclude blank article names.

```dax
Total SUMX Importe / ha = 
CALCULATE(
    [SUMX Importe] / [SUMX Superficie],
    NOT ISBLANK('PARTES_ERP'[Nombre Familia Artículo]),
    ALL('PARTES_ERP'[Nombre Familia Artículo])
)
```

**12. Average PWP**

Calculates the average of plant available water capacity (PWP) across different products.

```dax
Average PWP = 
AVERAGEX(
    VALUES('Water Dynamics'[PWP]),
    CALCULATE(
        SUM('Water Dynamics'[PWP])
    )
)
```

**13. FRC Income**

Calculates income based on year-to-date figures and the average surface area.

```dax
FRC Income = [YTD Income] * [AVERAGEX of Superficie_ha]
```


**14. FRC Cost**

Calculates the total costs related to year-to-date figures and average surface area.

```dax
FRC Cost = ([YTD Costs]) * [AVERAGEX of Superficie_ha]
```

**15. Fixed Cost Measure**

Calculates fixed costs for each project and campaign by averaging the total costs per hectare.

```dax
Fixed Cost Measure = 
CALCULATE (
    AVERAGEX (
        SUMMARIZE (
            ANALITICA_ERP,
            ANALITICA_ERP[PROYECTO],
            ANALITICA_ERP[Campaña],
            "TotalImporte_ha", SUM ( ANALITICA_ERP[Importe_ha] )
        ),
        [TotalImporte_ha]
    ),
    ALL ( PARTES_ERP[Nombre Familia Artículo] )
)
```


**16. Field Sample Biomass FM (kg/ha)**

Calculates field sample biomass for fresh matter, adjusting based on plant quantity.

```dax
Field Sample Biomass FM (kg/ha) = 
SUMX(
    FILTER(
        'Field Validation Data Patatas',
        'Field Validation Data Patatas'[Parameter_code] = "AB" || 'Field Validation Data Patatas'[Parameter_code] = "RB"
    ),
    'Field Validation Data Patatas'[Value]
) / 1000 * [Plant Quantity (#)] * (10000 / (2*0.7))
```

**17. Field Sample Tuber FM (kg/ha)**

Calculates field sample tuber fresh matter, applying a correction factor for specific dates.

```dax
Field Sample Tuber FM (kg/ha) = 
SUMX(
    FILTER(
        'Field Validation Data Patatas',
        'Field Validation Data Patatas'[Parameter_code] = "TW"
    ),
    'Field Validation Data Patatas'[Value]
) / 1000 * [Plant Quantity (#)] *
IF(
    SELECTEDVALUE('Wiseconn Data'[Date]) = DATE(2024, 6, 20),
    1.5 * (10000 / (2 * 0.7)), 
    (10000 / (2 * 0.7)) 
)
```

**18. Productividad_ha**

Computes productivity per hectare, factoring in the area and product quantities.

```dax
Productividad_ha = 
AVERAGEX (
    VALUES('ANALITICA_ERP'[PROYECTO]),
    DIVIDE (
        SUMX (
            'ANALITICA_ERP',
            [Cantidad_PRODUCTO]
        ),
        CALCULATE (
            SUMX (
                'Areas',
                'Areas'[Superficie_ha]
            ),
            ALLEXCEPT('ANALITICA_ERP', 'ANALITICA_ERP'[ID.AREA]),
            VALUES('ANALITICA_ERP'[ID.AREA])
        )
    )
)
```





## Notes and Comments
Include any additional notes, comments, or considerations relevant to the Power Query M source code.


