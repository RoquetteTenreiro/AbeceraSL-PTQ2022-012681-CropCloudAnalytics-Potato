# Data Storage and Structuring for CropCloudAnalytics: ETL Models

This document contains the Power Query M source code utilized in the CropCloudAnalytics model. It serves as a comprehensive repository for all queries related to data extraction, transformation, and loading processes within the application. The structured format allows for easy navigation and reference for developers and data analysts involved in maintaining and enhancing the CropCloudAnalytics system.

## Table of Contents
1. [Overview of CropCloudAnalytics](#overview-of-cropcloudanalytics)
2. [Data Sources](#data-sources)
3. [Data Transformations](#data-transformations)
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

## Data Transformations

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

## Load Queries
Include the queries responsible for loading the transformed data into the appropriate destinations (e.g., databases, data warehouses).

## Usage Examples
Provide examples of how to utilize the queries effectively within CropCloud Analytics, including any necessary parameters or configuration settings.

## Notes and Comments
Include any additional notes, comments, or considerations relevant to the Power Query M source code.


