USE master;
GO

-- Crear base de datos si no existe
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'database_test')
BEGIN
    CREATE DATABASE [database_test];
END
GO

-- Crear login si no existe
IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'python_dev')
BEGIN
    CREATE LOGIN [python_dev] 
        WITH PASSWORD = N'python_dev123.', 
        DEFAULT_DATABASE = [database_test], 
        DEFAULT_LANGUAGE = [us_english], 
        CHECK_EXPIRATION = OFF, 
        CHECK_POLICY = OFF;
END
GO

-- Habilitar login (por si estaba deshabilitado)
ALTER LOGIN [python_dev] ENABLE;
GO

-- Usar la base de datos
USE [database_test];
GO

-- Crear usuario si no existe
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'python_dev')
BEGIN
    CREATE USER [python_dev] FOR LOGIN [python_dev];
END
GO

-- Corregir los typos en ADD MEMBER (no MEMBER)
ALTER ROLE db_datareader ADD MEMBER [python_dev];
ALTER ROLE db_datawriter ADD MEMBER [python_dev];
GRANT CREATE TABLE TO [python_dev];
GRANT CONTROL ON SCHEMA::dbo TO [python_dev];
GO

-- Crear tabla si no existe
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'data_test' AND type = 'U')
BEGIN
    CREATE TABLE [dbo].[data_test](
        [ID] [int] IDENTITY(1,1) NOT NULL,
        [Year] [smallint] NULL,
        [Month] [tinyint] NULL,
        [DayofMonth] [tinyint] NULL,
        [DayOfWeek] [tinyint] NULL,
        [Carrier] [nvarchar](50) NULL,
        [OriginAirportID] [smallint] NULL,
        [OriginAirportName] [nvarchar](100) NULL,
        [OriginCity] [nvarchar](50) NULL,
        [OriginState] [nvarchar](50) NULL,
        [DestAirportID] [smallint] NULL,
        [DestAirportName] [nvarchar](100) NULL,
        [DestCity] [nvarchar](50) NULL,
        [DestState] [nvarchar](50) NULL,
        [CRSDepTime] [smallint] NULL,
        [DepDelay] [smallint] NULL,
        [DepDel15] [bit] NULL,
        [CRSArrTime] [smallint] NULL,
        [ArrDelay] [smallint] NULL,
        [ArrDel15] [bit] NULL,
        [Cancelled] [bit] NULL,
    PRIMARY KEY CLUSTERED 
    (
        [ID] ASC
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, 
            OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
    ) ON [PRIMARY]
END
GO