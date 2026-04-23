-- SQL Database Fix Script for CAEManufacturing_SQLDB
-- Connection Details:
--   Server: glhdjewslwruzpuscihr6nmsre-urbryfqunkhuxapla4hqtangbe.database.fabric.microsoft.com,1433
--   Database: CAEManufacturing_SQLDB-6c31cad3-74a3-4eae-91f3-e2a4ed845e7e
-- Run these statements in order using Fabric Portal SQL Editor, Azure Data Studio, or SSMS

-- ==== STEP 1: Check current state ====
SELECT name, type_name(user_type_id) as type_name, is_computed 
FROM sys.columns 
WHERE object_id = OBJECT_ID('plm.tasks') 
AND name IN ('Calculated_Start_Date', 'Calculated_End_Date', 'Is_Milestone');

-- ==== STEP 2: Drop computed columns if they exist ====
IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') 
    ALTER TABLE plm.tasks DROP COLUMN Calculated_End_Date;

IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') 
    ALTER TABLE plm.tasks DROP COLUMN Calculated_Start_Date;

IF EXISTS (SELECT 1 FROM sys.computed_columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') 
    ALTER TABLE plm.tasks DROP COLUMN Is_Milestone;

-- ==== STEP 3: Add regular columns ====
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_Start_Date') 
    ALTER TABLE plm.tasks ADD Calculated_Start_Date DATE;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Calculated_End_Date') 
    ALTER TABLE plm.tasks ADD Calculated_End_Date DATE;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('plm.tasks') AND name = 'Is_Milestone') 
    ALTER TABLE plm.tasks ADD Is_Milestone BIT;

-- ==== STEP 4: Populate columns ====
UPDATE plm.tasks SET
    Calculated_Start_Date = COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start),
    Calculated_End_Date = COALESCE(Actual_End, DATEADD(day, ISNULL(Standard_Duration, 0), COALESCE(Actual_Start, Modified_Planned_Start, Initial_Planned_Start))),
    Is_Milestone = CASE WHEN Milestone = 1 THEN 1 ELSE 0 END;

-- ==== STEP 5: Verify ====
SELECT TOP 5 Task_ID, Calculated_Start_Date, Calculated_End_Date, Is_Milestone, Milestone 
FROM plm.tasks 
ORDER BY Task_ID;

-- After running these, refresh the semantic model:
-- Workspace ID: 161c43a4-6a14-4b8f-81eb-070f0981a609
-- Dataset ID: 2e76bd04-3994-485f-b2b6-847fea6aa0aa
