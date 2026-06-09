-- Cleanup: GPU-Accelerated Global Banking Network — 10M Accounts

-- Step 1: Drop graph definition
DROP GRAPH IF EXISTS {{zone_name}}.gpu_finance_network.gpu_finance_network;

-- Step 2: Drop tables (edges before vertices before lookup)
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_finance_network.gfn_transactions WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_finance_network.gfn_accounts WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.gpu_finance_network.gfn_banks WITH FILES;

-- Step 3: Drop schema

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'graph-gpu-10m-finance' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.gpu_finance_network;
