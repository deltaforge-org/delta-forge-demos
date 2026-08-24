-- Cleanup: Reservoir Simulation History Match Review

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.simulation.sim_arrays WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.simulation.cell_pressure WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.simulation;
