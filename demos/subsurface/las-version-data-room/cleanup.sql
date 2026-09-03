-- Cleanup: Licence Divestment Data Room

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.data_room.log_files WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.data_room.room_audit WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.data_room;
